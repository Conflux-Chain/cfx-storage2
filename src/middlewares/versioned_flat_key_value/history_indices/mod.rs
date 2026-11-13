mod version_range;
pub use version_range::PushError;

use static_assertions::const_assert;
pub use version_range::OffsetBasedVersionRange;

use crate::errors::{Result, StorageError};
use crate::middlewares::HistoryNumber;

pub const LATEST: u64 = u64::MAX;

const VERSION_RANGE_BYTES_LOG: usize = 6; // 6 for 64 bytes, or 7 for 128 bytes
pub const VERSION_RANGE_BYTES: usize = 1 << VERSION_RANGE_BYTES_LOG;

const_assert!(VERSION_RANGE_BYTES == 64 || VERSION_RANGE_BYTES == 128);

/// Tracks version history for a database key through chained records.
///
/// A `HistoryIndices` represents a single record that describes a range of modified version numbers.
/// Records form a chain where each record's `start_version_number` equals the previous record's
/// `end_version_number`, creating a continuous version history.
///
/// See [`super::HistoryIndexKey`] for the storage key structure used to persist these records.
///
/// There are two types of records distinguished by their position in the chain:
///
/// 1. **Latest Record** (stored at `HistoryIndexKey(key, LATEST)`):
///    - Represents ongoing modifications (mutable head of the chain).
///    - Contains:
///      - `start_version_number`: Starting version of the current modification range.
///      - `range_encoding`: Encoding of version numbers in this range (excluding the implicit start).
///        May be empty if only containing the starting version.
///      - `latest_value`: Current value at the latest version (`None` marks a deletion tombstone).
///
/// 2. **Previous Record** (stored at `HistoryIndexKey(key, end_version_number)`):
///    - Represents immutable historical data.
///    - Contains:
///      - `range_encoding`: Encoding of version numbers in this range (excluding the implicit start).
///        Guaranteed non-empty to ensure valid version ranges (end > start).
#[derive(Clone, Debug)]
pub enum HistoryIndices<V: Clone> {
    /// Active record tracking ongoing modifications.
    Latest {
        /// Starting version number for this record
        start_version_number: HistoryNumber,
        /// Range encoding structure (may be empty)
        range_encoding: OffsetBasedVersionRange,
        /// Current value (None indicates deletion)
        latest_value: Option<V>,
    },

    /// Immutable historical record. Contains:
    /// - Non-empty range encoding ensuring valid version ranges
    Previous(OffsetBasedVersionRange),
}

/// Represents a complete previous record.
pub struct PreviousRecord {
    end_version_number: HistoryNumber,
    range_encoding: OffsetBasedVersionRange,
}

#[cfg(test)]
impl PartialEq for HistoryIndices<Box<[u8]>> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Latest {
                    start_version_number: sn1,
                    range_encoding: or1,
                    latest_value: v1,
                },
                Self::Latest {
                    start_version_number: sn2,
                    range_encoding: or2,
                    latest_value: v2,
                },
            ) => sn1 == sn2 && or1 == or2 && v1 == v2,
            (Self::Previous(or1), Self::Previous(or2)) => or1 == or2,
            _ => false,
        }
    }
}

impl<V: Clone> HistoryIndices<V> {
    /// This can only be called when `self` is the latest record and the latest version number <= `version_number`.
    /// Returns the latest_value corresponding to the latest version, otherwise returns Error.
    pub fn get_latest_value(&self, version_number: HistoryNumber) -> Result<Option<V>> {
        match self {
            HistoryIndices::Latest {
                start_version_number,
                range_encoding,
                latest_value,
            } => {
                let latest_version_number = start_version_number + range_encoding.max_offset();
                if latest_version_number > version_number {
                    Err(StorageError::CorruptedHistoryIndices)
                } else {
                    Ok(latest_value.clone())
                }
            }
            HistoryIndices::Previous(_) => Err(StorageError::CorruptedHistoryIndices),
        }
    }

    /// New a latest record. Can only be called when there is no record.
    pub fn new(version_number: HistoryNumber, value: Option<V>) -> Self {
        Self::Latest {
            start_version_number: version_number,
            range_encoding: OffsetBasedVersionRange::new(),
            latest_value: value,
        }
    }

    /// Attempts to push a new version number with its value into the lastest record.
    /// This can only be called when `self` is the latest record and the latest version number < `version_number`,
    /// otherwise returns an error.
    ///
    /// # Behavior
    ///
    /// - If appending `version_number` maintains the constraints of the range_encoding for the lastest record:
    ///   - Modifies `self` to include the new version_number.
    ///   - Returns `Ok(None)`.
    /// - If appending `version_number` would violate the constraints:
    ///   - Converts the original latest record to a previous record, and returns `Ok(Some(this_previous_record))`.
    ///   - Modifies `self` to be the new lastest record, which includes
    ///     - the latest_version_number of the original lastest record as the start_version_number,
    ///     - the `version_number` as the only version except for the start_version_number.
    pub fn push(
        &mut self,
        version_number: HistoryNumber,
        value: Option<V>,
    ) -> Result<Option<PreviousRecord>> {
        match self {
            HistoryIndices::Latest {
                start_version_number,
                range_encoding,
                latest_value,
            } => {
                let latest_version_number = *start_version_number + range_encoding.max_offset();
                if latest_version_number >= version_number {
                    Err(StorageError::CorruptedHistoryIndices)
                } else {
                    // start_version_number <= latest_version_number < version_number
                    let offset = version_number - *start_version_number;
                    let maybe_new_range = range_encoding.try_push_or_new(offset)?;

                    *latest_value = value;

                    if let Some(new_range) = maybe_new_range {
                        *start_version_number = latest_version_number;

                        let previous_record = PreviousRecord {
                            end_version_number: latest_version_number,
                            range_encoding: range_encoding.clone(),
                        };

                        *range_encoding = new_range;

                        Ok(Some(previous_record))
                    } else {
                        Ok(None)
                    }
                }
            }
            HistoryIndices::Previous(_) => Err(StorageError::CorruptedHistoryIndices),
        }
    }

    /// Finds the largest version number `found_version_number` such that:
    /// `found_version_number <= version_number` and exists in this history range.
    /// Returns `None` if no such version is found.
    ///
    /// # Preconditions
    /// - `self` must correspond to the record stored at [`super::HistoryIndexKey`] with
    ///   `version_specifier` parameter (from this function's arguments) as its version specifier.
    /// - The record's `version_specifier` should be the **smallest** value
    ///   satisfying `version_specifier >= version_number` in the version chain.
    ///
    /// # Cases Analysis
    /// 1. **With previous record:**
    ///    - Current record's `start_version_number` = previous record's `end_version_number`.
    ///    - Previous record's `end_version_number` < `version_number`.
    ///    - Therefore: `start_version_number < version_number <= version_specifier`.
    ///      The `found_version_number` exists.
    ///
    /// 2. **No previous record:**
    ///    - If `version_number < start_version_number`: Returns `None`.
    ///    - Else: The `found_version_number` exists.
    pub fn last_le(
        &self,
        version_number: HistoryNumber,
        version_specifier: HistoryNumber,
    ) -> Result<Option<HistoryNumber>> {
        if version_number > version_specifier {
            return Err(StorageError::CorruptedHistoryIndices);
        };

        let (start_version_number, range_encoding) =
            self.compute_start_version(version_specifier)?;

        Ok(range_encoding.last_le(start_version_number, version_number))
    }

    /// Generates a list of existing version numbers in increasing order
    /// that are less than or equal to the given `version_number` and belong to this history range.
    ///
    /// # Preconditions
    /// - `self` must correspond to the record stored at [`super::HistoryIndexKey`] with
    ///   `version_specifier` parameter (from this function's arguments) as its version specifier.
    /// - The record's `version_specifier` should be the **smallest** value
    ///   satisfying `version_specifier >= version_number` in the version chain.
    pub fn collect_versions_le(
        &self,
        version_number: HistoryNumber,
        version_specifier: HistoryNumber,
    ) -> Result<Vec<HistoryNumber>> {
        if version_number > version_specifier {
            return Err(StorageError::CorruptedHistoryIndices);
        }

        let (start_version_number, range_encoding) =
            self.compute_start_version(version_specifier)?;

        Ok(range_encoding.collect_versions_le(start_version_number, version_number))
    }

    fn compute_start_version(
        &self,
        version_specifier: HistoryNumber,
    ) -> Result<(HistoryNumber, &OffsetBasedVersionRange)> {
        match self {
            HistoryIndices::Latest {
                start_version_number,
                range_encoding,
                ..
            } => {
                if version_specifier != LATEST {
                    return Err(StorageError::CorruptedHistoryIndices);
                }
                Ok((*start_version_number, range_encoding))
            }
            HistoryIndices::Previous(range_encoding) => {
                if version_specifier == LATEST {
                    return Err(StorageError::CorruptedHistoryIndices);
                }

                let max_offset = range_encoding.max_offset();
                if version_specifier < max_offset {
                    return Err(StorageError::CorruptedHistoryIndices);
                }
                let start_version_number = version_specifier - max_offset;

                Ok((start_version_number, range_encoding))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tests::version_range::Bitmap;

    use super::*;
    use crate::middlewares::HistoryNumber;

    fn create_latest(
        start_version_number: HistoryNumber,
        range_encoding: OffsetBasedVersionRange,
        latest_value: Option<Vec<u8>>,
    ) -> HistoryIndices<Vec<u8>> {
        HistoryIndices::Latest {
            start_version_number,
            range_encoding,
            latest_value,
        }
    }

    fn create_previous(range_encoding: OffsetBasedVersionRange) -> HistoryIndices<Vec<u8>> {
        HistoryIndices::Previous(range_encoding)
    }

    #[test]
    fn test_get_latest_value() {
        let value = Some(vec![1, 2, 3]);
        let start = 1000;
        let offset = 500;

        // Test Latest with empty version_range
        let latest = create_latest(start, OffsetBasedVersionRange::new(), value.clone());
        assert_eq!(latest.get_latest_value(start + 1).unwrap(), value);
        assert_eq!(latest.get_latest_value(start).unwrap(), value);
        assert!(latest.get_latest_value(start - 1).is_err());

        // Test Latest with non-empty version_range
        let latest_with_range = create_latest(
            start,
            OffsetBasedVersionRange::new_with_offset(offset),
            value.clone(),
        );
        assert_eq!(
            latest_with_range
                .get_latest_value(start + offset + 1)
                .unwrap(),
            value
        );
        assert_eq!(
            latest_with_range.get_latest_value(start + offset).unwrap(),
            value
        );
        assert!(latest_with_range
            .get_latest_value(start + offset - 1)
            .is_err());

        // Test Previous with non-empty version_range
        let previous = create_previous(OffsetBasedVersionRange::new_with_offset(500));
        assert!(previous.get_latest_value(start + offset).is_err());
    }

    #[test]
    fn test_last_le() {
        let start = 1000;

        // Test Latest
        let only_end_offset = u32::MAX as u64 + 1;
        let latest = create_latest(
            start,
            OffsetBasedVersionRange::OnlyEnd(only_end_offset),
            None,
        );
        assert_eq!(
            latest.last_le(start + only_end_offset + 1, LATEST).unwrap(),
            Some(start + only_end_offset)
        );
        assert_eq!(
            latest.last_le(start + only_end_offset, LATEST).unwrap(),
            Some(start + only_end_offset)
        );
        assert_eq!(
            latest.last_le(start + only_end_offset - 1, LATEST).unwrap(),
            Some(start)
        );
        assert_eq!(latest.last_le(start, LATEST).unwrap(), Some(start));
        assert_eq!(latest.last_le(start - 1, LATEST).unwrap(), None);
        assert!(latest.last_le(LATEST, LATEST - 1).is_err());
        assert!(latest.last_le(start, LATEST - 1).is_err());

        // Test Previous
        let max_u32_entry = u32::MAX;
        let max_u32_entry_as_u64 = max_u32_entry as u64;
        let u32_vec = OffsetBasedVersionRange::U32Vector(vec![50, 100, max_u32_entry]);
        let previous = create_previous(u32_vec);
        let version_specifier = start + max_u32_entry_as_u64;
        assert!(previous
            .last_le(version_specifier + 1, version_specifier)
            .is_err());
        assert_eq!(
            previous
                .last_le(version_specifier, version_specifier)
                .unwrap(),
            Some(version_specifier)
        );
        assert_eq!(
            previous
                .last_le(version_specifier - 1, version_specifier)
                .unwrap(),
            Some(start + 100)
        );
        assert_eq!(
            previous.last_le(start, version_specifier).unwrap(),
            Some(start)
        );
        assert_eq!(
            previous.last_le(start - 1, version_specifier).unwrap(),
            None
        );
        assert!(previous
            .last_le(start + 1, max_u32_entry_as_u64 - 1)
            .is_err());
        assert!(previous.last_le(start, LATEST).is_err());
    }

    #[test]
    fn test_collect_versions_le() {
        let start = 1000;

        // Test Latest
        let bitmap = Bitmap::new_from_vec(&[0, 7, 8, 15]);
        let latest = create_latest(start, OffsetBasedVersionRange::Bitmap(bitmap), None);
        assert_eq!(
            latest.collect_versions_le(start + 16, LATEST).unwrap(),
            vec![start, start + 7, start + 8, start + 15]
        );
        assert_eq!(
            latest.collect_versions_le(start + 15, LATEST).unwrap(),
            vec![start, start + 7, start + 8, start + 15]
        );
        assert_eq!(
            latest.collect_versions_le(start, LATEST).unwrap(),
            vec![start]
        );
        assert_eq!(
            latest.collect_versions_le(start - 1, LATEST).unwrap(),
            vec![]
        );
        assert!(latest.collect_versions_le(LATEST, LATEST - 1).is_err());
        assert!(latest.collect_versions_le(start, LATEST - 1).is_err());

        // Test Previous
        let max_u16_entry = u16::MAX;
        let max_u16_entry_as_u64 = max_u16_entry as u64;
        let u16_vec = OffsetBasedVersionRange::U16Vector(vec![50, 100, max_u16_entry]);
        let previous = create_previous(u16_vec);
        let version_specifier = start + max_u16_entry_as_u64;
        assert!(previous
            .collect_versions_le(version_specifier + 1, version_specifier)
            .is_err());
        assert_eq!(
            previous
                .collect_versions_le(version_specifier, version_specifier)
                .unwrap(),
            vec![start, start + 50, start + 100, version_specifier]
        );
        assert_eq!(
            previous
                .collect_versions_le(version_specifier - 1, version_specifier)
                .unwrap(),
            vec![start, start + 50, start + 100]
        );
        assert_eq!(
            previous
                .collect_versions_le(start, version_specifier)
                .unwrap(),
            vec![start]
        );
        assert_eq!(
            previous
                .collect_versions_le(start - 1, version_specifier)
                .unwrap(),
            vec![]
        );
        assert!(previous
            .collect_versions_le(start + 1, max_u16_entry_as_u64 - 1)
            .is_err());
        assert!(previous.collect_versions_le(start, LATEST).is_err());
    }
}
