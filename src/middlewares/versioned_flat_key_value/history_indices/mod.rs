mod version_range;

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
    /// Active record tracking ongoing modifications. Contains:
    /// - Starting version number for this record
    /// - Range encoding structure (may be empty)
    /// - Current value (None indicates deletion)
    Latest((HistoryNumber, OffsetBasedVersionRange, Option<V>)),

    /// Immutable historical record. Contains:
    /// - Non-empty range encoding ensuring valid version ranges
    Previous(OffsetBasedVersionRange),
}

#[cfg(test)]
impl PartialEq for HistoryIndices<Box<[u8]>> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Latest((sn1, or1, v1)), Self::Latest((sn2, or2, v2))) => {
                sn1 == sn2 && or1 == or2 && v1 == v2
            }
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
            HistoryIndices::Latest((start_version_number, range_encoding, latest_value)) => {
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
            HistoryIndices::Latest((start, range, _)) => {
                if version_specifier != LATEST {
                    return Err(StorageError::CorruptedHistoryIndices);
                }
                Ok((*start, range))
            }
            HistoryIndices::Previous(range) => {
                if version_specifier == LATEST {
                    return Err(StorageError::CorruptedHistoryIndices);
                }

                let max_offset = range.max_offset();
                if version_specifier < max_offset {
                    return Err(StorageError::CorruptedHistoryIndices);
                }
                let start = version_specifier - max_offset;

                Ok((start, range))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middlewares::HistoryNumber;

    fn create_latest(
        start: HistoryNumber,
        range: OffsetBasedVersionRange,
        value: Option<Vec<u8>>,
    ) -> HistoryIndices<Vec<u8>> {
        HistoryIndices::Latest((start, range, value))
    }

    fn create_previous(range: OffsetBasedVersionRange) -> HistoryIndices<Vec<u8>> {
        HistoryIndices::Previous(range)
    }

    // Test helper to create a Bitmap with specific bits set
    pub fn create_bitmap_with_bits(bits: &[u64]) -> [u8; VERSION_RANGE_BYTES] {
        let mut bitmap = [0u8; VERSION_RANGE_BYTES];
        for &bit in bits {
            let byte_idx = (bit / 8) as usize;
            let bit_pos = bit % 8;
            bitmap[byte_idx] |= 1 << bit_pos;
        }
        bitmap
    }

    #[test]
    fn test_get_latest_value() {
        let value = Some(vec![1, 2, 3]);
        let start = 1000;
        let offset_minus_1 = 500;

        // Test Latest with empty version_range
        let latest = create_latest(start, OffsetBasedVersionRange::new(), value.clone());
        assert_eq!(latest.get_latest_value(start + 1).unwrap(), value);
        assert_eq!(latest.get_latest_value(start).unwrap(), value);
        assert!(latest.get_latest_value(start - 1).is_err());

        // Test Latest with non-empty version_range
        let latest_with_range = create_latest(
            start,
            OffsetBasedVersionRange::new_with_offset_minus_1(offset_minus_1),
            value.clone(),
        );
        assert_eq!(
            latest_with_range
                .get_latest_value(start + offset_minus_1 + 2)
                .unwrap(),
            value
        );
        assert_eq!(
            latest_with_range
                .get_latest_value(start + offset_minus_1 + 1)
                .unwrap(),
            value
        );
        assert!(latest_with_range
            .get_latest_value(start + offset_minus_1)
            .is_err());

        // Test Previous with non-empty version_range
        let previous = create_previous(OffsetBasedVersionRange::new_with_offset_minus_1(500));
        assert!(previous
            .get_latest_value(start + offset_minus_1 + 1)
            .is_err());
    }

    #[test]
    fn test_last_le() {
        let start = 1000;

        // Test Latest
        let only_end_offset_minus_1 = u32::MAX as u64 + 1;
        let latest = create_latest(
            start,
            OffsetBasedVersionRange::OnlyEnd(only_end_offset_minus_1),
            None,
        );
        assert_eq!(
            latest
                .last_le(start + only_end_offset_minus_1 + 2, LATEST)
                .unwrap(),
            Some(start + only_end_offset_minus_1 + 1)
        );
        assert_eq!(
            latest
                .last_le(start + only_end_offset_minus_1 + 1, LATEST)
                .unwrap(),
            Some(start + only_end_offset_minus_1 + 1)
        );
        assert_eq!(
            latest
                .last_le(start + only_end_offset_minus_1, LATEST)
                .unwrap(),
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
        let version_specifier = start + max_u32_entry_as_u64 + 1;
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
            Some(start + 100 + 1)
        );
        assert_eq!(
            previous.last_le(start, version_specifier).unwrap(),
            Some(start)
        );
        assert_eq!(
            previous.last_le(start - 1, version_specifier).unwrap(),
            None
        );
        assert!(previous.last_le(start + 1, max_u32_entry_as_u64).is_err());
        assert!(previous.last_le(start, LATEST).is_err());
    }

    #[test]
    fn test_collect_versions_le() {
        let start = 1000;

        // Test Latest
        let bitmap = create_bitmap_with_bits(&[0, 7, 8, 15]);
        let latest = create_latest(start, OffsetBasedVersionRange::Bitmap(bitmap), None);
        assert_eq!(
            latest.collect_versions_le(start + 17, LATEST).unwrap(),
            vec![start, start + 1, start + 8, start + 9, start + 16]
        );
        assert_eq!(
            latest.collect_versions_le(start + 16, LATEST).unwrap(),
            vec![start, start + 1, start + 8, start + 9, start + 16]
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
        let version_specifier = start + max_u16_entry_as_u64 + 1;
        assert!(previous
            .collect_versions_le(version_specifier + 1, version_specifier)
            .is_err());
        assert_eq!(
            previous
                .collect_versions_le(version_specifier, version_specifier)
                .unwrap(),
            vec![start, start + 50 + 1, start + 100 + 1, version_specifier]
        );
        assert_eq!(
            previous
                .collect_versions_le(version_specifier - 1, version_specifier)
                .unwrap(),
            vec![start, start + 50 + 1, start + 100 + 1]
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
            .collect_versions_le(start + 1, max_u16_entry_as_u64)
            .is_err());
        assert!(previous.collect_versions_le(start, LATEST).is_err());
    }
}
