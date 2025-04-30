mod one_range;
mod serde;

pub use one_range::{OneRange, BITMAP_MAX_INDEX, MAX_FOUR_ENTRIES, MAX_TWO_ENTRIES};

use crate::errors::{Result, StorageError};
use crate::middlewares::HistoryNumber;

pub const LATEST: u64 = u64::MAX;

const ONE_RANGE_BYTES_LOG: usize = 6; // 6 for 64 bytes, or 7 for 128 bytes
pub const ONE_RANGE_BYTES: usize = 1 << ONE_RANGE_BYTES_LOG;

const _: () = assert!(ONE_RANGE_BYTES == 64 || ONE_RANGE_BYTES == 128);

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
    Latest((HistoryNumber, OneRange, Option<V>)),

    /// Immutable historical record. Contains:
    /// - Non-empty range encoding ensuring valid version ranges
    Previous(OneRange),
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
            HistoryIndices::Latest((start_version_number, one_range, latest_value)) => {
                let latest_version_number = start_version_number + one_range.max_offset();
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

        let (start_version_number, one_range) = self.compute_start_version(version_specifier)?;

        Ok(one_range.last_le(start_version_number, version_number))
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

        let (start_version_number, one_range) = self.compute_start_version(version_specifier)?;

        one_range.collect_versions_le(start_version_number, version_number)
    }

    fn compute_start_version(
        &self,
        version_specifier: HistoryNumber,
    ) -> Result<(HistoryNumber, &OneRange)> {
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
