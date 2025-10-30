#[cfg(fuzzing)]
pub mod version_range;

#[cfg(not(fuzzing))]
mod version_range;

pub use version_range::{BitmapValidationError, PushError};

use static_assertions::const_assert;
pub use version_range::{OffsetBasedVersionRange, U16_VECTOR_CAPACITY, U32_VECTOR_CAPACITY};
mod serde;

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
    pub end_version_number: HistoryNumber,
    pub range_encoding: OffsetBasedVersionRange,
}

#[cfg(test)]
impl<V: Clone + PartialEq> PartialEq for HistoryIndices<V> {
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
                    Err(StorageError::CorruptedHistoryIndices(format!("The queried version_number {} is older than the latest {} in get_latest_value().", version_number, latest_version_number)))
                } else {
                    Ok(latest_value.clone())
                }
            }
            HistoryIndices::Previous(_) => Err(StorageError::CorruptedHistoryIndices(
                "HistoryIndices::Previous calls get_latest_value().".to_string(),
            )),
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
                    Err(StorageError::CorruptedHistoryIndices(format!(
                        "The version_number {} to be pushed is older than the latest {} in push().",
                        version_number, latest_version_number
                    )))
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
            HistoryIndices::Previous(_) => Err(StorageError::CorruptedHistoryIndices(
                "HistoryIndices::Previous calls push().".to_string(),
            )),
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
            return Err(StorageError::CorruptedHistoryIndices(format!("The queried version_number {} is larger than the version_specifier {} in last_le().", version_number, version_specifier)));
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
    ///
    /// # Cases Analysis
    /// 1. **With previous record:**
    ///    - Current record's `start_version_number` = previous record's `end_version_number`.
    ///    - Previous record's `end_version_number` < `version_number`.
    ///    - Therefore: `start_version_number < version_number <= version_specifier`.
    ///      The output list contains at least one element (i.e., current record's `start_version_number`).
    ///
    /// 2. **No previous record:**
    ///    - If `version_number < start_version_number`: The output list is empty.
    ///    - Else: The output list contains at least one element (i.e., current record's `start_version_number`).
    pub fn collect_versions_le(
        &self,
        version_number: HistoryNumber,
        version_specifier: HistoryNumber,
    ) -> Result<Vec<HistoryNumber>> {
        if version_number > version_specifier {
            return Err(StorageError::CorruptedHistoryIndices(format!("The queried version_number {} is larger than the version_specifier {} in collect_versions_le().", version_number, version_specifier)));
        }

        let (start_version_number, range_encoding) =
            self.compute_start_version(version_specifier)?;

        Ok(range_encoding.collect_versions_le(start_version_number, version_number))
    }

    /// # Preconditions
    /// - `self` must correspond to the record stored at [`super::HistoryIndexKey`] with
    ///   `version_specifier` parameter (from this function's arguments) as its version specifier.
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
                    return Err(StorageError::CorruptedHistoryIndices(format!("HistoryIndices::Latest calls compute_start_version() with version_specifier {} that is not equal to LATEST {}", version_specifier, LATEST)));
                }
                Ok((*start_version_number, range_encoding))
            }
            HistoryIndices::Previous(range_encoding) => {
                if version_specifier == LATEST {
                    return Err(StorageError::CorruptedHistoryIndices(format!("HistoryIndices::Previous calls compute_start_version() with version_specifier that is equal to LATEST {}", LATEST)));
                }

                let max_offset = range_encoding.max_offset();
                if version_specifier < max_offset {
                    return Err(StorageError::CorruptedHistoryIndices(format!("HistoryIndices::Previous calls compute_start_version() with version_specifier {} that is less than max_offset {}", version_specifier, max_offset)));
                }
                let start_version_number = version_specifier - max_offset;

                Ok((start_version_number, range_encoding))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tests::{test_utils::version_number_sequences_strategy, version_range::Bitmap};

    use super::*;
    use crate::middlewares::HistoryNumber;

    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1_000))]

        #[test]
        fn proptest_push(version_seqs in version_number_sequences_strategy()) {
            if version_seqs.len() > 1 {
                let mut latest = HistoryIndices::new(version_seqs[0], Some(0));
                for (i, version) in version_seqs[1..].iter().enumerate() {
                    let latest_backup = latest.clone();
                    let maybe_previous = latest.push(*version, Some(i + 1)).unwrap();

                    let latest_version_number = match latest_backup {
                        HistoryIndices::Latest { start_version_number, ref range_encoding, latest_value } => {
                            assert_eq!(latest_value, Some(i));
                            start_version_number + range_encoding.max_offset()
                        },
                        HistoryIndices::Previous(_) => unreachable!(),
                    };

                    match latest {
                        HistoryIndices::Latest { start_version_number, ref range_encoding, latest_value } => {
                            assert_eq!(latest_value, Some(i + 1))
                        },
                        HistoryIndices::Previous(_) => unreachable!(),
                    }

                    if let Some(previous) = maybe_previous {
                        assert_eq!(previous.end_version_number, latest.compute_start_version(LATEST).unwrap().0);
                        assert_eq!(previous.end_version_number, latest_version_number);
                        assert_eq!(previous.range_encoding, latest_backup.compute_start_version(LATEST).unwrap().1.clone());
                    } else {
                        assert_eq!(latest.compute_start_version(LATEST).unwrap().0, latest_backup.compute_start_version(LATEST).unwrap().0);
                    }
                }
            }
        }
    }

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
        let bitmap = Bitmap::try_new_from_vec(&[0, 7, 8, 15]).unwrap();
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

#[cfg(test)]
pub mod test_utils {
    use proptest::collection::vec;
    use proptest::prelude::*;

    use crate::middlewares::{
        versioned_flat_key_value::history_indices::U32_VECTOR_CAPACITY, HistoryNumber,
    };

    use super::{
        version_range::{Bitmap, ValidBitmap, BITMAP_MAX_INDEX},
        HistoryIndices, OffsetBasedVersionRange, U16_VECTOR_CAPACITY,
    };

    pub fn only_end_strategy() -> impl Strategy<Value = u64> {
        (u32::MAX as u64 + 1)..u64::MAX
    }

    fn remove_elements<T: Clone>(elems: &mut Vec<T>, num_elems_to_remove: usize) {
        assert!(elems.len() >= num_elems_to_remove);

        let num_before_remove = elems.len();
        let num_after_remove = num_before_remove - num_elems_to_remove;

        if num_after_remove == 0 {
            elems.clear();
            return;
        }

        // Safety for `num_before_remove - 1`:
        //     Since assert!(elems.len() >= num_elems_to_remove);
        //     and num_after_remove > 0 from here
        //     elems.len() > 0; i.e., num_before_remove > 0

        let mut retain_indices = Vec::with_capacity(num_after_remove);

        if num_after_remove == 1 {
            retain_indices.push((num_before_remove - 1) / 2);
        } else {
            for i in 0..num_after_remove {
                let pos =
                    (i as f64) * ((num_before_remove - 1) as f64) / ((num_after_remove - 1) as f64);
                retain_indices.push(pos.round() as usize);
            }
        }

        assert!(retain_indices
            .iter()
            .all(|element| *element < num_before_remove));
        assert!(retain_indices
            .windows(2)
            .all(|window| window[0] < window[1]));
        assert_eq!(retain_indices.len(), num_after_remove);

        let new_elems: Vec<_> = retain_indices.iter().map(|&i| elems[i].clone()).collect();

        *elems = new_elems;
    }

    pub fn u32_vec_strategy() -> impl Strategy<Value = Vec<u32>> {
        let max_offset_lower_exclude = u16::MAX as u32;
        let max_offset_upper = u32::MAX;

        let prefer_large = any::<bool>();

        let small_elements = vec(1..=max_offset_lower_exclude, 0..U32_VECTOR_CAPACITY);

        let larget_elements = vec(
            max_offset_lower_exclude..=max_offset_upper,
            1..=U32_VECTOR_CAPACITY,
        );

        (prefer_large, small_elements, larget_elements).prop_map(
            |(prefer_large_elem, mut small_elems, mut large_elems)| {
                small_elems.sort_unstable();
                small_elems.dedup();

                large_elems.sort_unstable();
                large_elems.dedup();

                assert!(small_elems.len() < U32_VECTOR_CAPACITY);
                assert!(large_elems.len() <= U32_VECTOR_CAPACITY);

                let total_len = small_elems.len() + large_elems.len();
                if total_len > U32_VECTOR_CAPACITY {
                    let num_elems_to_remove = total_len - U32_VECTOR_CAPACITY;
                    assert!(num_elems_to_remove <= small_elems.len());
                    assert!(num_elems_to_remove < large_elems.len());
                    if prefer_large_elem {
                        remove_elements(&mut small_elems, num_elems_to_remove);
                    } else {
                        remove_elements(&mut large_elems, num_elems_to_remove);
                    }
                    assert_eq!(small_elems.len() + large_elems.len(), U32_VECTOR_CAPACITY);
                }

                let mut result = small_elems;
                result.extend(large_elems);

                assert!(result.len() <= U32_VECTOR_CAPACITY);

                result
            },
        )
    }

    pub fn u16_vec_strategy() -> impl Strategy<Value = Vec<u16>> {
        vec(1..=u16::MAX, 0..=U16_VECTOR_CAPACITY).prop_map(|mut vec| {
            vec.sort_unstable();
            vec.dedup();
            vec
        })
    }

    pub fn u16_vec_non_empty_strategy() -> impl Strategy<Value = Vec<u16>> {
        vec(1..=u16::MAX, 1..=U16_VECTOR_CAPACITY).prop_map(|mut vec| {
            vec.sort_unstable();
            vec.dedup();
            vec
        })
    }

    pub fn get_bitmap_vec_from_binary(existing: &[u8]) -> Vec<u16> {
        let mut data = vec![0u16];
        for (i, i_existing) in existing.iter().enumerate() {
            if *i_existing == 1 {
                data.push(i as u16 + 1);
            }
        }
        data
    }

    pub fn get_bitmap_from_binary(existing: &[u8]) -> ValidBitmap {
        let data = get_bitmap_vec_from_binary(existing);
        Bitmap::try_new_from_vec(&data).unwrap()
    }

    pub fn bitmap_vec_strategy() -> impl Strategy<Value = Vec<u16>> {
        let binary = vec(0u8..=1, BITMAP_MAX_INDEX as usize);
        binary
            .prop_filter(
                "the number of non-zero offsets in OffsetBasedVersionRange::Bitmap should be larger than U16_VECTOR_CAPACITY", 
                |existing| {
                        existing.iter().map(|&x| x as u16).sum::<u16>() as usize > U16_VECTOR_CAPACITY
                    })
            .prop_map(|existing| get_bitmap_vec_from_binary(&existing))
    }

    pub fn bitmap_strategy() -> impl Strategy<Value = ValidBitmap> {
        let binary = vec(0u8..=1, BITMAP_MAX_INDEX as usize);
        binary
            .prop_filter(
                "the number of non-zero offsets in OffsetBasedVersionRange::Bitmap should be larger than U16_VECTOR_CAPACITY", 
                |existing| {
                        existing.iter().map(|&x| x as u16).sum::<u16>() as usize > U16_VECTOR_CAPACITY
                    })
            .prop_map(|existing| get_bitmap_from_binary(&existing))
    }

    pub fn version_number_sequences_strategy() -> impl Strategy<Value = Vec<u64>> {
        let start = prop_oneof![
            start_number_strategy(None),
            start_number_strategy(Some(0)),
            start_number_strategy(Some(100)),
            start_number_strategy(Some(10000)),
        ];

        start.prop_flat_map(|initial_current| {
            vec(0u8..4, 1..=100)
                .prop_flat_map(|control_seq| {
                    control_seq
                        .into_iter()
                        .map(|seg_type| match seg_type {
                            0 => vec(1u64..5, 0..100).boxed(),
                            1 => vec(1u64..100, 0..100).boxed(),
                            2 => vec(1u64..10000, 0..100).boxed(),
                            3 => vec(Just(1u64), 0..100).boxed(),
                            _ => unreachable!(),
                        })
                        .collect::<Vec<_>>()
                        .prop_map(|segments| segments.into_iter().flatten().collect::<Vec<u64>>())
                })
                .prop_map(move |deltas| {
                    let mut current = initial_current;
                    let mut result = vec![current];
                    for delta in deltas {
                        if let Some(next) = current.checked_add(delta) {
                            current = next;
                            result.push(current);
                        } else {
                            break;
                        }
                    }
                    result
                })
        })
    }

    pub fn range_strategy() -> impl Strategy<Value = OffsetBasedVersionRange> {
        prop_oneof![
            only_end_strategy().prop_map(OffsetBasedVersionRange::OnlyEnd),
            u32_vec_strategy().prop_map(OffsetBasedVersionRange::U32Vector),
            u16_vec_strategy().prop_map(OffsetBasedVersionRange::U16Vector),
            bitmap_strategy().prop_map(OffsetBasedVersionRange::Bitmap),
        ]
    }

    pub fn range_non_empty_strategy() -> impl Strategy<Value = OffsetBasedVersionRange> {
        prop_oneof![
            only_end_strategy().prop_map(OffsetBasedVersionRange::OnlyEnd),
            u32_vec_strategy().prop_map(OffsetBasedVersionRange::U32Vector),
            u16_vec_non_empty_strategy().prop_map(OffsetBasedVersionRange::U16Vector),
            bitmap_strategy().prop_map(OffsetBasedVersionRange::Bitmap),
        ]
    }

    pub fn start_number_strategy(upper: Option<u64>) -> impl Strategy<Value = HistoryNumber> {
        let max_start = upper.unwrap_or(u64::MAX - 1);
        0u64..=max_start
    }

    pub fn value_strategy() -> impl Strategy<Value = Option<Box<[u8]>>> {
        prop_oneof![
            vec(0u8..=255, 0..128).prop_map(|x| Some(x.into_boxed_slice())),
            Just(None),
        ]
    }

    pub fn only_end_and_start_strategy() -> impl Strategy<Value = (u64, HistoryNumber)> {
        only_end_strategy().prop_flat_map(|offset| {
            assert!(offset < u64::MAX);
            let upper = u64::MAX - 1 - offset;
            let start_strategy = start_number_strategy(Some(upper));
            (Just(offset), start_strategy)
        })
    }

    pub fn u32_vec_and_start_strategy() -> impl Strategy<Value = (Vec<u32>, HistoryNumber)> {
        u32_vec_strategy().prop_flat_map(|u32_vec| {
            let max_offset = u32_vec.last().unwrap_or(&0);
            let upper = u64::MAX - 1 - *max_offset as u64;
            let start_strategy = start_number_strategy(Some(upper));
            (Just(u32_vec), start_strategy)
        })
    }

    pub fn u16_vec_and_start_strategy() -> impl Strategy<Value = (Vec<u16>, HistoryNumber)> {
        u16_vec_strategy().prop_flat_map(|u16_vec| {
            let max_offset = u16_vec.last().unwrap_or(&0);
            let upper = u64::MAX - 1 - *max_offset as u64;
            let start_strategy = start_number_strategy(Some(upper));
            (Just(u16_vec), start_strategy)
        })
    }

    pub fn bitmap_vec_and_start_strategy() -> impl Strategy<Value = (Vec<u16>, HistoryNumber)> {
        bitmap_vec_strategy().prop_flat_map(|bitmap_vec| {
            let max_offset = bitmap_vec.last().unwrap_or(&0);
            let upper = u64::MAX - 1 - *max_offset as u64;
            let start_strategy = start_number_strategy(Some(upper));
            (Just(bitmap_vec), start_strategy)
        })
    }

    pub fn history_indices_strategy() -> impl Strategy<Value = HistoryIndices<Box<[u8]>>> {
        prop_oneof![
            range_strategy()
                .prop_flat_map(|range_encoding| {
                    let max_offset = range_encoding.max_offset();
                    assert!(max_offset < u64::MAX);
                    let upper = u64::MAX - 1 - max_offset;
                    let start_strategy = start_number_strategy(Some(upper));
                    (start_strategy, Just(range_encoding), value_strategy())
                })
                .prop_map(|(start_version_number, range_encoding, latest_value)| {
                    HistoryIndices::Latest {
                        start_version_number,
                        range_encoding,
                        latest_value,
                    }
                }),
            range_non_empty_strategy().prop_map(HistoryIndices::Previous)
        ]
    }
}
