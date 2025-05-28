mod bitmap;
pub use bitmap::Bitmap;
mod error;
pub use error::PushError;

use super::VERSION_RANGE_BYTES;
use crate::middlewares::HistoryNumber;

/// `OffsetBasedVersionRange` encodes version numbers relative to a base
/// `start_version_number` that is **not stored in this struct**.
///
/// Key implementation notes:
/// - All stored values represent offsets: `offset = version_number - start_version_number`.
/// - Offset 0 (the base version `start_version_number`) is always present and
///   excluded from storage except in Bitmap variant.
///
/// # Variant selection criteria
/// All variants occupy ≤ `VERSION_RANGE_BYTES`. When multiple representations satisfy the constraints,
/// the smallest binary size variant is selected. U16Vector takes precedence over Bitmap when their sizes are equal.
///
/// - **`OnlyEnd(u64)`**  
///   - Single offset exceeding u32::MAX.
///
/// - **`U32Vector(Vec<u32>)`**  
///   - `u16::MAX < max_offset ≤ u32::MAX` **AND** `(number of non-zero offsets) ≤ VERSION_RANGE_BYTES / 4`.
///   - Non-empty sorted vector of non-zero offsets (saves space by omitting 0).
///
/// - **`U16Vector(Vec<u16>)`**  
///   - `max_offset ≤ u16::MAX` **AND** `(number of non-zero offsets) ≤ VERSION_RANGE_BYTES / 2`.
///   - May be empty, contains only non-zero offsets (0 omitted for space efficiency).
///
/// - **`Bitmap([u8; VERSION_RANGE_BYTES])`**  
///   - `max_offset ≤ (VERSION_RANGE_BYTES * 8 - 1)` **AND** `(number of non-zero offsets) > VERSION_RANGE_BYTES / 2`.
///   - Bit `i` corresponds to `start_version_number + i` (LSB-first packing).
///   - Bit 0 is always set (no storage cost for base version).
#[derive(Debug, Clone, PartialEq)]
pub enum OffsetBasedVersionRange {
    OnlyEnd(u64),
    U32Vector(Vec<u32>),
    U16Vector(Vec<u16>),
    Bitmap(Bitmap),
}

/// Maximum allowed number of u32 entries in an `OffsetBasedVersionRange::U32Vector`
pub const U32_VECTOR_CAPACITY: usize = VERSION_RANGE_BYTES / 4;

/// Maximum allowed number of u16 entries in an `OffsetBasedVersionRange::U16Vector`
pub const U16_VECTOR_CAPACITY: usize = VERSION_RANGE_BYTES / 2;

/// Maximum offset value that can be represented in an `OffsetBasedVersionRange::Bitmap`
pub use bitmap::BITMAP_MAX_INDEX;

impl OffsetBasedVersionRange {
    /// Creates an empty `OffsetBasedVersionRange` containing only the implicit `start_version_number`.
    pub fn new() -> Self {
        OffsetBasedVersionRange::U16Vector(Vec::new())
    }

    /// Creates a new `OffsetBasedVersionRange` containing only the start version number and one additional
    /// version number at the specified offset.
    pub fn new_with_offset(offset: u64) -> Self {
        if offset <= u16::MAX as u64 {
            OffsetBasedVersionRange::U16Vector(vec![offset as u16])
        } else if offset <= u32::MAX as u64 {
            OffsetBasedVersionRange::U32Vector(vec![offset as u32])
        } else {
            OffsetBasedVersionRange::OnlyEnd(offset)
        }
    }
}

// These functions are correct only if self meets the following conditions:
// - For OffsetBasedVersionRange::OnlyEnd variants, the offset must be non-zero.
// - For OffsetBasedVersionRange::U32Vector and OffsetBasedVersionRange::U16Vector variants, the vector must be strictly increasing and contain no zero elements.
// - For OffsetBasedVersionRange::Bitmap variants, Bit 0 (LSB of the first byte) must be set to 1.
// Violating these conditions may result in incorrect outcomes or panics.
impl OffsetBasedVersionRange {
    /// Returns the maximum offset (i.e., max version_number - start_version_number) present in this OffsetBasedVersionRange.
    /// If there are no non-zero offsets (only start_version_number), returns 0.
    pub fn max_offset(&self) -> u64 {
        match self {
            OffsetBasedVersionRange::OnlyEnd(offset) => *offset,
            OffsetBasedVersionRange::U32Vector(vec) => vec.last().copied().unwrap_or(0) as u64,
            OffsetBasedVersionRange::U16Vector(vec) => vec.last().copied().unwrap_or(0) as u64,
            OffsetBasedVersionRange::Bitmap(bitmap) => bitmap.max_bit(),
        }
    }

    /// Finds the largest present version number in this range such that version <= upper_bound.
    /// Note that `upper_bound <= start_version_number` is possible.
    pub fn last_le(
        &self,
        start_version_number: HistoryNumber,
        upper_bound: HistoryNumber,
    ) -> Option<HistoryNumber> {
        if upper_bound < start_version_number {
            return None;
        }

        // The start version is always present
        if upper_bound == start_version_number {
            return Some(start_version_number);
        }

        let offset = upper_bound - start_version_number;

        match self {
            OffsetBasedVersionRange::OnlyEnd(end_offset) => {
                if offset >= *end_offset {
                    Some(start_version_number + end_offset)
                } else {
                    Some(start_version_number)
                }
            }

            OffsetBasedVersionRange::U32Vector(vec) => {
                Some(handle_vec_for_last_le(vec, start_version_number, offset))
            }

            OffsetBasedVersionRange::U16Vector(vec) => {
                Some(handle_vec_for_last_le(vec, start_version_number, offset))
            }

            OffsetBasedVersionRange::Bitmap(bitmap) => {
                Some(start_version_number + bitmap.last_le(offset))
            }
        }
    }

    /// Collects the present version numbers in increasing order in this range such that version <= upper_bound.
    /// Note that `upper_bound <= start_version_number` is possible.
    pub fn collect_versions_le(
        &self,
        start_version_number: HistoryNumber,
        upper_bound: HistoryNumber,
    ) -> Vec<HistoryNumber> {
        let mut versions = Vec::new();

        if start_version_number <= upper_bound {
            match self {
                OffsetBasedVersionRange::OnlyEnd(end_offset) => {
                    versions.push(start_version_number);

                    let end_version_number = start_version_number + end_offset;
                    if end_version_number <= upper_bound {
                        versions.push(end_version_number);
                    }
                }

                OffsetBasedVersionRange::U32Vector(vec) => {
                    versions.push(start_version_number);

                    handle_vec_for_collect_le(
                        vec,
                        start_version_number,
                        upper_bound,
                        &mut versions,
                    );
                }

                OffsetBasedVersionRange::U16Vector(vec) => {
                    versions.push(start_version_number);

                    handle_vec_for_collect_le(
                        vec,
                        start_version_number,
                        upper_bound,
                        &mut versions,
                    );
                }

                OffsetBasedVersionRange::Bitmap(bitmap) => {
                    let offsets = bitmap.collect_le(upper_bound - start_version_number);
                    versions.extend(offsets.into_iter().map(|x| start_version_number + x));
                }
            }
        }

        versions
    }
}

impl OffsetBasedVersionRange {
    /// This function will return an error if `self` does not satisfy the constraints of [`OffsetBasedVersionRange`].
    fn validate(&self) -> Result<(), PushError> {
        match self {
            OffsetBasedVersionRange::OnlyEnd(existing_offset) => {
                if *existing_offset <= u32::MAX as u64 {
                    return Err(PushError::InvalidState);
                }
            }
            OffsetBasedVersionRange::U32Vector(vec) => {
                if vec.is_empty() {
                    return Err(PushError::InvalidState);
                }
                if *vec.first().unwrap() == 0 {
                    return Err(PushError::InvalidState);
                }
                if vec.windows(2).any(|window| window[0] >= window[1]) {
                    return Err(PushError::InvalidState);
                }
                if vec.len() > U32_VECTOR_CAPACITY {
                    return Err(PushError::InvalidState);
                }

                let last_offset = *vec.last().unwrap();
                if last_offset <= u16::MAX as u32 {
                    return Err(PushError::InvalidState);
                }
            }
            OffsetBasedVersionRange::U16Vector(vec) => {
                if let Some(first_offset) = vec.first() {
                    if *first_offset == 0 {
                        return Err(PushError::InvalidState);
                    }
                }

                if vec.windows(2).any(|window| window[0] >= window[1]) {
                    return Err(PushError::InvalidState);
                }
                if vec.len() > U16_VECTOR_CAPACITY {
                    return Err(PushError::InvalidState);
                }
            }
            OffsetBasedVersionRange::Bitmap(bits) => {
                if bits.count_ones() <= U16_VECTOR_CAPACITY {
                    return Err(PushError::InvalidState);
                }

                return bits.validate();
            }
        }

        Ok(())
    }

    /// Attempts to push a new offset into the range.
    ///
    /// # Errors
    ///
    /// This function will return an error if either:
    /// - `self` does not initially satisfy the constraints of [`OffsetBasedVersionRange`], or
    /// - `offset` is not larger than all existing offsets in the range.
    ///
    /// # Behavior
    ///
    /// - If appending `offset` maintains the constraints of [`OffsetBasedVersionRange`]:
    ///   - Modifies `self` to include the new offset.
    ///   - Returns `Ok(None)`.
    /// - If appending `offset` would violate the constraints:
    ///   - Leaves `self` unchanged.
    ///   - Returns `Ok(Some(new_range))` containing a single-offset range of `offset`.
    pub fn try_push_or_new(
        &mut self,
        offset: HistoryNumber,
    ) -> Result<Option<OffsetBasedVersionRange>, PushError> {
        // check whether `self` initially satisfies the constraints of [`OffsetBasedVersionRange`]
        self.validate()?;

        // check whether `offset` is larger than all existing offsets in the range
        let max_offset = self.max_offset();
        if offset <= max_offset {
            return Err(PushError::OffsetNotLarger);
        }

        // delayed calculation of the new_range, which will be used when appending `offset` would violate the constraints
        let new_range = || {
            let new_offset = offset - max_offset;
            OffsetBasedVersionRange::new_with_offset(new_offset)
        };

        match self {
            OffsetBasedVersionRange::OnlyEnd(existing_offset) => {
                // Can't push to OnlyEnd; must split
                Ok(Some(new_range()))
            }
            OffsetBasedVersionRange::U32Vector(vec) => {
                if (offset <= u32::MAX as u64) && (vec.len() < U32_VECTOR_CAPACITY) {
                    vec.push(offset as u32);
                    Ok(None)
                } else {
                    Ok(Some(new_range()))
                }
            }
            OffsetBasedVersionRange::U16Vector(vec) => {
                if offset > u16::MAX as u64 {
                    if (offset > u32::MAX as u64) || (vec.len() + 1 > U32_VECTOR_CAPACITY) {
                        return Ok(Some(new_range()));
                    } else {
                        let old_vec = std::mem::take(vec);
                        let new_vec: Vec<u32> = old_vec
                            .into_iter()
                            .map(|x| x as u32)
                            .chain(std::iter::once(offset as u32))
                            .collect();
                        *self = OffsetBasedVersionRange::U32Vector(new_vec);
                        return Ok(None);
                    }
                }

                if vec.len() + 1 > U16_VECTOR_CAPACITY {
                    if offset > BITMAP_MAX_INDEX as u64 {
                        Ok(Some(new_range()))
                    } else {
                        let mut old_vec = std::mem::take(vec);
                        old_vec.push(offset as u16);
                        let bitmap = Bitmap::new_from_vec(&old_vec);
                        *self = OffsetBasedVersionRange::Bitmap(bitmap);
                        Ok(None)
                    }
                } else {
                    assert!(offset <= u16::MAX as u64);
                    vec.push(offset as u16);
                    Ok(None)
                }
            }
            OffsetBasedVersionRange::Bitmap(bits) => {
                if bits.set_unchecked(offset) {
                    Ok(None)
                } else {
                    Ok(Some(new_range()))
                }
            }
        }
    }
}

pub trait SaturatingCastable: Ord + Copy + Into<u64> {
    fn saturating_from(value: u64) -> Self;
}

impl SaturatingCastable for u16 {
    fn saturating_from(value: u64) -> Self {
        value.min(u16::MAX as u64) as u16
    }
}

impl SaturatingCastable for u32 {
    fn saturating_from(value: u64) -> Self {
        value.min(u32::MAX as u64) as u32
    }
}

fn handle_vec_for_last_le<T>(vec: &[T], start_version_number: u64, offset: u64) -> u64
where
    T: SaturatingCastable,
{
    let target = T::saturating_from(offset);
    match vec.binary_search(&target) {
        Ok(idx) => start_version_number + vec[idx].into(),
        Err(idx) => {
            if idx > 0 {
                start_version_number + vec[idx - 1].into()
            } else {
                start_version_number
            }
        }
    }
}

fn handle_vec_for_collect_le<T: Into<u64> + Copy>(
    vec: &[T],
    start_version_number: u64,
    upper_bound: u64,
    versions: &mut Vec<u64>,
) {
    for offset in vec {
        let v = start_version_number + (*offset).into();
        if v <= upper_bound {
            versions.push(v);
        } else {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::any::TypeId;

    use ark_std::iterable::Iterable;
    use itertools::Itertools;
    use rand_distr::num_traits::Bounded;

    use super::*;

    #[test]
    fn test_u16_saturation() {
        assert_eq!(u16::saturating_from(0), 0);
        assert_eq!(u16::saturating_from(1), 1);
        assert_eq!(u16::saturating_from(65534), 65534);
        assert_eq!(u16::saturating_from(65535), 65535);
        assert_eq!(u16::saturating_from(65536), 65535);
        assert_eq!(u16::saturating_from(100000), 65535);
    }

    #[test]
    fn test_u32_saturation() {
        assert_eq!(u32::saturating_from(0), 0);
        assert_eq!(u32::saturating_from(1), 1);
        assert_eq!(u32::saturating_from(4294967294), 4294967294);
        assert_eq!(u32::saturating_from(4294967295), 4294967295);
        assert_eq!(u32::saturating_from(4294967296), 4294967295);
        assert_eq!(u32::saturating_from(5000000000), 4294967295);
    }

    #[test]
    fn test_new() {
        let range = OffsetBasedVersionRange::new();
        assert!(matches!(range, OffsetBasedVersionRange::U16Vector(v) if v.is_empty()));
    }

    #[test]
    fn test_new_with_offset() {
        // u16 case
        let range = OffsetBasedVersionRange::new_with_offset(100);
        assert!(matches!(range, OffsetBasedVersionRange::U16Vector(v) if v == vec![100]));

        // u32 case
        let range = OffsetBasedVersionRange::new_with_offset(70000);
        assert!(matches!(range, OffsetBasedVersionRange::U32Vector(v) if v == vec![70000]));

        // OnlyEnd case
        let large_offset = u32::MAX as u64 + 1;
        let range = OffsetBasedVersionRange::new_with_offset(large_offset);
        assert!(
            matches!(range, OffsetBasedVersionRange::OnlyEnd(offset) if offset == large_offset)
        );
    }

    #[test]
    fn test_max_offset() {
        // Test OnlyEnd
        let only_end = OffsetBasedVersionRange::OnlyEnd(100);
        assert_eq!(only_end.max_offset(), 100);

        // Test U32Vector with non-empty and empty cases
        let u32_valid = OffsetBasedVersionRange::U32Vector(vec![10, 20, 30]);
        assert_eq!(u32_valid.max_offset(), 30);
        let u32_empty = OffsetBasedVersionRange::U32Vector(vec![]);
        assert_eq!(u32_empty.max_offset(), 0);

        // Test U16Vector with non-empty and empty cases
        let u16_valid = OffsetBasedVersionRange::U16Vector(vec![5]);
        assert_eq!(u16_valid.max_offset(), 5);
        let u16_empty = OffsetBasedVersionRange::U16Vector(vec![]);
        assert_eq!(u16_empty.max_offset(), 0);

        // Test Bitmap with various bit configurations
        for bit_index in 0..=BITMAP_MAX_INDEX {
            let bitmap = Bitmap::new_from_vec(&[0, bit_index]);
            let range = OffsetBasedVersionRange::Bitmap(bitmap);
            assert_eq!(
                range.max_offset(),
                bit_index as u64,
                "Failed for bit index {}",
                bit_index
            );
        }

        let bitmap = Bitmap::new_from_vec(&[0, 50, 100]);
        let bitmap_range = OffsetBasedVersionRange::Bitmap(bitmap);
        assert_eq!(bitmap_range.max_offset(), 100);

        let bitmap = Bitmap::new_from_vec(&[50, 100]);
        let bitmap_range = OffsetBasedVersionRange::Bitmap(bitmap);
        assert_eq!(bitmap_range.max_offset(), 100);

        let bitmap = Bitmap::new_from_vec(&[]);
        let bitmap_range = OffsetBasedVersionRange::Bitmap(bitmap);
        assert_eq!(bitmap_range.max_offset(), 0);
    }

    #[derive(Debug, Clone)]
    struct VersionRangeTestCase {
        start_version: u64,
        range: OffsetBasedVersionRange,
        last_le_cases: Vec<(u64, Option<u64>)>,
        collect_le_cases: Vec<(u64, Vec<u64>)>,
    }

    fn test_range_method<F, R>(
        start: u64,
        range: &OffsetBasedVersionRange,
        cases: &[(u64, R)],
        method: F,
    ) where
        F: Fn(&OffsetBasedVersionRange, u64, u64) -> R,
        R: PartialEq + std::fmt::Debug,
    {
        for &(target, ref expected) in cases {
            assert_eq!(
                method(range, start, target),
                *expected,
                "target={} start={} range={:?}",
                target,
                start,
                range
            );
        }
    }

    fn only_end_cases(offset: u64, start: u64) -> VersionRangeTestCase {
        assert!(offset > 0);
        assert!(offset <= u64::MAX - start);

        let mut targets = vec![start + offset, start, start - 1];

        let mut last_le = vec![Some(start + offset), Some(start), None];

        let mut collect_le = vec![vec![start, start + offset], vec![start], vec![]];

        if offset < u64::MAX - start {
            targets.push(start + offset + 1);
            last_le.push(Some(start + offset));
            collect_le.push(vec![start, start + offset]);
        }

        if offset > 1 {
            targets.push(start + 1);
            last_le.push(Some(start));
            collect_le.push(vec![start]);
        }

        assert_eq!(targets.len(), last_le.len());
        assert_eq!(targets.len(), collect_le.len());

        VersionRangeTestCase {
            start_version: start,
            range: OffsetBasedVersionRange::OnlyEnd(offset),
            last_le_cases: targets.clone().into_iter().zip(last_le).collect(),
            collect_le_cases: targets.into_iter().zip(collect_le).collect(),
        }
    }

    fn vec_cases<T: Ord + Into<u64> + Default + Bounded + Copy + 'static>(
        vec: Vec<T>,
        start: u64,
    ) -> VersionRangeTestCase {
        assert!(vec.iter().all(|element| (*element).into() > 0));
        assert!(vec.windows(2).all(|window| window[0] < window[1]));

        let max_offset: u64 = (*vec.last().unwrap_or(&T::min_value())).into();
        assert!(max_offset <= u64::MAX - start);

        let mut targets = vec![
            start + T::max_value().into() + 2,
            start + T::max_value().into() + 1,
            start + T::max_value().into(),
            start,
            start - 1,
        ];

        let mut last_le = vec![
            Some(start + max_offset),
            Some(start + max_offset),
            Some(start + max_offset),
            Some(start),
            None,
        ];

        let mut all_versions = vec![start];
        all_versions.extend(vec.iter().map(|offset| start + (*offset).into()));
        let mut collect_le = vec![
            all_versions.clone(),
            all_versions.clone(),
            all_versions.clone(),
            vec![start],
            vec![],
        ];

        if let Some(first_offset) = vec.first() {
            if (*first_offset).into() > 1 {
                targets.push(start + 1);
                last_le.push(Some(start));
                collect_le.push(vec![start]);
            }
        }

        if max_offset < T::max_value().into() {
            targets.push(start + T::max_value().into() - 1);
            last_le.push(Some(start + max_offset));
            collect_le.push(all_versions);
        }

        for (idx, element) in vec.iter().enumerate() {
            let offset = (*element).into();
            targets.extend([start + offset + 1, start + offset, start + offset - 1].iter());

            let next_should_plus_one = if idx + 1 < vec.len() {
                if offset + 1 == vec[idx + 1].into() {
                    1
                } else {
                    0
                }
            } else {
                0
            };
            let next_last_le = start + offset + next_should_plus_one as u64;
            let next_idx = idx + next_should_plus_one;

            let prev_last_le = start + if idx > 0 { vec[idx - 1].into() } else { 0 };

            last_le.extend([Some(next_last_le), Some(start + offset), Some(prev_last_le)].iter());

            let mut next_versions = vec![start];
            next_versions.extend(
                vec[..=next_idx]
                    .iter()
                    .map(|offset| start + (*offset).into()),
            );
            let mut versions = vec![start];
            versions.extend(vec[..=idx].iter().map(|offset| start + (*offset).into()));
            let mut prev_versions = vec![start];
            prev_versions.extend(vec[..idx].iter().map(|offset| start + (*offset).into()));

            collect_le.extend([next_versions, versions, prev_versions]);
        }

        let version_range = if TypeId::of::<T>() == TypeId::of::<u16>() {
            let vec_u16 = unsafe { std::mem::transmute(vec) };
            OffsetBasedVersionRange::U16Vector(vec_u16)
        } else if TypeId::of::<T>() == TypeId::of::<u32>() {
            let vec_u32 = unsafe { std::mem::transmute(vec) };
            OffsetBasedVersionRange::U32Vector(vec_u32)
        } else {
            dbg!(TypeId::of::<T>());
            panic!("T must be either u16 or u32");
        };

        assert_eq!(targets.len(), last_le.len());
        assert_eq!(targets.len(), collect_le.len());

        VersionRangeTestCase {
            start_version: start,
            range: version_range,
            last_le_cases: targets.clone().into_iter().zip(last_le).collect(),
            collect_le_cases: targets.into_iter().zip(collect_le).collect(),
        }
    }

    fn bitmap_cases(input_vec: Vec<u16>, start: u64) -> VersionRangeTestCase {
        let bitmap = Bitmap::new_from_vec(&input_vec);
        let vec = bitmap.to_vec();

        let mut targets = vec![start - 1];

        let mut last_le = vec![None];

        let mut collect_le = vec![vec![]];

        for target_offset in 0..BITMAP_MAX_INDEX + 3 {
            let end_idx_excluded = match vec.binary_search(&target_offset) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            };
            let versions: Vec<_> = vec[..end_idx_excluded]
                .iter()
                .map(|offset| start + *offset as u64)
                .collect();
            targets.push(start + target_offset as u64);
            last_le.push(versions.last().cloned());
            collect_le.push(versions);
        }

        assert_eq!(targets.len(), last_le.len());
        assert_eq!(targets.len(), collect_le.len());

        VersionRangeTestCase {
            start_version: start,
            range: OffsetBasedVersionRange::Bitmap(bitmap),
            last_le_cases: targets.clone().into_iter().zip(last_le).collect(),
            collect_le_cases: targets.into_iter().zip(collect_le).collect(),
        }
    }

    fn test_common(test_cases: VersionRangeTestCase) {
        let VersionRangeTestCase {
            start_version: start,
            range,
            last_le_cases: last_cases,
            collect_le_cases: collect_cases,
        } = test_cases;
        test_range_method(start, &range, &last_cases, |r, s, t| r.last_le(s, t));
        test_range_method(start, &range, &collect_cases, |r, s, t| {
            r.collect_versions_le(s, t)
        });
    }

    fn test_only_end(offset: u64, start: u64) {
        test_common(only_end_cases(offset, start));
    }

    fn test_vec<T: Ord + Into<u64> + Default + Bounded + Copy + 'static>(vec: Vec<T>, start: u64) {
        test_common(vec_cases(vec, start));
    }

    fn test_bitmap(vec: Vec<u16>, start: u64) {
        test_common(bitmap_cases(vec, start));
    }

    #[test]
    fn test_last_le_and_collect_versions_le() {
        let start = 1000;

        // Test OnlyEnd cases
        //   valid cases
        test_only_end(u32::MAX as u64 + 1, start);
        test_only_end(u64::MAX - start, start);
        //   cases to check robustness
        test_only_end(u32::MAX as u64, start);
        test_only_end(1, start);

        // Test U32Vector
        //   valid cases
        let max_u32_entry = u32::MAX;
        test_vec(vec![50, 100, max_u32_entry], start);
        test_vec(vec![max_u32_entry], start);
        test_vec(
            vec![
                1u32,
                2,
                3,
                4,
                5,
                100,
                max_u32_entry - 2,
                max_u32_entry - 1,
                max_u32_entry,
            ],
            start,
        );
        test_vec(vec![2, u16::MAX as u32 + 1], start);
        //   cases to check robustness
        test_vec(Vec::<u32>::new(), start);
        test_vec(vec![1u32, 100, u16::MAX as u32], start);
        test_vec((1..=U32_VECTOR_CAPACITY as u32 + 1).collect_vec(), start);

        // Test U16Vector
        //   valid cases
        let max_u16_entry = u16::MAX;
        test_vec(Vec::<u16>::new(), start);
        test_vec(vec![50, 100, max_u16_entry], start);
        test_vec(vec![max_u16_entry], start);
        test_vec(
            vec![
                1u16,
                2,
                3,
                4,
                5,
                100,
                max_u16_entry - 2,
                max_u16_entry - 1,
                max_u16_entry,
            ],
            start,
        );
        test_vec(vec![1u16, 100, 10000], start);
        test_vec(vec![2, BITMAP_MAX_INDEX + 1], start);
        test_vec(
            ((BITMAP_MAX_INDEX - U16_VECTOR_CAPACITY as u16 + 1)..=BITMAP_MAX_INDEX).collect_vec(),
            start,
        );
        //   cases to check robustness
        test_vec(
            ((BITMAP_MAX_INDEX - U16_VECTOR_CAPACITY as u16)..=BITMAP_MAX_INDEX).collect_vec(),
            start,
        );

        // Test Bitmap
        //   valid cases
        test_bitmap(
            ((BITMAP_MAX_INDEX - U16_VECTOR_CAPACITY as u16)..=BITMAP_MAX_INDEX).collect_vec(),
            start,
        );
        test_bitmap((0..=U16_VECTOR_CAPACITY as u16 + 1).collect_vec(), start);
        test_bitmap((0..=BITMAP_MAX_INDEX).collect(), start);
        test_bitmap((0..=BITMAP_MAX_INDEX / 2).map(|x| x * 2).collect(), start);
        test_bitmap((0..=BITMAP_MAX_INDEX / 3).map(|x| x * 3).collect(), start);
        //   cases to check robustness
        test_bitmap(
            ((BITMAP_MAX_INDEX - U16_VECTOR_CAPACITY as u16 + 1)..=BITMAP_MAX_INDEX).collect_vec(),
            start,
        );
        test_bitmap(vec![0, 7, 8, 15], start);
    }

    mod validate_tests {
        use super::*;

        #[test]
        fn test_only_end_valid() {
            let valid_offset = u32::MAX as u64 + 1;
            let range = OffsetBasedVersionRange::OnlyEnd(valid_offset);
            assert!(range.validate().is_ok());
        }

        #[test]
        fn test_only_end_invalid() {
            let invalid_offset = u32::MAX as u64;
            let range = OffsetBasedVersionRange::OnlyEnd(invalid_offset);
            assert_eq!(range.validate(), Err(PushError::InvalidState));
        }

        #[test]
        fn test_u32_vector_empty() {
            let range = OffsetBasedVersionRange::U32Vector(vec![]);
            assert_eq!(range.validate(), Err(PushError::InvalidState));
        }

        #[test]
        fn test_u32_vector_first_zero() {
            let range = OffsetBasedVersionRange::U32Vector(vec![0, 1, u32::MAX]);
            assert_eq!(range.validate(), Err(PushError::InvalidState));
        }

        #[test]
        fn test_u32_vector_non_increasing() {
            let range = OffsetBasedVersionRange::U32Vector(vec![1, 1, 2, u32::MAX]);
            assert_eq!(range.validate(), Err(PushError::InvalidState));
        }

        #[test]
        fn test_u32_vector_exceeds_capacity() {
            let mut vec = (1..=(U32_VECTOR_CAPACITY as u32 + 1)).collect::<Vec<_>>();
            vec[U32_VECTOR_CAPACITY] = u32::MAX;
            let range = OffsetBasedVersionRange::U32Vector(vec);
            assert_eq!(range.validate(), Err(PushError::InvalidState));
        }

        #[test]
        fn test_u32_vector_last_too_small() {
            let mut vec = (1..=U32_VECTOR_CAPACITY as u32).collect::<Vec<_>>();
            vec[U32_VECTOR_CAPACITY - 1] = u16::MAX as u32;
            let range = OffsetBasedVersionRange::U32Vector(vec);
            assert_eq!(range.validate(), Err(PushError::InvalidState));
        }

        #[test]
        fn test_u32_vector_valid() {
            let mut vec = (1..=U32_VECTOR_CAPACITY as u32).collect::<Vec<_>>();
            vec[U32_VECTOR_CAPACITY - 1] = u16::MAX as u32 + 1;
            let range = OffsetBasedVersionRange::U32Vector(vec);
            assert!(range.validate().is_ok());
        }

        #[test]
        fn test_u32_vector_valid_max() {
            let mut vec = (1..=U32_VECTOR_CAPACITY as u32).collect::<Vec<_>>();
            vec[U32_VECTOR_CAPACITY - 1] = u32::MAX;
            let range = OffsetBasedVersionRange::U32Vector(vec);
            assert!(range.validate().is_ok());
        }

        #[test]
        fn test_u16_vector_empty() {
            let range = OffsetBasedVersionRange::U16Vector(vec![]);
            assert!(range.validate().is_ok());
        }

        #[test]
        fn test_u16_vector_first_zero() {
            let range = OffsetBasedVersionRange::U16Vector(vec![0, 1, 2]);
            assert_eq!(range.validate(), Err(PushError::InvalidState));
        }

        #[test]
        fn test_u16_vector_non_increasing() {
            let range = OffsetBasedVersionRange::U16Vector(vec![1, 2, 2]);
            assert_eq!(range.validate(), Err(PushError::InvalidState));
        }

        #[test]
        fn test_u16_vector_exceeds_capacity() {
            let vec = (1..=U16_VECTOR_CAPACITY as u16 + 1).collect::<Vec<_>>();
            let range = OffsetBasedVersionRange::U16Vector(vec);
            assert_eq!(range.validate(), Err(PushError::InvalidState));
        }

        #[test]
        fn test_u16_vector_valid() {
            let mut vec = (1..=U16_VECTOR_CAPACITY as u16).collect::<Vec<_>>();
            vec[U16_VECTOR_CAPACITY - 1] = u16::MAX;
            let range = OffsetBasedVersionRange::U16Vector(vec);
            assert!(range.validate().is_ok());
        }

        #[test]
        fn test_bitmap_insufficient_bits() {
            let vec = (0..U16_VECTOR_CAPACITY as u16).collect::<Vec<_>>();
            let bitmap = Bitmap::new_from_vec(&vec);
            let range = OffsetBasedVersionRange::Bitmap(bitmap);
            assert_eq!(range.validate(), Err(PushError::InvalidState));
        }

        #[test]
        fn test_bitmap_valid_bits_invalid_validate() {
            let vec = (0..=U16_VECTOR_CAPACITY as u16).collect::<Vec<_>>();
            let bitmap = Bitmap::new_invalid_from_vec(&vec);
            let range = OffsetBasedVersionRange::Bitmap(bitmap);
            assert_eq!(range.validate(), Err(PushError::InvalidState));
        }

        #[test]
        fn test_bitmap_valid() {
            let vec = (0..=U16_VECTOR_CAPACITY as u16).collect::<Vec<_>>();
            let bitmap = Bitmap::new_from_vec(&vec);
            let range = OffsetBasedVersionRange::Bitmap(bitmap);
            assert!(range.validate().is_ok());
        }
    }
}
