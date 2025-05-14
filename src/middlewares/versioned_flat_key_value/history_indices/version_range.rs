use super::VERSION_RANGE_BYTES;
use crate::middlewares::HistoryNumber;

/// `OffsetBasedVersionRange` encodes version numbers relative to a base `start_version_number` that is **not stored in this struct**.
///
/// Key characteristics:
/// 1. The `start_version_number` exists by default and is **never explicitly recorded** in the struct.
/// 2. All stored information is expressed through `offset_minus_1` values, where:
///    `offset_minus_1 = version_number - start_version_number - 1`
///
/// The data structures are optimized for different ranges and densities:
///
/// - `OnlyEnd(u64)`:
///     - Special case for when the only version number (besides the implicit `start_version_number`)
///       is an `end_version_number` whose `offset_minus_1` (i.e., end - start - 1) exceeds `u32::MAX`.
///
/// - `U32Vector(Vec<u32>)`:
///     - Used when the maximum `offset_minus_1` is in `2^16 ..= 2^32-1` and there are <= (VERSION_RANGE_BYTES / 4) version numbers (excluding `start_version_number`).
///     - `offset_minus_1` values are stored as `u32` in increasing order; each entry is (version_number - start_version_number - 1).
///     - The `Vec<u32>` must not be empty.
///
/// - `U16Vector(Vec<u16>)`:
///     - Used when the maximum `offset_minus_1` is in `0 ..= 2^16-1` and there are <= (VERSION_RANGE_BYTES / 2) version numbers (excluding `start_version_number`).
///     - `offset_minus_1` values are stored as `u16` in increasing order; each entry is (version_number - start_version_number - 1).
///     - The `Vec<u16>` can be empty.
///
/// - `Bitmap([u8; VERSION_RANGE_BYTES])`:
///     - Used when the maximum `offset_minus_1` is in `0 ..= (VERSION_RANGE_BYTES * 8 - 1)` and there are > (VERSION_RANGE_BYTES / 2) version numbers (excluding `start_version_number`).
///     - Each bit at index i indicates the existence of `start_version_number + i + 1`.
///         Specifically, the `i`th bit is the `(i % 8)`-th **least significant bit** (LSB) in `bits[i / 8]`.
///     - There are more than `(VERSION_RANGE_BYTES / 2)` bits.
#[derive(Debug, Clone, PartialEq)]
pub enum OffsetBasedVersionRange {
    OnlyEnd(u64),
    U32Vector(Vec<u32>),
    U16Vector(Vec<u16>),
    Bitmap([u8; VERSION_RANGE_BYTES]),
}

/// Maximum allowed number of u32 entries in an `OffsetBasedVersionRange::U32Vector`
pub const U32_VECTOR_CAPACITY: usize = VERSION_RANGE_BYTES / 4;

/// Maximum allowed number of u16 entries in an `OffsetBasedVersionRange::U16Vector`
pub const U16_VECTOR_CAPACITY: usize = VERSION_RANGE_BYTES / 2;

/// Maximum offset_minus_1 value that can be represented in a OffsetBasedVersionRange::Bitmap variant
pub const BITMAP_MAX_INDEX: u64 = VERSION_RANGE_BYTES as u64 * 8 - 1;

impl OffsetBasedVersionRange {
    /// Creates an empty `OffsetBasedVersionRange` containing only the implicit `start_version_number`.
    pub fn new() -> Self {
        OffsetBasedVersionRange::U16Vector(Vec::new())
    }

    /// Creates a new `OffsetBasedVersionRange` containing only the start version number and one additional
    /// version number at the specified offset_minus_1.
    pub fn new_with_offset_minus_1(offset_minus_1: u64) -> Self {
        if offset_minus_1 <= u16::MAX as u64 {
            OffsetBasedVersionRange::U16Vector(vec![offset_minus_1 as u16])
        } else if offset_minus_1 <= u32::MAX as u64 {
            OffsetBasedVersionRange::U32Vector(vec![offset_minus_1 as u32])
        } else {
            OffsetBasedVersionRange::OnlyEnd(offset_minus_1)
        }
    }
}

// By design, the `vec` in `U32Vector` is guaranteed to be non-empty, and `Bitmap` is guaranteed to contain more than
// `(VERSION_RANGE_BYTES / 2)` bits. These constraints exist for external guarantees (e.g., data validity elsewhere).
// The functions below are fully robust to all inputs — they handle these cases as if the guarantees never existed,
// with no behavioral dependency on these preconditions.
impl OffsetBasedVersionRange {
    /// Returns the maximum offset (i.e., max version_number - start_version_number) present in this OffsetBasedVersionRange.
    /// If there are no "extra" versions (only start_version_number), returns 0.
    pub fn max_offset(&self) -> u64 {
        match self.max_offset_minus_1() {
            Some(offset_minus_1) => offset_minus_1 + 1,
            None => 0,
        }
    }

    /// Returns the greatest present offset_minus_1 in this OffsetBasedVersionRange.
    /// If there are no "extra" versions (only start_version_number), returns None.
    fn max_offset_minus_1(&self) -> Option<u64> {
        match self {
            OffsetBasedVersionRange::OnlyEnd(offset) => Some(*offset),
            OffsetBasedVersionRange::U32Vector(vec) => vec.last().map(|&v| v as u64),
            OffsetBasedVersionRange::U16Vector(vec) => vec.last().map(|&v| v as u64),
            OffsetBasedVersionRange::Bitmap(bitmap) => {
                for (byte_idx, &byte) in bitmap.iter().enumerate().rev() {
                    if byte != 0 {
                        // then byte.leading_zeros() <= 7
                        let bit_pos = 7 - byte.leading_zeros() as u64;
                        return Some(byte_idx as u64 * 8 + bit_pos);
                    }
                }
                None
            }
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

        let offset_minus_1 = upper_bound - start_version_number - 1;

        match self {
            OffsetBasedVersionRange::OnlyEnd(end_offset_minus_1) => {
                if offset_minus_1 >= *end_offset_minus_1 {
                    Some(start_version_number + end_offset_minus_1 + 1)
                } else {
                    Some(start_version_number)
                }
            }

            OffsetBasedVersionRange::U32Vector(vec) => Some(handle_vec_for_last_le(
                vec,
                start_version_number,
                offset_minus_1,
            )),

            OffsetBasedVersionRange::U16Vector(vec) => Some(handle_vec_for_last_le(
                vec,
                start_version_number,
                offset_minus_1,
            )),

            OffsetBasedVersionRange::Bitmap(bitmap) => {
                let max_bit = offset_minus_1.min(BITMAP_MAX_INDEX);
                let max_byte = (max_bit / 8) as usize;
                let max_bit_in_byte = (max_bit % 8) as u8;

                for byte_idx in (0..=max_byte).rev() {
                    let byte = bitmap[byte_idx];

                    // Generate a mask to handle truncation of the last byte
                    let mask = if byte_idx == max_byte {
                        0xFFu8 >> (8 - (max_bit_in_byte + 1))
                    } else {
                        0xFF
                    };

                    let masked_byte = byte & mask;

                    if masked_byte != 0 {
                        // then masked_byte.leading_zeros() <= 7
                        let bit_pos = 7 - masked_byte.leading_zeros() as u64;
                        let i = byte_idx as u64 * 8 + bit_pos;
                        return Some(start_version_number + i + 1);
                    }
                }

                Some(start_version_number)
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
            versions.push(start_version_number);

            match self {
                OffsetBasedVersionRange::OnlyEnd(end_offset_minus_1) => {
                    let end_version = start_version_number + end_offset_minus_1 + 1;
                    if end_version <= upper_bound {
                        versions.push(end_version);
                    }
                }

                OffsetBasedVersionRange::U32Vector(vec) => {
                    handle_vec_for_collect_le(vec, start_version_number, upper_bound, &mut versions)
                }

                OffsetBasedVersionRange::U16Vector(vec) => {
                    handle_vec_for_collect_le(vec, start_version_number, upper_bound, &mut versions)
                }

                OffsetBasedVersionRange::Bitmap(bitmap) => {
                    if let Some(max_possible_offset_minus_1) =
                        upper_bound.checked_sub(start_version_number + 1)
                    {
                        let max_bit = max_possible_offset_minus_1.min(BITMAP_MAX_INDEX);

                        for i in 0..=max_bit {
                            let byte_idx = (i / 8) as usize;
                            let bit = i % 8;
                            let mask = 1 << bit;
                            if (bitmap[byte_idx] & mask) != 0 {
                                let v = start_version_number + i + 1;
                                versions.push(v);
                            }
                        }
                    }
                }
            }
        }

        versions
    }
}

pub trait SaturatingCastable: Ord + Copy + Into<u64> {
    const DESTINATION_MAX: u64;
    fn saturating_from(value: u64) -> Self;
}

impl SaturatingCastable for u16 {
    const DESTINATION_MAX: u64 = u16::MAX as u64;

    fn saturating_from(value: u64) -> Self {
        value.min(Self::DESTINATION_MAX) as u16
    }
}

impl SaturatingCastable for u32 {
    const DESTINATION_MAX: u64 = u32::MAX as u64;

    fn saturating_from(value: u64) -> Self {
        value.min(Self::DESTINATION_MAX) as u32
    }
}

fn handle_vec_for_last_le<T>(vec: &[T], start_version_number: u64, offset_minus_1: u64) -> u64
where
    T: SaturatingCastable,
{
    let target = T::saturating_from(offset_minus_1);
    match vec.binary_search(&target) {
        Ok(idx) => start_version_number + vec[idx].into() + 1,
        Err(idx) => {
            if idx > 0 {
                start_version_number + vec[idx - 1].into() + 1
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
    for offset_minus_1 in vec {
        let v = start_version_number + (*offset_minus_1).into() + 1;
        if v <= upper_bound {
            versions.push(v);
        } else {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let range = OffsetBasedVersionRange::new();
        assert!(matches!(range, OffsetBasedVersionRange::U16Vector(v) if v.is_empty()));
    }

    #[test]
    fn test_new_with_offset_minus_1() {
        // u16 case
        let range = OffsetBasedVersionRange::new_with_offset_minus_1(100);
        assert!(matches!(range, OffsetBasedVersionRange::U16Vector(v) if v == vec![100]));

        // u32 case
        let range = OffsetBasedVersionRange::new_with_offset_minus_1(70000);
        assert!(matches!(range, OffsetBasedVersionRange::U32Vector(v) if v == vec![70000]));

        // OnlyEnd case
        let large_offset_minus_1 = u32::MAX as u64 + 1;
        let range = OffsetBasedVersionRange::new_with_offset_minus_1(large_offset_minus_1);
        assert!(
            matches!(range, OffsetBasedVersionRange::OnlyEnd(offset_minus_1) if offset_minus_1 == large_offset_minus_1)
        );
    }

    // Test helper to create a Bitmap with specific bits set
    fn create_bitmap_with_bits(bits: &[u64]) -> [u8; VERSION_RANGE_BYTES] {
        let mut bitmap = [0u8; VERSION_RANGE_BYTES];
        for &bit in bits {
            let byte_idx = (bit / 8) as usize;
            let bit_pos = bit % 8;
            bitmap[byte_idx] |= 1 << bit_pos;
        }
        bitmap
    }

    #[test]
    fn test_max_offset() {
        // Test OnlyEnd
        let only_end = OffsetBasedVersionRange::OnlyEnd(100);
        assert_eq!(only_end.max_offset(), 101);

        // Test U32Vector with non-empty and empty cases
        let u32_valid = OffsetBasedVersionRange::U32Vector(vec![10, 20, 30]);
        assert_eq!(u32_valid.max_offset(), 31);
        let u32_empty = OffsetBasedVersionRange::U32Vector(vec![]);
        assert_eq!(u32_empty.max_offset(), 0);

        // Test U16Vector with non-empty and empty cases
        let u16_valid = OffsetBasedVersionRange::U16Vector(vec![5]);
        assert_eq!(u16_valid.max_offset(), 6);
        let u16_empty = OffsetBasedVersionRange::U16Vector(vec![]);
        assert_eq!(u16_empty.max_offset(), 0);

        // Test Bitmap with various bit configurations
        let bitmap_full = create_bitmap_with_bits(&[BITMAP_MAX_INDEX]);
        let bitmap_range = OffsetBasedVersionRange::Bitmap(bitmap_full);
        assert_eq!(bitmap_range.max_offset(), BITMAP_MAX_INDEX + 1);

        let bitmap_empty = OffsetBasedVersionRange::Bitmap([0; VERSION_RANGE_BYTES]);
        assert_eq!(bitmap_empty.max_offset(), 0);

        let bitmap_mid = create_bitmap_with_bits(&[50, 100]);
        let bitmap_mid_range = OffsetBasedVersionRange::Bitmap(bitmap_mid);
        assert_eq!(bitmap_mid_range.max_offset(), 101);
    }

    #[test]
    fn test_last_le() {
        let start = 1000;

        // Test OnlyEnd cases
        let only_end_offset_minus_1 = u32::MAX as u64 + 1;
        let only_end = OffsetBasedVersionRange::OnlyEnd(only_end_offset_minus_1);
        assert_eq!(
            only_end.last_le(start, start + only_end_offset_minus_1 + 2),
            Some(start + only_end_offset_minus_1 + 1)
        );
        assert_eq!(
            only_end.last_le(start, start + only_end_offset_minus_1 + 1),
            Some(start + only_end_offset_minus_1 + 1)
        );
        assert_eq!(only_end.last_le(start, start + 1), Some(start));
        assert_eq!(only_end.last_le(start, start), Some(start));
        assert_eq!(only_end.last_le(start, start - 1), None);

        // Test U32Vector with empty case
        let u32_empty = OffsetBasedVersionRange::U32Vector(vec![]);
        assert_eq!(
            u32_empty.last_le(start, start + u32::MAX as u64 + 2),
            Some(start)
        );
        assert_eq!(u32_empty.last_le(start, start + 1), Some(start));
        assert_eq!(u32_empty.last_le(start, start), Some(start));
        assert_eq!(u32_empty.last_le(start, start - 1), None);

        // Test U32Vector with non-empty case
        let max_u32_entry = u32::MAX;
        let max_u32_entry_as_u64 = max_u32_entry as u64;
        let u32_vec = OffsetBasedVersionRange::U32Vector(vec![50, 100, max_u32_entry]);
        assert_eq!(
            u32_vec.last_le(start, start + max_u32_entry_as_u64 + 2),
            Some(start + max_u32_entry_as_u64 + 1)
        );
        assert_eq!(
            u32_vec.last_le(start, start + max_u32_entry_as_u64 + 1),
            Some(start + max_u32_entry_as_u64 + 1)
        );
        assert_eq!(
            u32_vec.last_le(start, start + max_u32_entry_as_u64),
            Some(start + 100 + 1)
        );
        assert_eq!(u32_vec.last_le(start, start + 50 + 1), Some(start + 50 + 1));
        assert_eq!(u32_vec.last_le(start, start + 50), Some(start));
        assert_eq!(u32_vec.last_le(start, start + 1), Some(start));
        assert_eq!(u32_vec.last_le(start, start), Some(start));
        assert_eq!(u32_vec.last_le(start, start - 1), None);

        // Test U16Vector with empty case
        let u16_empty = OffsetBasedVersionRange::U16Vector(vec![]);
        assert_eq!(
            u16_empty.last_le(start, start + u16::MAX as u64 + 2),
            Some(start)
        );
        assert_eq!(u16_empty.last_le(start, start + 1), Some(start));
        assert_eq!(u16_empty.last_le(start, start), Some(start));
        assert_eq!(u16_empty.last_le(start, start - 1), None);

        // Test U16Vector with non-empty case
        let max_u16_entry = u16::MAX;
        let max_u16_entry_as_u64 = max_u16_entry as u64;
        let u16_vec = OffsetBasedVersionRange::U16Vector(vec![50, 100, max_u16_entry]);
        assert_eq!(
            u16_vec.last_le(start, start + max_u16_entry_as_u64 + 2),
            Some(start + max_u16_entry_as_u64 + 1)
        );
        assert_eq!(
            u16_vec.last_le(start, start + max_u16_entry_as_u64 + 1),
            Some(start + max_u16_entry_as_u64 + 1)
        );
        assert_eq!(
            u16_vec.last_le(start, start + max_u16_entry_as_u64),
            Some(start + 100 + 1)
        );
        assert_eq!(u16_vec.last_le(start, start + 50 + 1), Some(start + 50 + 1));
        assert_eq!(u16_vec.last_le(start, start + 50), Some(start));
        assert_eq!(u16_vec.last_le(start, start + 1), Some(start));
        assert_eq!(u16_vec.last_le(start, start), Some(start));
        assert_eq!(u16_vec.last_le(start, start - 1), None);

        // Test Bitmap with various bit patterns
        let bitmap = create_bitmap_with_bits(&[0, 7, 8, 15]);
        let bitmap_range = OffsetBasedVersionRange::Bitmap(bitmap);
        assert_eq!(
            bitmap_range.last_le(start, start + 15 + 2),
            Some(start + 15 + 1)
        );
        assert_eq!(
            bitmap_range.last_le(start, start + 15 + 1),
            Some(start + 15 + 1)
        );
        assert_eq!(bitmap_range.last_le(start, start + 15), Some(start + 8 + 1));
        assert_eq!(
            bitmap_range.last_le(start, start + 8 + 1),
            Some(start + 8 + 1)
        );
        assert_eq!(
            bitmap_range.last_le(start, start + 7 + 1),
            Some(start + 7 + 1)
        );
        assert_eq!(bitmap_range.last_le(start, start + 7), Some(start + 1));
        assert_eq!(bitmap_range.last_le(start, start + 1), Some(start + 1));
        assert_eq!(bitmap_range.last_le(start, start), Some(start));
        assert_eq!(bitmap_range.last_le(start, start - 1), None);

        // Test Bitmap exceeding index bounds
        let upper = start + BITMAP_MAX_INDEX + 2;
        assert_eq!(bitmap_range.last_le(start, upper), Some(start + 15 + 1));

        // Test bitmap at capacity edge
        let max_bitmap = create_bitmap_with_bits(&[BITMAP_MAX_INDEX]);
        let max_bitmap_range = OffsetBasedVersionRange::Bitmap(max_bitmap);
        assert_eq!(
            max_bitmap_range.last_le(start, start + BITMAP_MAX_INDEX + 2),
            Some(start + BITMAP_MAX_INDEX + 1)
        );
        assert_eq!(
            max_bitmap_range.last_le(start, start + BITMAP_MAX_INDEX + 1),
            Some(start + BITMAP_MAX_INDEX + 1)
        );
        assert_eq!(
            max_bitmap_range.last_le(start, start + BITMAP_MAX_INDEX),
            Some(start)
        );
        assert_eq!(max_bitmap_range.last_le(start, start), Some(start));
        assert_eq!(max_bitmap_range.last_le(start, start - 1), None);
    }

    #[test]
    fn test_collect_versions_le() {
        let start = 1000;

        // Test OnlyEnd cases
        let only_end_offset_minus_1 = u32::MAX as u64 + 1;
        let only_end = OffsetBasedVersionRange::OnlyEnd(only_end_offset_minus_1);
        assert_eq!(
            only_end.collect_versions_le(start, start + only_end_offset_minus_1 + 2),
            vec![start, start + only_end_offset_minus_1 + 1]
        );
        assert_eq!(
            only_end.collect_versions_le(start, start + only_end_offset_minus_1 + 1),
            vec![start, start + only_end_offset_minus_1 + 1]
        );
        assert_eq!(only_end.collect_versions_le(start, start + 1), vec![start]);
        assert_eq!(only_end.collect_versions_le(start, start), vec![start]);
        assert_eq!(only_end.collect_versions_le(start, start - 1), vec![]);

        // Test U32Vector with empty case
        let u32_empty = OffsetBasedVersionRange::U32Vector(vec![]);
        assert_eq!(
            u32_empty.collect_versions_le(start, start + u32::MAX as u64 + 2),
            vec![start]
        );
        assert_eq!(u32_empty.collect_versions_le(start, start + 1), vec![start]);
        assert_eq!(u32_empty.collect_versions_le(start, start), vec![start]);
        assert_eq!(u32_empty.collect_versions_le(start, start - 1), vec![]);

        // Test U32Vector with non-empty case
        let max_u32_entry = u32::MAX;
        let max_u32_entry_as_u64 = max_u32_entry as u64;
        let u32_vec = OffsetBasedVersionRange::U32Vector(vec![50, 100, max_u32_entry]);
        assert_eq!(
            u32_vec.collect_versions_le(start, start + max_u32_entry_as_u64 + 2),
            vec![
                start,
                start + 51,
                start + 101,
                start + max_u32_entry_as_u64 + 1
            ]
        );
        assert_eq!(
            u32_vec.collect_versions_le(start, start + max_u32_entry_as_u64 + 1),
            vec![
                start,
                start + 51,
                start + 101,
                start + max_u32_entry_as_u64 + 1
            ]
        );
        assert_eq!(
            u32_vec.collect_versions_le(start, start + max_u32_entry_as_u64),
            vec![start, start + 51, start + 101]
        );
        assert_eq!(
            u32_vec.collect_versions_le(start, start + 50 + 1),
            vec![start, start + 51]
        );
        assert_eq!(u32_vec.collect_versions_le(start, start + 50), vec![start]);
        assert_eq!(u32_vec.collect_versions_le(start, start + 1), vec![start]);
        assert_eq!(u32_vec.collect_versions_le(start, start), vec![start]);
        assert_eq!(u32_vec.collect_versions_le(start, start - 1), vec![]);

        // Test U16Vector with empty case
        let u16_empty = OffsetBasedVersionRange::U16Vector(vec![]);
        assert_eq!(
            u16_empty.collect_versions_le(start, start + u16::MAX as u64 + 2),
            vec![start]
        );
        assert_eq!(u16_empty.collect_versions_le(start, start + 1), vec![start]);
        assert_eq!(u16_empty.collect_versions_le(start, start), vec![start]);
        assert_eq!(u16_empty.collect_versions_le(start, start - 1), vec![]);

        // Test U16Vector with non-empty case
        let max_u16_entry = u16::MAX;
        let max_u16_entry_as_u64 = max_u16_entry as u64;
        let u16_vec = OffsetBasedVersionRange::U16Vector(vec![50, 100, max_u16_entry]);
        assert_eq!(
            u16_vec.collect_versions_le(start, start + max_u16_entry_as_u64 + 2),
            vec![
                start,
                start + 51,
                start + 101,
                start + max_u16_entry_as_u64 + 1
            ]
        );
        assert_eq!(
            u16_vec.collect_versions_le(start, start + max_u16_entry_as_u64 + 1),
            vec![
                start,
                start + 51,
                start + 101,
                start + max_u16_entry_as_u64 + 1
            ]
        );
        assert_eq!(
            u16_vec.collect_versions_le(start, start + max_u16_entry_as_u64),
            vec![start, start + 51, start + 101]
        );
        assert_eq!(
            u16_vec.collect_versions_le(start, start + 50 + 1),
            vec![start, start + 51]
        );
        assert_eq!(u16_vec.collect_versions_le(start, start + 50), vec![start]);
        assert_eq!(u16_vec.collect_versions_le(start, start + 1), vec![start]);
        assert_eq!(u16_vec.collect_versions_le(start, start), vec![start]);
        assert_eq!(u16_vec.collect_versions_le(start, start - 1), vec![]);

        // Test Bitmap with various bit patterns
        let bitmap = create_bitmap_with_bits(&[0, 7, 8, 15]);
        let bitmap_range = OffsetBasedVersionRange::Bitmap(bitmap);
        assert_eq!(
            bitmap_range.collect_versions_le(start, start + 15 + 2),
            vec![start, start + 1, start + 8, start + 9, start + 16]
        );
        assert_eq!(
            bitmap_range.collect_versions_le(start, start + 15 + 1),
            vec![start, start + 1, start + 8, start + 9, start + 16]
        );
        assert_eq!(
            bitmap_range.collect_versions_le(start, start + 15),
            vec![start, start + 1, start + 8, start + 9]
        );
        assert_eq!(
            bitmap_range.collect_versions_le(start, start + 8 + 1),
            vec![start, start + 1, start + 8, start + 9]
        );
        assert_eq!(
            bitmap_range.collect_versions_le(start, start + 7 + 1),
            vec![start, start + 1, start + 8]
        );
        assert_eq!(
            bitmap_range.collect_versions_le(start, start + 1),
            vec![start, start + 1]
        );
        assert_eq!(bitmap_range.collect_versions_le(start, start), vec![start]);
        assert_eq!(bitmap_range.collect_versions_le(start, start - 1), vec![]);

        // Test Bitmap exceeding index bounds
        let upper = start + BITMAP_MAX_INDEX + 2;
        assert_eq!(
            bitmap_range.collect_versions_le(start, upper),
            vec![start, start + 1, start + 8, start + 9, start + 16]
        );

        // Test bitmap at capacity edge
        let max_bitmap = create_bitmap_with_bits(&[BITMAP_MAX_INDEX]);
        let max_bitmap_range = OffsetBasedVersionRange::Bitmap(max_bitmap);
        assert_eq!(
            max_bitmap_range.collect_versions_le(start, start + BITMAP_MAX_INDEX + 2),
            vec![start, start + BITMAP_MAX_INDEX + 1]
        );
        assert_eq!(
            max_bitmap_range.collect_versions_le(start, start + BITMAP_MAX_INDEX + 1),
            vec![start, start + BITMAP_MAX_INDEX + 1]
        );
        assert_eq!(
            max_bitmap_range.collect_versions_le(start, start + BITMAP_MAX_INDEX),
            vec![start]
        );
        assert_eq!(
            max_bitmap_range.collect_versions_le(start, start),
            vec![start]
        );
        assert_eq!(
            max_bitmap_range.collect_versions_le(start, start - 1),
            vec![]
        );
    }
}
