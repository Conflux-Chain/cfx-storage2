use super::VERSION_RANGE_BYTES;
use crate::errors::Result;
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
                // TODO: test HistoryIndices::last_le for a Bitmap
                let max_bit = offset_minus_1.min(BITMAP_MAX_INDEX);
                let max_byte = (max_bit / 8) as usize;
                let max_bit_in_byte = (max_bit % 8) as u8;

                for byte_idx in (0..=max_byte).rev() {
                    let byte = bitmap[byte_idx];

                    // Generate a mask to handle truncation of the last byte
                    let mask = if byte_idx == max_byte {
                        (1 << (max_bit_in_byte + 1)) - 1
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
    ) -> Result<Vec<HistoryNumber>> {
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

        Ok(versions)
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
    // TODO: test HistoryIndices::last_le a version_number > Latest record's start_version_number + T::MAX + 1 for empty/non-empty vec
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
