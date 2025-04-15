use crate::middlewares::HistoryNumber; // u64

/// Represents the set of version numbers in a value's history.
/// By design, the `start_version_number` of a record overlaps with the `end_version_number` of the previous record.
#[derive(Clone, Debug)]
pub enum HistoryIndices<V: Clone> {
    /// The latest record in the history.
    /// - `start_version_number`: The start version number of this range.
    /// - `one_range`: The structure encoding all version numbers in this range.
    /// - `latest_v`: Optionally, the value at the latest version.
    Latest((HistoryNumber, OneRange, Option<V>)),
    /// Previous (non-latest) record in the history.
    /// - `one_range`: The structure encoding all version numbers in this range.
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
    /// The `last` function finds the **largest version number** `found_version_number` such that
    /// `found_version_number <= version_number` and `found_version_number` exists in this history range.
    ///
    /// - The `&self` always references the record range with the **smallest `end_version_number` such that
    ///   `end_version_number >= version_number`**.
    /// - There are two cases:
    ///     1. **If a previous record exists:**  
    ///         The `start_version_number` of the current record is exactly the `end_version_number` of the previous record.  
    ///         The previous record's `end_version_number` is strictly less than `version_number`, so  
    ///         `start_version_number < version_number <= end_version_number` always holds.  
    ///         So, the largest existing `found_version_number <= version_number` can be found in this range.
    ///     2. **If no previous record exists:**  
    ///         - Still, `version_number <= end_version_number`.
    ///         - If `version_number < start_version_number`: No `found_version_number <= version_number` exists (return `None`).
    ///         - Else (`start_version_number <= version_number`): Find the largest existing `found_version_number <= version_number` in this range.
    ///
    /// # Returns
    /// - `Some(version_number)` if such a version exists.
    /// - `None` if no such version is found.
    pub fn last(&self, version_number: HistoryNumber, end_version_number: HistoryNumber) -> Option<HistoryNumber> {
        match self {
            HistoryIndices::Latest((start_version_number, one_range, _)) => {
                assert!(end_version_number == LATEST);

                one_range.last_le(*start_version_number, version_number)
            },
            HistoryIndices::Previous(one_range) => {
                assert!(end_version_number < LATEST);
                let start_version_number = end_version_number - one_range.max_offset();

                one_range.last_le(start_version_number, version_number)
            },
        }
    }
}

const ONE_RANGE_BYTES_LOG: usize = 6; // 6 for 64 bytes, or 7 for 128 bytes
pub const ONE_RANGE_BYTES: usize = 1 << ONE_RANGE_BYTES_LOG;
pub const LATEST: u64 = u64::MAX;
const _: () = assert!(ONE_RANGE_BYTES == 64 || ONE_RANGE_BYTES == 128);

/// `OneRange` encodes which version numbers exist within a history range.
/// The data structures are optimized for different densities and ranges:
/// By default, `start_version_number` is always present and is not explicitly recorded.
/// The following offsets are all in terms of `offset_minus_1`, which is (version_number - start_version_number - 1).
///
/// - `OnlyEnd(u64)`:
///     - Special case for when the only version number (besides `start_version_number`)
///       is an `end_version_number` whose `offset_minus_1` (i.e., end - start - 1) exceeds `u32::MAX`.
///
/// - `Four(Vec<u32>)`:
///     - Used when the maximum `offset_minus_1` is in `2^16 ..= 2^32-1` and there are <= (ONE_RANGE_BYTES / 4) version numbers (excluding `start_version_number`).
///     - `offset_minus_1` values are stored as `u32` in increasing order; each entry is (version_number - start_version_number - 1).
///     - The `Vec<u32>` must not be empty.
///
/// - `Two(Vec<u16>)`:
///     - Used when the maximum `offset_minus_1` is in `0 ..= 2^16-1` and there are <= (ONE_RANGE_BYTES / 2) version numbers (excluding `start_version_number`).
///     - `offset_minus_1` values are stored as `u16` in increasing order; each entry is (version_number - start_version_number - 1).
///     - The `Vec<u16>` can be empty.
///
/// - `Bitmap([u8; ONE_RANGE_BYTES])`:
///     - Used when the maximum `offset_minus_1` is in `0 ..= (ONE_RANGE_BYTES * 8 - 1)` and there are > (ONE_RANGE_BYTES / 2) version numbers (excluding `start_version_number`).
///     - Each bit at index i indicates the existence of `start_version_number + i + 1`.
///         Specifically, the `i`th bit is the `(i % 8)`-th **least significant bit** (LSB) in `bits[i / 8]`.
///     - There is at least one bit exists.
#[derive(Debug, Clone, PartialEq)]
pub enum OneRange {
    OnlyEnd(u64),
    Four(Vec<u32>),
    Two(Vec<u16>),
    Bitmap([u8; ONE_RANGE_BYTES]),
}

impl OneRange {
    /// Returns the maximum offset (i.e., max version_number - start_version_number) present in this OneRange.
    /// If there are no "extra" versions (only start_version_number), returns 0.
    pub fn max_offset(&self) -> u64 {
        match self.max_offset_minus_1() {
            Some(offset_minus_1) => offset_minus_1 + 1,
            None => 0,
        }
    }

    /// Returns the greatest present offset_minus_1 in this OneRange.
    /// If there are no "extra" versions (only start_version_number), returns None.
    // By design, the `vec` in `Four` is guaranteed to be non-empty, and `Bitmap` is guaranteed to not be all zeros.
    // However, this function is robust: if these cases ever occur, it will safely return `None`.
    fn max_offset_minus_1(&self) -> Option<u64> {
        match self {
            OneRange::OnlyEnd(offset) => Some(*offset),
            OneRange::Four(vec) => vec.last().map(|&v| v as u64),
            OneRange::Two(vec) => vec.last().map(|&v| v as u64),
            OneRange::Bitmap(bitmap) => {
                for (byte_idx, &byte) in bitmap.iter().enumerate().rev() {
                    if byte != 0 { // then byte.leading_zeros() <= 7
                        let bit_pos = 7 - byte.leading_zeros() as u64;
                        return Some(byte_idx as u64 * 8 + bit_pos);
                    }
                }
                None
            }
        }
    }

    /// Finds the largest version number in this range such that
    /// start_version_number <= version <= upper_bound, and version exists.
    // By design, the `vec` in `Four` is guaranteed to be non-empty, and `Bitmap` is guaranteed to not be all zeros.
    // However, this function is robust: if these cases ever occur, it will safely return `Some(start_version_number)`.
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
            OneRange::OnlyEnd(end_offset_minus_1) => {
                if offset_minus_1 >= *end_offset_minus_1 {
                    Some(start_version_number + end_offset_minus_1 + 1)
                } else {
                    Some(start_version_number)
                }
            }

            OneRange::Four(vec) => {
                // Binary search for the largest element <= offset_minus_1
                match vec.binary_search(&(offset_minus_1 as u32)) {
                    Ok(idx) => Some(start_version_number + vec[idx] as u64 + 1),
                    Err(idx) => {
                        if idx > 0 {
                            Some(start_version_number + vec[idx - 1] as u64 + 1)
                        } else {
                            Some(start_version_number)
                        }
                    }
                }
            }

            OneRange::Two(vec) => {
                // Binary search for the largest element <= offset_minus_1
                match vec.binary_search(&(offset_minus_1 as u16)) {
                    Ok(idx) => Some(start_version_number + vec[idx] as u64 + 1),
                    Err(idx) => {
                        if idx > 0 {
                            Some(start_version_number + vec[idx - 1] as u64 + 1)
                        } else {
                            Some(start_version_number)
                        }
                    }
                }
            }

            OneRange::Bitmap(bitmap) => {
                let max_bit = offset_minus_1.min((ONE_RANGE_BYTES * 8 - 1) as u64);
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
                    
                    if masked_byte != 0 { // then masked_byte.leading_zeros() <= 7
                        let bit_pos = 7 - masked_byte.leading_zeros() as u64;
                        let i = byte_idx as u64 * 8 + bit_pos;
                        return Some(start_version_number + i + 1);
                    }
                }

                Some(start_version_number)
            }
        }
    }
}