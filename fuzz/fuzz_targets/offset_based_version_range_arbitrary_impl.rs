use cfx_storage2::middlewares::versioned_flat_key_value::history_indices::{VERSION_RANGE_BYTES, version_range::{OffsetBasedVersionRange, U32_VECTOR_CAPACITY, U16_VECTOR_CAPACITY, bitmap::{Bitmap, ValidBitmap}}};
use arbitrary::{Arbitrary, Result, Unstructured};
use std::collections::HashSet;

#[path = "util_shuffle.rs"]
mod util_shuffle;
use util_shuffle::shuffle_slice;

#[derive(Debug, Arbitrary)]
pub enum FuzzOffsetBasedVersionRange {
    Empty, // U16Vector(vec![])
    NonEmpty(FuzzNonEmptyOffsetBasedVersionRange),
}

impl From<FuzzOffsetBasedVersionRange> for OffsetBasedVersionRange {
    fn from(arb: FuzzOffsetBasedVersionRange) -> Self {
        match arb {
            FuzzOffsetBasedVersionRange::Empty => OffsetBasedVersionRange::U16Vector(vec![]),
            FuzzOffsetBasedVersionRange::NonEmpty(n) => n.into(),
        }
    }
}

#[derive(Debug)]
pub enum FuzzNonEmptyOffsetBasedVersionRange {
    OnlyEnd(u64),
    U32Vector(Vec<u32>),
    U16Vector(Vec<u16>),
    Bitmap(ValidBitmap),
}

impl<'a> Arbitrary<'a> for FuzzNonEmptyOffsetBasedVersionRange {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        // Choose which enum variant to generate.
        let variant_index = u.int_in_range(0..=3)?;

        match variant_index {
            // --- Case 0: Generate a valid `OnlyEnd` ---
            0 => {
                // Constraint: value must be > u32::MAX.
                // Strategy: Directly generate a u64 within the valid range [u32::MAX + 1, u64::MAX].
                // This provides full coverage of all possible valid values.
                
                // Define the inclusive lower bound of the valid range.
                let min_value = (u32::MAX as u64) + 1;

                // Generate a random u64 from the lower bound up to the maximum possible u64 value.
                let value = u.int_in_range(min_value..=u64::MAX)?;
                
                Ok(FuzzNonEmptyOffsetBasedVersionRange::OnlyEnd(value))
            }

            // --- Case 1: Generate a valid `U32Vector` ---
            1 => {
                // Constraints for U32Vector:
                // 1. Not empty: 1 <= len <= U32_VECTOR_CAPACITY.
                // 2. The first element must be > 0.
                // 3. Elements must be strictly monotonically increasing.
                // 4. The last element must be > u16::MAX.

                // --- Constructive Strategy ---

                // Step 1: Determine a valid length `len` for the vector.
                let len = u.int_in_range(1..=U32_VECTOR_CAPACITY)?;

                // Special case: if the length is 1, the logic is very simple.
                if len == 1 {
                    let val = u.int_in_range((u16::MAX as u32 + 1)..=u32::MAX)?;
                    return Ok(FuzzNonEmptyOffsetBasedVersionRange::U32Vector(vec![val]));
                }

                // Step 2: Determine the last element, `last_val`.
                // `last_val` must be > u16::MAX.
                // It also must be at least `len` so that we can pick `len - 1` 
                // distinct numbers from the range `1..last_val`.
                let min_last_val = (u16::MAX as u32 + 1).max(len as u32);
                let last_val = u.int_in_range(min_last_val..=u32::MAX)?;

                // Step 3: Pick `len - 1` unique elements from the range `1..last_val`.
                // We use a HashSet to guarantee uniqueness.
                let mut other_elements = HashSet::with_capacity(len - 1);
                
                // To prevent potential infinite loops if `last_val` is pathologically small
                // (which is extremely unlikely given the constraints, but good practice),
                // we set a reasonable upper bound on the number of attempts.
                // Setting the number of tries to 3 times the number of required elements
                // is a reasonable heuristic.
                let max_tries = (len - 1).saturating_mul(3); 

                for _ in 0..max_tries {
                    if other_elements.len() == len - 1 {
                        break;
                    }
                    // The range is `1..last_val`, so the smallest element will be 1.
                    let val = u.int_in_range(1..=last_val - 1)?;
                    other_elements.insert(val);
                }

                // If we still haven't collected enough unique elements after enough tries,
                // this path is not viable.
                if other_elements.len() != len - 1 {
                    // This signals to libfuzzer that the input is invalid, prompting it
                    // to try a different one. This is more robust than trying to "fix" the data.
                    return Err(arbitrary::Error::IncorrectFormat);
                }

                // Step 4: Combine and sort.
                let mut vec: Vec<u32> = other_elements.into_iter().collect();
                vec.push(last_val);
                vec.sort_unstable(); // `sort_unstable` is generally faster.

                // The vector constructed by the steps above is guaranteed to satisfy all constraints.
                Ok(FuzzNonEmptyOffsetBasedVersionRange::U32Vector(vec))
            }

            // --- Case 2: Generate a valid `U16Vector` (New Implementation) ---
            2 => {
                // Constraints:
                // 1. 1 <= len <= U16_VECTOR_CAPACITY since non-empty.
                // 2. If non-empty, first element > 0.
                // 3. Strictly monotonically increasing.

                // Step 1: Determine a valid length `len` for the vector.
                let len = u.int_in_range(1..=U16_VECTOR_CAPACITY)?;

                // Step 2: Pick `len` unique elements from the range `1..=u16::MAX`.
                // We use a HashSet to guarantee uniqueness efficiently.
                let mut elements = HashSet::with_capacity(len);
                
                // Set a reasonable upper bound on attempts to prevent pathologically
                // slow inputs, even though collisions are very unlikely.
                let max_tries = len.saturating_mul(3);

                for _ in 0..max_tries {
                    if elements.len() == len {
                        break;
                    }
                    // The range `1..=u16::MAX` ensures all elements are > 0.
                    let val = u.int_in_range(1..=u16::MAX)?;
                    elements.insert(val);
                }

                // If we couldn't collect enough unique elements, fail this input.
                if elements.len() != len {
                    return Err(arbitrary::Error::IncorrectFormat);
                }

                // Step 3: Convert to a vector and sort it to ensure it's increasing.
                let mut vec: Vec<u16> = elements.into_iter().collect();
                vec.sort_unstable();

                // The constructed vector is guaranteed to be valid.
                Ok(FuzzNonEmptyOffsetBasedVersionRange::U16Vector(vec))
            }

            // --- Case 3: Generate a valid `Bitmap` ---
            _ => { // case 3
                // Constraints:
                // 1. Bit 0 must always be 1.
                // 2. The total number of set bits (ones) must be > U16_VECTOR_CAPACITY.

                // Strategy (Constructive Method with fixed bit 0):
                // 1. Start with a bitmap where bit 0 is already set. This accounts for one '1'.
                // 2. Decide how many *additional* '1's to set. The total must be > 10.
                // 3. Create a list of all *other* possible bit positions (U16_VECTOR_CAPACITY..=(8 * VERSION_RANGE_BYTES - 1)).
                // 4. Shuffle this list of available positions.
                // 5. Take the required number of additional positions from the shuffled list and set those bits.
                
                // 1. Determine the total number of '1's to generate.
                // The count must be > U16_VECTOR_CAPACITY and <= 8 * VERSION_RANGE_BYTES.
                let min_total_ones = U16_VECTOR_CAPACITY + 1;
                let max_total_ones = 8 * VERSION_RANGE_BYTES;
                let total_ones_to_set = u.int_in_range(min_total_ones..=max_total_ones)?;

                // 2. Since bit 0 is fixed as '1', we need to set (total_ones_to_set - 1) more bits.
                let additional_ones_to_set = total_ones_to_set - 1;

                // 3. Create a list of available positions for the additional bits.
                let mut available_positions: Vec<u16> = (1..8 * VERSION_RANGE_BYTES as u16).collect();

                // 4. Shuffle this list randomly.
                shuffle_slice(u, &mut available_positions)?;

                // 5. Start with a bitmap with only bit 0 set.
                let mut data = [0u8; VERSION_RANGE_BYTES];
                data[0] |= 1; // Set bit 0. (byte_index=0, bit_in_byte_index=0)

                // 6. Set the additional bits from the shuffled list.
                for &bit_pos_to_set in available_positions.iter().take(additional_ones_to_set) {
                    let byte_index = (bit_pos_to_set / 8) as usize;
                    let bit_in_byte_index = (bit_pos_to_set % 8) as u8;
                    data[byte_index] |= 1 << bit_in_byte_index;
                }
                
                Ok(FuzzNonEmptyOffsetBasedVersionRange::Bitmap(Bitmap::try_new(data).unwrap()))
            }
        }
    }
}

impl From<FuzzNonEmptyOffsetBasedVersionRange> for OffsetBasedVersionRange {
    fn from(arb: FuzzNonEmptyOffsetBasedVersionRange) -> Self {
        match arb {
            FuzzNonEmptyOffsetBasedVersionRange::OnlyEnd(o) => OffsetBasedVersionRange::OnlyEnd(o),
            FuzzNonEmptyOffsetBasedVersionRange::U32Vector(v) => OffsetBasedVersionRange::U32Vector(v),
            FuzzNonEmptyOffsetBasedVersionRange::U16Vector(v) => OffsetBasedVersionRange::U16Vector(v),
            FuzzNonEmptyOffsetBasedVersionRange::Bitmap(b) => OffsetBasedVersionRange::Bitmap(b),
        }
    }
}
