use super::{PushError, VERSION_RANGE_BYTES};

/// The bit at index 0 should always be set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bitmap {
    data: [u8; VERSION_RANGE_BYTES],
}

/// Maximum index represented in [`Bitmap`].
pub const BITMAP_MAX_INDEX: u16 = VERSION_RANGE_BYTES as u16 * 8 - 1;

impl Bitmap {
    /// - Will panic if element in vec > BITMAP_MAX_INDEX.
    /// - The bit at index 0 will be set regardless of whether there is 0 in vec or not.
    pub fn new_from_vec(vec: &[u16]) -> Self {
        let mut bitmap = [0u8; VERSION_RANGE_BYTES];

        // Bitmap must have bit 0 set
        bitmap[0] |= 1;

        for &bit in vec {
            let byte_idx = (bit / 8) as usize;
            let bit_pos = bit % 8;
            bitmap[byte_idx] |= 1 << bit_pos;
        }

        Bitmap { data: bitmap }
    }

    #[cfg(test)]
    /// - Will panic if element in vec > BITMAP_MAX_INDEX.
    /// - Invalid: the bit at index 0 will not be set regardless of whether there is 0 in vec or not.
    pub fn new_invalid_from_vec(vec: &[u16]) -> Self {
        let mut bitmap = [0u8; VERSION_RANGE_BYTES];

        for &bit in vec {
            if bit > 0 {
                let byte_idx = (bit / 8) as usize;
                let bit_pos = bit % 8;
                bitmap[byte_idx] |= 1 << bit_pos;
            }
        }

        Bitmap { data: bitmap }
    }

    /// Collects the indices of all set bits in increasing order.
    #[cfg(test)]
    pub fn to_vec(&self) -> Vec<u16> {
        let mut indices = Vec::new();

        for (byte_idx, &byte) in self.data.iter().enumerate() {
            let mut mut_byte = byte;
            while mut_byte != 0 {
                let index_in_byte = mut_byte.trailing_zeros();
                indices.push(byte_idx as u16 * 8 + index_in_byte as u16);
                mut_byte &= mut_byte - 1;
            }
        }

        assert!(!indices.is_empty());
        assert_eq!(indices[0], 0);
        assert!(indices[1..].iter().all(|element| *element > 0));
        assert!(indices.windows(2).all(|window| window[0] < window[1]));
        assert!(indices.iter().all(|element| *element <= BITMAP_MAX_INDEX));

        indices
    }

    /// Finds the index of the maximum set bit.
    pub fn max_bit(&self) -> u64 {
        for (byte_idx, &byte) in self.data.iter().enumerate().rev() {
            if byte != 0 {
                // then byte.leading_zeros() <= 7
                let bit_pos = 7 - byte.leading_zeros() as u64;
                return byte_idx as u64 * 8 + bit_pos;
            }
        }

        unreachable!("Bitmap must have bit 0 set");
    }

    /// Finds the index of the largest set bit that is <= upper_bound.
    pub fn last_le(&self, upper_bound: u64) -> u64 {
        let index_upper_bound = upper_bound.min(BITMAP_MAX_INDEX as u64);
        let max_byte_idx = (index_upper_bound / 8) as usize;
        let max_bit_in_last_byte = (index_upper_bound % 8) as u8;

        for byte_idx in (0..=max_byte_idx).rev() {
            let byte = self.data[byte_idx];

            // Generate a mask to handle truncation of the last byte
            let mask = if byte_idx == max_byte_idx {
                0xFFu8 >> (8 - (max_bit_in_last_byte + 1))
            } else {
                0xFF
            };

            let masked_byte = byte & mask;

            if masked_byte != 0 {
                // then masked_byte.leading_zeros() <= 7
                let bit_pos = 7 - masked_byte.leading_zeros() as u64;
                return byte_idx as u64 * 8 + bit_pos;
            }
        }

        unreachable!("Bitmap must have bit 0 set");
    }

    /// Collects the indices of all set bits that are <= upper_bound, in increasing order.
    pub fn collect_le(&self, upper_bound: u64) -> Vec<u64> {
        let mut indices = Vec::new();

        for (byte_idx, &byte) in self.data.iter().enumerate() {
            let start_idx_for_byte = byte_idx as u64 * 8;
            let end_idx_for_byte = start_idx_for_byte + 7;

            // Pre-filter: Skip bytes that are entirely above the upper bound
            if start_idx_for_byte > upper_bound {
                break;
            }

            // Completely within range: Add all set bits directly
            if end_idx_for_byte <= upper_bound {
                let mut mut_byte = byte;
                while mut_byte != 0 {
                    let index_in_byte = mut_byte.trailing_zeros();
                    indices.push(start_idx_for_byte + index_in_byte as u64);
                    mut_byte &= mut_byte - 1;
                }
                continue;
            }

            // Partially within range: Check each bit individually
            let mut mut_byte = byte;
            while mut_byte != 0 {
                let index_in_byte = mut_byte.trailing_zeros();
                let index = start_idx_for_byte + index_in_byte as u64;
                if index <= upper_bound {
                    indices.push(index);
                } else {
                    break;
                }
                mut_byte &= mut_byte - 1;
            }
        }

        indices
    }

    pub fn count_ones(&self) -> usize {
        self.data
            .iter()
            .map(|&byte| byte.count_ones() as usize)
            .sum()
    }

    /// Returns an error if the bit at index 0 is not set.
    pub fn validate(&self) -> Result<(), PushError> {
        if (self.data[0] & 1) == 0 {
            Err(PushError::InvalidState)
        } else {
            Ok(())
        }
    }

    /// Set an index, without checking whether this index has already been set.
    ///
    /// # Behavior
    ///
    /// - If the index does not exceed the maximum allowed index ([`BITMAP_MAX_INDEX`]):
    ///   - Modifies `self` to include this index.
    ///   - Returns `true`.
    /// - If the index exceeds the maximum allowed index:
    ///   - Leaves `self` unchanged.
    ///   - Returns `false`.
    pub fn set_unchecked(&mut self, index: u64) -> bool {
        if index > (BITMAP_MAX_INDEX as u64) {
            return false;
        }

        let idx = index as usize;
        let byte = idx / 8;
        let bit = idx % 8;
        self.data[byte] |= 1 << bit;
        true
    }
}

impl Bitmap {
    pub fn new(data: [u8; VERSION_RANGE_BYTES]) -> Self {
        Bitmap { data }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use super::super::{BITMAP_MAX_INDEX, U16_VECTOR_CAPACITY, VERSION_RANGE_BYTES};
    use super::*;

    #[test]
    fn test_new_from_vec_empty() {
        let bitmap = Bitmap::new_from_vec(&[]);
        assert_eq!(bitmap.data[0], 0b00000001);
        for i in 1..VERSION_RANGE_BYTES {
            assert_eq!(bitmap.data[i], 0);
        }
        assert_eq!(bitmap.to_vec(), vec![0]);
    }

    #[test]
    fn test_new_from_vec_with_bits() {
        let vec = vec![0, 1, 8];
        let bitmap = Bitmap::new_from_vec(&vec);
        assert_eq!(bitmap.data[0], 0b00000011);
        assert_eq!(bitmap.data[1], 0b00000001);
        assert_eq!(bitmap.to_vec(), vec![0, 1, 8]);
    }

    #[test]
    #[should_panic]
    fn test_new_from_vec_panic() {
        let vec = vec![BITMAP_MAX_INDEX + 1];
        Bitmap::new_from_vec(&vec);
    }

    #[test]
    fn test_to_vec() {
        let bitmap = Bitmap::new_from_vec(&[0, 3, 7, 8]);
        assert_eq!(bitmap.to_vec(), vec![0, 3, 7, 8]);

        let bitmap = Bitmap::new_from_vec(&[7, 8, 3, 7]);
        assert_eq!(bitmap.to_vec(), vec![0, 3, 7, 8]);
    }

    fn test_max_bit_method(input: &[u16]) {
        let bitmap = Bitmap::new_from_vec(input);
        let max_bit = bitmap.max_bit();
        let vec_from_bitmap = bitmap.to_vec();
        let last_element = *vec_from_bitmap.last().unwrap() as u64;

        assert_eq!(
            max_bit, last_element,
            "max_bit() failed: max_bit ({}) != last element ({}) for input {:?}",
            max_bit, last_element, input
        );
    }

    fn test_count_ones_method(input: &[u16]) {
        let bitmap = Bitmap::new_from_vec(input);
        let vec_from_bitmap = bitmap.to_vec();
        let num_bits = bitmap.count_ones();
        let vec_len = vec_from_bitmap.len();

        assert_eq!(
            num_bits, vec_len,
            "count_ones() failed: num_bits ({}) != vec_len ({}) for input {:?}",
            num_bits, vec_len, input
        );
    }

    fn test_validate_method(input: &[u16]) {
        // valid case
        let bitmap = Bitmap::new_from_vec(input);
        bitmap.validate().unwrap();

        // invalid case
        let bitmap_without_0 = Bitmap::new_invalid_from_vec(input);
        bitmap_without_0.validate().unwrap_err();
    }

    fn test_set_unchecked_method(input: &[u16]) {
        let bitmap = Bitmap::new_from_vec(input);
        let vec_from_bitmap = bitmap.to_vec();
        let last_element = *vec_from_bitmap.last().unwrap() as u64;

        for offset in 0..(BITMAP_MAX_INDEX as u64 + 3) {
            let mut bitmap_mut = bitmap.clone();
            let success = bitmap_mut.set_unchecked(offset);
            let should_success = offset <= BITMAP_MAX_INDEX as u64;
            assert_eq!(
                success, should_success,
                "set_unchecked() failed: success ({}) != should_success ({}) for input {:?}",
                success, should_success, input
            );

            if success {
                let vec_after_push = bitmap_mut.to_vec();

                let mut expected_vec_after_push = vec_from_bitmap.clone();
                expected_vec_after_push.push(offset as u16);
                expected_vec_after_push.sort_unstable();
                expected_vec_after_push.dedup();

                assert_eq!(
                    vec_after_push, expected_vec_after_push,
                    "set_unchecked() failed: vec_after_push ({:?}) != expected_vec_after_push ({:?}) for input {:?}",
                    vec_after_push, expected_vec_after_push, input
                );
            }
        }
    }

    #[derive(Debug, Clone)]
    struct BitmapTestCase {
        bitmap: Bitmap,
        last_le_cases: Vec<(u64, u64)>,
        collect_le_cases: Vec<(u64, Vec<u64>)>,
    }

    fn test_le_cases(input_vec: Vec<u16>) -> BitmapTestCase {
        let bitmap = Bitmap::new_from_vec(&input_vec);
        let vec = bitmap.to_vec();

        let mut targets = vec![];

        let mut last_le = vec![];

        let mut collect_le = vec![];

        for target_bitmap_index in 0..BITMAP_MAX_INDEX + 3 {
            let end_idx_excluded = match vec.binary_search(&target_bitmap_index) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            };
            let bitmap_indices: Vec<_> = vec[..end_idx_excluded]
                .iter()
                .map(|offset| *offset as u64)
                .collect();
            targets.push(target_bitmap_index as u64);
            last_le.push(bitmap_indices.last().cloned().unwrap());
            collect_le.push(bitmap_indices);
        }

        assert_eq!(targets.len(), last_le.len());
        assert_eq!(targets.len(), collect_le.len());

        BitmapTestCase {
            bitmap,
            last_le_cases: targets.clone().into_iter().zip(last_le).collect(),
            collect_le_cases: targets.into_iter().zip(collect_le).collect(),
        }
    }

    fn test_le_method<F, R>(bitmap: &Bitmap, cases: &[(u64, R)], method: F)
    where
        F: Fn(&Bitmap, u64) -> R,
        R: PartialEq + std::fmt::Debug,
    {
        for &(target, ref expected) in cases {
            assert_eq!(
                method(bitmap, target),
                *expected,
                "target={} bitmap={:?}",
                target,
                bitmap
            );
        }
    }

    fn test_bitmap_method(vec: Vec<u16>) {
        test_max_bit_method(&vec);

        test_count_ones_method(&vec);

        test_validate_method(&vec);

        test_set_unchecked_method(&vec);

        let BitmapTestCase {
            bitmap,
            last_le_cases: last_cases,
            collect_le_cases: collect_cases,
        } = test_le_cases(vec);
        test_le_method(&bitmap, &last_cases, |r, t| r.last_le(t));
        test_le_method(&bitmap, &collect_cases, |r, t| r.collect_le(t));
    }

    #[test]
    fn test_bitmap() {
        test_bitmap_method(vec![0, 5, 15]);
        test_bitmap_method(vec![0]);
        test_bitmap_method(vec![]);
        test_bitmap_method(vec![0, BITMAP_MAX_INDEX]);
        test_bitmap_method(vec![4, 5, 6, 7]);
        test_bitmap_method(vec![0, 7, 8, 15]);
        test_bitmap_method(
            ((BITMAP_MAX_INDEX - U16_VECTOR_CAPACITY as u16)..=BITMAP_MAX_INDEX).collect(),
        );
        test_bitmap_method((0..=U16_VECTOR_CAPACITY as u16 + 1).collect());
        test_bitmap_method((0..=BITMAP_MAX_INDEX).collect());
        test_bitmap_method((0..=BITMAP_MAX_INDEX / 2).map(|x| x * 2).collect());
        test_bitmap_method((0..=BITMAP_MAX_INDEX / 3).map(|x| x * 3).collect());
        test_bitmap_method(
            ((BITMAP_MAX_INDEX - U16_VECTOR_CAPACITY as u16 + 1)..=BITMAP_MAX_INDEX).collect(),
        );
    }
}
