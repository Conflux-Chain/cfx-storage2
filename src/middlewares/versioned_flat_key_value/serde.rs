use std::borrow::Cow;

use super::history_indices_table::{OneRange, ONE_RANGE_BYTES};
use super::{history_indices_table::HistoryIndices, HistoryIndexKey};
use crate::backends::serde::{Decode, Encode, EncodeSubKey, FixedLengthEncoded};
use crate::errors::{DecResult, DecodeError};
use crate::middlewares::HistoryNumber;

impl<K: Clone + Encode> Encode for HistoryIndexKey<K> {
    fn encode(&self) -> Cow<[u8]> {
        let encoded_key = self.0.encode();
        let encoded_version = self.1.encode();

        Cow::Owned([encoded_key.as_ref(), encoded_version.as_ref()].concat())
    }
}

impl<K: Clone + FixedLengthEncoded> FixedLengthEncoded for HistoryIndexKey<K> {
    const LENGTH: usize = K::LENGTH + std::mem::size_of::<HistoryNumber>();
}

impl<K: Clone + Encode + ToOwned<Owned = K>> EncodeSubKey for HistoryIndexKey<K> {
    const HAVE_SUBKEY: bool = true;

    fn encode_subkey(&self) -> (Cow<[u8]>, Cow<[u8]>) {
        (self.0.encode(), self.1.encode())
    }

    fn encode_subkey_owned(input: <Self as ToOwned>::Owned) -> (Vec<u8>, Vec<u8>) {
        (
            K::encode_owned(input.0),
            HistoryNumber::encode_owned(input.1),
        )
    }
}

impl<K: Clone + Decode + ToOwned<Owned = K>> Decode for HistoryIndexKey<K> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        const BYTES: usize = std::mem::size_of::<HistoryNumber>();
        if input.len() < BYTES {
            return Err(DecodeError::IncorrectLength);
        }

        let (key_raw, version_raw) = input.split_at(input.len() - BYTES);
        let (key, version) = (K::decode(key_raw)?, HistoryNumber::decode(version_raw)?);
        Ok(Cow::Owned(HistoryIndexKey(
            key.into_owned(),
            version.into_owned(),
        )))
    }

    fn decode_owned(mut input: Vec<u8>) -> DecResult<Self> {
        const BYTES: usize = std::mem::size_of::<HistoryNumber>();
        if input.len() < BYTES {
            return Err(DecodeError::IncorrectLength);
        }

        let version_raw = input.split_off(input.len() - BYTES);
        let key_raw = input;
        let key = K::decode_owned(key_raw)?;
        let version = HistoryNumber::decode_owned(version_raw)?;
        Ok(HistoryIndexKey(key, version))
    }
}

impl<V: Clone + Encode> Encode for HistoryIndices<V> {
    fn encode(&self) -> Cow<[u8]> {
        let mut buffer = Vec::new();
        match self {
            Self::Latest((version, range, v)) => {
                range.encode_impl(&mut buffer, v.is_none());
                buffer.extend(version.to_be_bytes());
                if let Some(value) = v {
                    buffer.extend(value.encode().into_owned());
                }
            }
            Self::Previous(range) => {
                range.encode_impl(&mut buffer, true);
            }
        }
        Cow::Owned(buffer)
    }
}

impl<V: Clone + Decode + ToOwned<Owned = V>> Decode for HistoryIndices<V> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        let (one_range, consumed, value_is_none) = OneRange::decode_impl(input)?;
        if input.len() == consumed {
            if let OneRange::Two(ref vec) = one_range {
                if vec.is_empty() {
                    return Err(DecodeError::Custom(
                        "Two vector should not be empty in Previous",
                    ));
                }
            }
            if !value_is_none {
                return Err(DecodeError::Custom(
                    "For Previous, value_is_none should always be true",
                ));
            }
            Ok(Cow::Owned(Self::Previous(one_range)))
        } else {
            let version_start = consumed;
            let version_end = version_start + 8;
            if version_end > input.len() {
                return Err(DecodeError::IncorrectLength);
            }
            let version = u64::from_be_bytes(input[version_start..version_end].try_into().unwrap());
            if value_is_none {
                if version_end < input.len() {
                    return Err(DecodeError::IncorrectLength);
                }
                Ok(Cow::Owned(Self::Latest((version, one_range, None))))
            } else {
                let v_raw = input[version_end..].to_vec();
                let v = V::decode(&v_raw)?;
                Ok(Cow::Owned(Self::Latest((
                    version,
                    one_range,
                    Some(v.into_owned()),
                ))))
            }
        }
    }
}

/*
Encoding scheme:

value_is_none = true:
00 000000 | OnlyEnd
01 000000 | Two(64)
01 000001-01 100000 | Four(1-32)
10 000000-10 111111 | Two(0-63)
11 000000 | Bitmap

value_is_none = false:
00 000001-00 100000 | Four(1-32)
00 111100 | Bitmap
00 111101 | Two(0)
00 111110 | Two(64)
00 111111 | OnlyEnd
11 000001-11 111111 | Two(1-63)

Safety: ONE_RANGE_BYTES <= 128
*/

impl OneRange {
    fn encode_impl(&self, buffer: &mut Vec<u8>, value_is_none: bool) {
        match (self, value_is_none) {
            // OnlyEnd cases
            (Self::OnlyEnd(end), true) => {
                buffer.push(0b00 << 6);
                buffer.extend(end.to_be_bytes());
            }
            (Self::OnlyEnd(end), false) => {
                buffer.push(0b00 << 6 | 0b111111);
                buffer.extend(end.to_be_bytes());
            }

            // Four cases
            (Self::Four(vec), true) => {
                let len = vec.len();
                if len == 0 || len > ONE_RANGE_BYTES / 4 {
                    panic!("Invalid Four length");
                }
                buffer.push(0b01 << 6 | len as u8);
                for version in vec {
                    buffer.extend(version.to_be_bytes());
                }
            }
            (Self::Four(vec), false) => {
                let len = vec.len();
                if len == 0 || len > ONE_RANGE_BYTES / 4 {
                    panic!("Invalid Four length");
                }
                buffer.push(0b00 << 6 | len as u8);
                for version in vec {
                    buffer.extend(version.to_be_bytes());
                }
            }

            // Two cases
            (Self::Two(vec), true) => {
                let len = vec.len();
                if len > ONE_RANGE_BYTES / 2 {
                    panic!("Invalid Two length");
                }
                if len == 64 {
                    buffer.push(0b01 << 6);
                } else {
                    buffer.push(0b10 << 6 | len as u8);
                }
                for version in vec {
                    buffer.extend(version.to_be_bytes());
                }
            }
            (Self::Two(vec), false) => {
                let len = vec.len();
                if len > ONE_RANGE_BYTES / 2 {
                    panic!("Invalid Two length");
                }
                if len == 0 {
                    buffer.push(0b00 << 6 | 0b111101);
                } else if len == 64 {
                    buffer.push(0b00 << 6 | 0b111110);
                } else {
                    buffer.push(0b11 << 6 | len as u8);
                }
                for version in vec {
                    buffer.extend(version.to_be_bytes());
                }
            }

            // Bitmap cases
            (Self::Bitmap(bits), true) => {
                buffer.push(0b11 << 6);
                buffer.extend_from_slice(bits);
            }
            (Self::Bitmap(bits), false) => {
                buffer.push(0b00 << 6 | 0b111100);
                buffer.extend_from_slice(bits);
            }
        }
    }

    fn decode_impl(input: &[u8]) -> DecResult<(Self, usize, bool)> {
        if input.is_empty() {
            return Err(DecodeError::IncorrectLength);
        }

        let first = input[0];
        let tag = first >> 6;
        let len_or_end = first & 0x3F;
        let mut offset = 1;
        let mut value_is_none = true;

        let one_range = match tag {
            0b00 => match len_or_end {
                0b000000 => {
                    // OnlyEnd (value_is_none = true)
                    if offset + 8 > input.len() {
                        return Err(DecodeError::IncorrectLength);
                    }
                    let end = u64::from_be_bytes(input[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    Self::OnlyEnd(end)
                }
                0b111100 => {
                    // Bitmap (value_is_none = false)
                    value_is_none = false;
                    if offset + ONE_RANGE_BYTES > input.len() {
                        return Err(DecodeError::IncorrectLength);
                    }
                    let bits = input[offset..offset + ONE_RANGE_BYTES]
                        .try_into()
                        .map_err(|_| DecodeError::IncorrectLength)?;
                    offset += ONE_RANGE_BYTES;
                    Self::Bitmap(bits)
                }
                0b111101 => {
                    // Two(0) (value_is_none = false)
                    value_is_none = false;
                    Self::Two(Vec::new())
                }
                0b111110 => {
                    // Two(64) (value_is_none = false)
                    value_is_none = false;
                    if offset + 64 * 2 > input.len() {
                        return Err(DecodeError::IncorrectLength);
                    }
                    let vec = input[offset..offset + 128]
                        .chunks_exact(2)
                        .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
                        .collect();
                    offset += 128;
                    Self::Two(vec)
                }
                0b111111 => {
                    // OnlyEnd (value_is_none = false)
                    value_is_none = false;
                    if offset + 8 > input.len() {
                        return Err(DecodeError::IncorrectLength);
                    }
                    let end = u64::from_be_bytes(input[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    Self::OnlyEnd(end)
                }
                len => {
                    // Four (value_is_none = false)
                    value_is_none = false;
                    if len == 0 || len as usize > ONE_RANGE_BYTES / 4 {
                        return Err(DecodeError::Custom("Invalid Four length"));
                    }
                    if offset + len as usize * 4 > input.len() {
                        return Err(DecodeError::IncorrectLength);
                    }
                    let vec = input[offset..offset + len as usize * 4]
                        .chunks_exact(4)
                        .map(|chunk| u32::from_be_bytes(chunk.try_into().unwrap()))
                        .collect();
                    offset += len as usize * 4;
                    Self::Four(vec)
                }
            },
            0b01 => match len_or_end {
                0 => {
                    // Two(64) (value_is_none = true)
                    if offset + 64 * 2 > input.len() {
                        return Err(DecodeError::IncorrectLength);
                    }
                    let vec = input[offset..offset + 128]
                        .chunks_exact(2)
                        .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
                        .collect();
                    offset += 128;
                    Self::Two(vec)
                }
                len => {
                    // Four (value_is_none = true)
                    if len as usize > ONE_RANGE_BYTES / 4 {
                        return Err(DecodeError::Custom("Invalid Four length"));
                    }
                    if offset + len as usize * 4 > input.len() {
                        return Err(DecodeError::IncorrectLength);
                    }
                    let vec = input[offset..offset + len as usize * 4]
                        .chunks_exact(4)
                        .map(|chunk| u32::from_be_bytes(chunk.try_into().unwrap()))
                        .collect();
                    offset += len as usize * 4;
                    Self::Four(vec)
                }
            },
            0b10 => {
                // Two (value_is_none = true)
                let len = len_or_end as usize;
                if offset + len * 2 > input.len() {
                    return Err(DecodeError::IncorrectLength);
                }
                let vec = input[offset..offset + len * 2]
                    .chunks_exact(2)
                    .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
                    .collect();
                offset += len * 2;
                Self::Two(vec)
            }
            0b11 => match len_or_end {
                0 => {
                    // Bitmap (value_is_none = true)
                    if offset + ONE_RANGE_BYTES > input.len() {
                        return Err(DecodeError::IncorrectLength);
                    }
                    let bits = input[offset..offset + ONE_RANGE_BYTES]
                        .try_into()
                        .map_err(|_| DecodeError::IncorrectLength)?;
                    offset += ONE_RANGE_BYTES;
                    Self::Bitmap(bits)
                }
                len => {
                    // Two (value_is_none = false)
                    value_is_none = false;
                    let len = len as usize;
                    if len == 0 || len > ONE_RANGE_BYTES / 2 {
                        return Err(DecodeError::Custom("Invalid Two length"));
                    }
                    if offset + len * 2 > input.len() {
                        return Err(DecodeError::IncorrectLength);
                    }
                    let vec = input[offset..offset + len * 2]
                        .chunks_exact(2)
                        .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
                        .collect();
                    offset += len * 2;
                    Self::Two(vec)
                }
            },
            _ => unreachable!(),
        };

        // Validation checks remain similar
        Ok((one_range, offset, value_is_none))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generate_one_ranges() -> Vec<OneRange> {
        let mut ranges = Vec::new();

        ranges.push(OneRange::OnlyEnd(0));
        ranges.push(OneRange::OnlyEnd(u64::MAX));

        let max_four_len = ONE_RANGE_BYTES / 4;
        for len in 1..=max_four_len {
            let data: Vec<u32> = (1..=len).map(|i| i as u32).collect();
            ranges.push(OneRange::Four(data));
        }

        let max_two_len = ONE_RANGE_BYTES / 2;
        for len in 0..=max_two_len {
            let data: Vec<u16> = (0..len).map(|i| i as u16).collect();
            ranges.push(OneRange::Two(data));
        }

        ranges.push(OneRange::Bitmap([0; ONE_RANGE_BYTES]));
        ranges.push(OneRange::Bitmap([0xFF; ONE_RANGE_BYTES]));
        let mut bitmap = [0; ONE_RANGE_BYTES];
        for i in 0..ONE_RANGE_BYTES {
            bitmap[i] = if i % 2 == 0 { 0xAA } else { 0x55 };
        }
        ranges.push(OneRange::Bitmap(bitmap));

        ranges
    }

    fn generate_test_values() -> Vec<Option<Box<[u8]>>> {
        vec![
            None,
            Some(vec![].into()),
            Some(vec![0].into()),
            Some(vec![0xFF, 0xFE].into()),
            Some((0..16).collect::<Vec<u8>>().into()),
            Some(vec![0; ONE_RANGE_BYTES].into()),
            Some((0..=255).collect::<Vec<u8>>().into()),
        ]
    }

    #[test]
    fn test_previous_roundtrip() {
        for range in generate_one_ranges().into_iter().filter(|r| match r {
            OneRange::Two(vec) => !vec.is_empty(),
            _ => true,
        }) {
            let original = HistoryIndices::Previous(range);
            let encoded = original.encode();
            let decoded = HistoryIndices::decode(&encoded).unwrap();
            assert_eq!(decoded.into_owned(), original);
        }
    }

    #[test]
    fn test_latest_roundtrip() {
        for range in generate_one_ranges() {
            for v in generate_test_values() {
                let original = HistoryIndices::Latest((u64::MAX, range.clone(), v.clone()));
                let encoded = original.encode();
                let decoded = HistoryIndices::decode(&encoded).unwrap();

                assert_eq!(decoded.into_owned(), original);
            }
        }
    }

    #[test]
    #[should_panic(expected = "Invalid Four length in Previous")]
    fn test_encode_panic_four_zero_length_in_previous() {
        let invalid_four = OneRange::Four(vec![]);
        HistoryIndices::<Box<[u8]>>::Previous(invalid_four).encode();
    }

    #[test]
    #[should_panic(expected = "Invalid Four length in Latest")]
    fn test_encode_panic_four_zero_length_in_latest() {
        let invalid_four = OneRange::Four(vec![]);
        HistoryIndices::<Box<[u8]>>::Latest((0 as u64, invalid_four, Some(vec![].into()))).encode();
    }

    #[test]
    #[should_panic(expected = "Invalid Four length in Latest")]
    fn test_encode_panic_four_zero_length_in_latest_none() {
        let invalid_four = OneRange::Four(vec![]);
        HistoryIndices::<Box<[u8]>>::Latest((0 as u64, invalid_four, None)).encode();
    }

    #[test]
    #[should_panic(expected = "Invalid Two length in Previous")]
    fn test_encode_panic_two_zero_length_in_previous() {
        let invalid_two = OneRange::Two(vec![]);
        HistoryIndices::<Box<[u8]>>::Previous(invalid_two).encode();
    }

    #[test]
    #[should_panic(expected = "Invalid Four length in Previous")]
    fn test_encode_panic_four_exceed_max_in_previous() {
        let vec = vec![0u32; ONE_RANGE_BYTES / 4 + 1];
        let invalid_four = OneRange::Four(vec);
        HistoryIndices::<Box<[u8]>>::Previous(invalid_four).encode();
    }

    #[test]
    #[should_panic(expected = "Invalid Four length in Latest")]
    fn test_encode_panic_four_exceed_max_in_latest() {
        let vec = vec![0u32; ONE_RANGE_BYTES / 4 + 1];
        let invalid_four = OneRange::Four(vec);
        HistoryIndices::<Box<[u8]>>::Latest((0 as u64, invalid_four, Some(vec![].into()))).encode();
    }

    #[test]
    #[should_panic(expected = "Invalid Four length in Latest")]
    fn test_encode_panic_four_exceed_max_in_latest_none() {
        let vec = vec![0u32; ONE_RANGE_BYTES / 4 + 1];
        let invalid_four = OneRange::Four(vec);
        HistoryIndices::<Box<[u8]>>::Latest((0 as u64, invalid_four, None)).encode();
    }

    #[test]
    #[should_panic(expected = "Invalid Two length in Previous")]
    fn test_encode_panic_two_exceed_max_in_previous() {
        let vec = vec![0u16; ONE_RANGE_BYTES / 2 + 1];
        let invalid_two = OneRange::Two(vec);
        HistoryIndices::<Box<[u8]>>::Previous(invalid_two).encode();
    }

    #[test]
    #[should_panic(expected = "Invalid Four length in Latest")]
    fn test_encode_panic_two_exceed_max_in_latest() {
        let vec = vec![0u16; ONE_RANGE_BYTES / 2 + 1];
        let invalid_two = OneRange::Two(vec);
        HistoryIndices::<Box<[u8]>>::Latest((0 as u64, invalid_two, Some(vec![].into()))).encode();
    }

    #[test]
    #[should_panic(expected = "Invalid Four length in Latest")]
    fn test_encode_panic_two_exceed_max_in_latest_none() {
        let vec = vec![0u16; ONE_RANGE_BYTES / 2 + 1];
        let invalid_two = OneRange::Two(vec);
        HistoryIndices::<Box<[u8]>>::Latest((0 as u64, invalid_two, None)).encode();
    }

    #[test]
    fn test_decode_error() {
        // Test incomplete input for OnlyEnd
        let data = vec![0b00 << 6]; // Missing 8-byte u64
        assert!(matches!(
            HistoryIndices::<Box<[u8]>>::decode(&data),
            Err(DecodeError::IncorrectLength)
        ));

        // Test invalid Four length (0 elements)
        let mut data = vec![0b01 << 6 | 0]; // Len=0 (special Two case)
        data.extend(vec![0u8; 64 * 2]); // Valid Two data
        if ONE_RANGE_BYTES == 128 {
            assert!(HistoryIndices::<Box<[u8]>>::decode(&data).is_ok()); // This should actually be valid
        } else {
            assert!(ONE_RANGE_BYTES == 64);
            assert!(matches!(
                HistoryIndices::<Box<[u8]>>::decode(&data),
                Err(DecodeError::Custom("Two vector length invalid"))
            ));
        }

        // Test invalid Four length in non-special case
        let mut data = vec![0b01 << 6 | 1]; // Normal Four case
        data.extend(vec![0u8; 3]); // Insufficient data (needs 4*1=4 bytes)
        assert!(matches!(
            OneRange::decode_impl(&data),
            Err(DecodeError::IncorrectLength)
        ));

        // Test invalid Bitmap length
        let data = vec![0b11 << 6]; // Tag only, no bitmap data
        assert!(matches!(
            HistoryIndices::<Box<[u8]>>::decode(&data),
            Err(DecodeError::IncorrectLength)
        ));

        // Test invalid Latest format (insufficient version/data), value_is_none
        let mut data = Vec::new();
        OneRange::OnlyEnd(123).encode_impl(&mut data, true);
        data.extend(&[0; 4]); // Insufficient u64 + value
        assert!(matches!(
            HistoryIndices::<Box<[u8]>>::decode(&data),
            Err(DecodeError::IncorrectLength)
        ));

        // Test invalid Latest format (insufficient version/data), !value_is_none
        let mut data = Vec::new();
        OneRange::OnlyEnd(123).encode_impl(&mut data, false);
        data.extend(&[0; 4]); // Insufficient u64 + value
        assert!(matches!(
            HistoryIndices::<Box<[u8]>>::decode(&data),
            Err(DecodeError::IncorrectLength)
        ));
    }
}
