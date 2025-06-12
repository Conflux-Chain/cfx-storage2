use super::version_range::Bitmap;
use std::borrow::Cow;

use super::{
    HistoryIndices, OffsetBasedVersionRange, U16_VECTOR_CAPACITY, U32_VECTOR_CAPACITY,
    VERSION_RANGE_BYTES,
};
use crate::backends::serde::{Decode, Encode};
use crate::errors::{DecResult, DecodeError};

impl<V: Clone + Encode> Encode for HistoryIndices<V> {
    fn encode(&self) -> Cow<[u8]> {
        let mut buffer = Vec::new();
        match self {
            Self::Latest {
                start_version_number,
                range_encoding,
                latest_value,
            } => {
                range_encoding.encode_impl(&mut buffer, latest_value.is_none());
                buffer.extend(start_version_number.to_be_bytes());
                if let Some(value) = latest_value {
                    buffer.extend(value.encode().into_owned());
                }
            }
            Self::Previous(range) => {
                if let OffsetBasedVersionRange::U16Vector(vec) = range {
                    if vec.is_empty() {
                        panic!("U16Vector vector should not be empty in Previous");
                    }
                }
                range.encode_impl(&mut buffer, true);
            }
        }
        Cow::Owned(buffer)
    }
}

impl<V: Clone + Decode + ToOwned<Owned = V>> Decode for HistoryIndices<V> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        let (range_encoding, consumed, value_is_none) =
            OffsetBasedVersionRange::decode_impl(input)?;
        if input.len() == consumed {
            if let OffsetBasedVersionRange::U16Vector(ref vec) = range_encoding {
                if vec.is_empty() {
                    return Err(DecodeError::Custom(
                        "U16Vector vector should not be empty in Previous",
                    ));
                }
            }
            if !value_is_none {
                return Err(DecodeError::Custom(
                    "For Previous, value_is_none should always be true",
                ));
            }
            Ok(Cow::Owned(Self::Previous(range_encoding)))
        } else {
            let version_start = consumed;
            let version_end = version_start + 8;
            if version_end > input.len() {
                return Err(DecodeError::IncorrectLength);
            }
            let start_version_number =
                u64::from_be_bytes(input[version_start..version_end].try_into().unwrap());
            if value_is_none {
                if version_end < input.len() {
                    return Err(DecodeError::IncorrectLength);
                }
                Ok(Cow::Owned(Self::Latest {
                    start_version_number,
                    range_encoding,
                    latest_value: None,
                }))
            } else {
                let v_raw = input[version_end..].to_vec();
                let v = V::decode(&v_raw)?;
                Ok(Cow::Owned(Self::Latest {
                    start_version_number,
                    range_encoding,
                    latest_value: Some(v.into_owned()),
                }))
            }
        }
    }
}

/*
Encoding tag:

if value_is_none == true:
00 000000 | OnlyEnd
01 000000 | U16Vector(64)
01 000001-01 100000 | U32Vector(1-32)
10 000000-10 111111 | U16Vector(0-63)
11 000000 | Bitmap

if value_is_none == false:
00 000001-00 100000 | U32Vector(1-32)
00 111100 | Bitmap
00 111101 | U16Vector(0)
00 111110 | U16Vector(64)
00 111111 | OnlyEnd
11 000001-11 111111 | U16Vector(1-63)

Safety preconditions: VERSION_RANGE_BYTES <= 128
*/

impl OffsetBasedVersionRange {
    fn encode_impl(&self, buffer: &mut Vec<u8>, value_is_none: bool) {
        match (self, value_is_none) {
            // OnlyEnd cases
            (Self::OnlyEnd(end), true) => {
                buffer.push(0b00 << 6);
                buffer.extend(end.to_be_bytes());
            }
            (Self::OnlyEnd(end), false) => {
                buffer.push(0b111111);
                buffer.extend(end.to_be_bytes());
            }

            // U32Vector cases
            (Self::U32Vector(vec), true) => {
                let len = vec.len();
                if len == 0 || len > U32_VECTOR_CAPACITY {
                    panic!("Invalid U32Vector length");
                }
                buffer.push(0b01 << 6 | len as u8);
                for version in vec {
                    buffer.extend(version.to_be_bytes());
                }
            }
            (Self::U32Vector(vec), false) => {
                let len = vec.len();
                if len == 0 || len > U32_VECTOR_CAPACITY {
                    panic!("Invalid U32Vector length");
                }
                buffer.push(len as u8);
                for version in vec {
                    buffer.extend(version.to_be_bytes());
                }
            }

            // U16Vector cases
            (Self::U16Vector(vec), true) => {
                let len = vec.len();
                if len > U16_VECTOR_CAPACITY {
                    panic!("Invalid U16Vector length");
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
            (Self::U16Vector(vec), false) => {
                let len = vec.len();
                if len > U16_VECTOR_CAPACITY {
                    panic!("Invalid U16Vector length");
                }
                if len == 0 {
                    buffer.push(0b111101);
                } else if len == 64 {
                    buffer.push(0b111110);
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
                buffer.extend_from_slice(bits.as_slice());
            }
            (Self::Bitmap(bits), false) => {
                buffer.push(0b111100);
                buffer.extend_from_slice(bits.as_slice());
            }
        }
    }

    fn decode_impl(input: &[u8]) -> DecResult<(Self, usize, bool)> {
        if input.is_empty() {
            return Err(DecodeError::IncorrectLength);
        }

        let tag = input[0];
        let tag_prefix = tag >> 6;
        let tag_suffix = tag & 0x3F;
        let start_offset = 1;

        let (version_range, offset_inc, value_is_none) = match tag_prefix {
            0b00 => match tag_suffix {
                0b000000 => {
                    // OnlyEnd (value_is_none = true)
                    let required = 8;
                    if start_offset + required > input.len() {
                        return Err(DecodeError::IncorrectLength);
                    }
                    let end = u64::from_be_bytes(
                        input[start_offset..start_offset + required]
                            .try_into()
                            .unwrap(),
                    );
                    (Self::OnlyEnd(end), required, true)
                }
                0b111100 => {
                    // Bitmap (value_is_none = false)
                    let required = VERSION_RANGE_BYTES;
                    if start_offset + required > input.len() {
                        return Err(DecodeError::IncorrectLength);
                    }
                    let bits = input[start_offset..start_offset + required]
                        .try_into()
                        .map_err(|_| DecodeError::IncorrectLength)?;
                    (Self::Bitmap(Bitmap::new(bits)), required, false)
                }
                0b111101 => {
                    // U16Vector(0) (value_is_none = false)
                    (Self::U16Vector(Vec::new()), 0, false)
                }
                0b111110 => {
                    // U16Vector(64) (value_is_none = false)
                    let (range, inc) = decode_vector(input, start_offset, 64, true)?;
                    (range, inc, false)
                }
                0b111111 => {
                    // OnlyEnd (value_is_none = false)
                    let required = 8;
                    if start_offset + required > input.len() {
                        return Err(DecodeError::IncorrectLength);
                    }
                    let end = u64::from_be_bytes(
                        input[start_offset..start_offset + required]
                            .try_into()
                            .unwrap(),
                    );
                    (Self::OnlyEnd(end), required, false)
                }
                len => {
                    // U32Vector (value_is_none = false)
                    let (range, inc) = decode_vector(input, start_offset, len as usize, false)?;
                    (range, inc, false)
                }
            },
            0b01 => match tag_suffix {
                0 => {
                    // U16Vector(64) (value_is_none = true)
                    let (range, inc) = decode_vector(input, start_offset, 64, true)?;
                    (range, inc, true)
                }
                len => {
                    // U32Vector (value_is_none = true)
                    let (range, inc) = decode_vector(input, start_offset, len as usize, false)?;
                    (range, inc, true)
                }
            },
            0b10 => {
                // U16Vector (value_is_none = true)
                let (range, inc) = decode_vector(input, start_offset, tag_suffix as usize, true)?;
                (range, inc, true)
            }
            0b11 => match tag_suffix {
                0 => {
                    // Bitmap (value_is_none = true)
                    let required = VERSION_RANGE_BYTES;
                    if start_offset + required > input.len() {
                        return Err(DecodeError::IncorrectLength);
                    }
                    let bits = input[start_offset..start_offset + required]
                        .try_into()
                        .map_err(|_| DecodeError::IncorrectLength)?;
                    (Self::Bitmap(Bitmap::new(bits)), required, true)
                }
                len => {
                    // U16Vector (value_is_none = false)
                    let (range, inc) = decode_vector(input, start_offset, len as usize, true)?;
                    (range, inc, false)
                }
            },
            _ => unreachable!(),
        };

        // Validation checks remain similar
        Ok((version_range, start_offset + offset_inc, value_is_none))
    }
}

fn decode_vector(
    input: &[u8],
    start_offset: usize,
    len: usize,
    is_u16: bool,
) -> DecResult<(OffsetBasedVersionRange, usize)> {
    let (bytes_per_element, error_msg, vector_capacity) = if is_u16 {
        (2, "Invalid U16Vector length", U16_VECTOR_CAPACITY)
    } else {
        (4, "Invalid U32Vector length", U32_VECTOR_CAPACITY)
    };

    if !is_u16 && len == 0 {
        return Err(DecodeError::Custom(error_msg));
    }
    if len > vector_capacity {
        return Err(DecodeError::Custom(error_msg));
    }
    let total_bytes = len * bytes_per_element;
    if start_offset + total_bytes > input.len() {
        return Err(DecodeError::IncorrectLength);
    }

    let range = if is_u16 {
        let vec = input[start_offset..start_offset + total_bytes]
            .chunks_exact(2)
            .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
            .collect();
        OffsetBasedVersionRange::U16Vector(vec)
    } else {
        let vec = input[start_offset..start_offset + total_bytes]
            .chunks_exact(4)
            .map(|chunk| u32::from_be_bytes(chunk.try_into().unwrap()))
            .collect();
        OffsetBasedVersionRange::U32Vector(vec)
    };

    Ok((range, total_bytes))
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use crate::middlewares::versioned_flat_key_value::history_indices::test_utils::history_indices_strategy;

    use super::*;

    use proptest::prelude::*;

    fn generate_version_ranges() -> Vec<OffsetBasedVersionRange> {
        let mut ranges = Vec::new();

        ranges.push(OffsetBasedVersionRange::OnlyEnd(0));
        ranges.push(OffsetBasedVersionRange::OnlyEnd(u64::MAX));

        for len in 1..=U32_VECTOR_CAPACITY {
            let data: Vec<u32> = (1..=len).map(|i| i as u32).collect();
            ranges.push(OffsetBasedVersionRange::U32Vector(data));
        }

        for len in 0..=U16_VECTOR_CAPACITY {
            let data: Vec<u16> = (0..len).map(|i| i as u16).collect();
            ranges.push(OffsetBasedVersionRange::U16Vector(data));
        }

        ranges.push(OffsetBasedVersionRange::Bitmap(Bitmap::new(
            [0; VERSION_RANGE_BYTES],
        )));
        ranges.push(OffsetBasedVersionRange::Bitmap(Bitmap::new(
            [0xFF; VERSION_RANGE_BYTES],
        )));
        let mut bitmap = [0; VERSION_RANGE_BYTES];
        for (i, byte) in bitmap.iter_mut().enumerate().take(VERSION_RANGE_BYTES) {
            *byte = if i % 2 == 0 { 0xAA } else { 0x55 };
        }
        ranges.push(OffsetBasedVersionRange::Bitmap(Bitmap::new(bitmap)));

        ranges
    }

    fn generate_test_values() -> Vec<Option<Box<[u8]>>> {
        vec![
            None,
            Some(vec![].into()),
            Some(vec![0].into()),
            Some(vec![0xFF, 0xFE].into()),
            Some((0..16).collect::<Vec<u8>>().into()),
            Some(vec![0; VERSION_RANGE_BYTES].into()),
            Some((0..=255).collect::<Vec<u8>>().into()),
        ]
    }

    fn test_roundtrip_method<
        V: Clone + Encode + Decode + ToOwned<Owned = V> + PartialEq + Debug,
    >(
        original: HistoryIndices<V>,
    ) {
        let encoded = original.encode();
        let decoded = HistoryIndices::decode(&encoded).unwrap();

        assert_eq!(decoded.into_owned(), original);
    }

    #[test]
    fn test_previous_roundtrip() {
        for range in generate_version_ranges().into_iter().filter(|r| match r {
            OffsetBasedVersionRange::U16Vector(vec) => !vec.is_empty(),
            _ => true,
        }) {
            let original = HistoryIndices::<Box<[u8]>>::Previous(range);
            test_roundtrip_method::<Box<[u8]>>(original);
        }
    }

    #[test]
    fn test_latest_roundtrip() {
        for range in generate_version_ranges() {
            for v in generate_test_values() {
                let original = HistoryIndices::<Box<[u8]>>::Latest {
                    start_version_number: u64::MAX,
                    range_encoding: range.clone(),
                    latest_value: v,
                };
                test_roundtrip_method::<Box<[u8]>>(original);
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        #[test]
        fn test_serde(original in history_indices_strategy()) {
            let encoded = original.encode();
            let decoded = HistoryIndices::<Box<[u8]>>::decode(&encoded).unwrap();

            assert_eq!(decoded.into_owned(), original);
        }
    }

    #[test]
    #[should_panic(expected = "Invalid U32Vector length")]
    fn test_encode_panic_empty_u32vec_in_previous() {
        let u32_empty = OffsetBasedVersionRange::U32Vector(vec![]);
        HistoryIndices::<Box<[u8]>>::Previous(u32_empty).encode();
    }

    #[test]
    #[should_panic(expected = "Invalid U32Vector length")]
    fn test_encode_panic_empty_u32vec_in_latest() {
        let u32_empty = OffsetBasedVersionRange::U32Vector(vec![]);
        HistoryIndices::<Box<[u8]>>::Latest {
            start_version_number: 0_u64,
            range_encoding: u32_empty,
            latest_value: Some(vec![].into()),
        }
        .encode();
    }

    #[test]
    #[should_panic(expected = "Invalid U32Vector length")]
    fn test_encode_panic_empty_u32vec_in_latest_none() {
        let u32_empty = OffsetBasedVersionRange::U32Vector(vec![]);
        HistoryIndices::<Box<[u8]>>::Latest {
            start_version_number: 0_u64,
            range_encoding: u32_empty,
            latest_value: None,
        }
        .encode();
    }

    #[test]
    #[should_panic(expected = "U16Vector vector should not be empty in Previous")]
    fn test_encode_panic_empty_u16vec_in_previous() {
        let u16_empty = OffsetBasedVersionRange::U16Vector(vec![]);
        HistoryIndices::<Box<[u8]>>::Previous(u16_empty).encode();
    }

    #[test]
    #[should_panic(expected = "Invalid U32Vector length")]
    fn test_encode_panic_u32vec_exceed_capacity_in_previous() {
        let vec = vec![0u32; U32_VECTOR_CAPACITY + 1];
        let u32_vec = OffsetBasedVersionRange::U32Vector(vec);
        HistoryIndices::<Box<[u8]>>::Previous(u32_vec).encode();
    }

    #[test]
    #[should_panic(expected = "Invalid U32Vector length")]
    fn test_encode_panic_u32vec_exceed_capacity_in_latest() {
        let vec = vec![0u32; U32_VECTOR_CAPACITY + 1];
        let u32_vec = OffsetBasedVersionRange::U32Vector(vec);
        HistoryIndices::<Box<[u8]>>::Latest {
            start_version_number: 0_u64,
            range_encoding: u32_vec,
            latest_value: Some(vec![].into()),
        }
        .encode();
    }

    #[test]
    #[should_panic(expected = "Invalid U32Vector length")]
    fn test_encode_panic_u32vec_exceed_capacity_in_latest_none() {
        let vec = vec![0u32; U32_VECTOR_CAPACITY + 1];
        let u32_vec = OffsetBasedVersionRange::U32Vector(vec);
        HistoryIndices::<Box<[u8]>>::Latest {
            start_version_number: 0_u64,
            range_encoding: u32_vec,
            latest_value: None,
        }
        .encode();
    }

    #[test]
    #[should_panic(expected = "Invalid U16Vector length")]
    fn test_encode_panic_u16vec_exceed_capacity_in_previous() {
        let vec = vec![0u16; U16_VECTOR_CAPACITY + 1];
        let u16_vec = OffsetBasedVersionRange::U16Vector(vec);
        HistoryIndices::<Box<[u8]>>::Previous(u16_vec).encode();
    }

    #[test]
    #[should_panic(expected = "Invalid U16Vector length")]
    fn test_encode_panic_u16vec_exceed_capacity_in_latest() {
        let vec = vec![0u16; U16_VECTOR_CAPACITY + 1];
        let u16_vec = OffsetBasedVersionRange::U16Vector(vec);
        HistoryIndices::<Box<[u8]>>::Latest {
            start_version_number: 0_u64,
            range_encoding: u16_vec,
            latest_value: Some(vec![].into()),
        }
        .encode();
    }

    #[test]
    #[should_panic(expected = "Invalid U16Vector length")]
    fn test_encode_panic_u16vec_exceed_capacity_in_latest_none() {
        let vec = vec![0u16; U16_VECTOR_CAPACITY + 1];
        let u16_vec = OffsetBasedVersionRange::U16Vector(vec);
        HistoryIndices::<Box<[u8]>>::Latest {
            start_version_number: 0_u64,
            range_encoding: u16_vec,
            latest_value: None,
        }
        .encode();
    }

    #[test]
    fn test_decode_error() {
        // Test incomplete input for OnlyEnd
        let data = vec![0b00 << 6]; // Missing 8-byte u64
        assert!(matches!(
            HistoryIndices::<Box<[u8]>>::decode(&data),
            Err(DecodeError::IncorrectLength)
        ));

        // Test invalid U16Vector length (64 elements)
        let mut data = vec![0b01 << 6];
        data.extend(vec![0u8; 64 * 2]); // Valid U16Vector data
        if VERSION_RANGE_BYTES == 128 {
            assert!(HistoryIndices::<Box<[u8]>>::decode(&data).is_ok()); // This should actually be valid
        } else {
            assert!(matches!(
                HistoryIndices::<Box<[u8]>>::decode(&data),
                Err(DecodeError::Custom("Invalid U16Vector length"))
            ));
        }

        // Test invalid U32Vector length in non-special case
        let mut data = vec![0b01 << 6 | 1];
        data.extend(vec![0u8; 3]); // Insufficient data (needs 4*1=4 bytes)
        assert!(matches!(
            OffsetBasedVersionRange::decode_impl(&data),
            Err(DecodeError::IncorrectLength)
        ));

        // Test invalid Bitmap: tag only, no bitmap data
        let data = vec![0b11 << 6];
        assert!(matches!(
            HistoryIndices::<Box<[u8]>>::decode(&data),
            Err(DecodeError::IncorrectLength)
        ));

        // Test invalid Latest format (insufficient version/data), value_is_none
        let mut data = Vec::new();
        OffsetBasedVersionRange::OnlyEnd(123).encode_impl(&mut data, true);
        data.extend(&[0; 4]); // Insufficient u64 + value
        assert!(matches!(
            HistoryIndices::<Box<[u8]>>::decode(&data),
            Err(DecodeError::IncorrectLength)
        ));

        // Test invalid Latest format (insufficient version/data), !value_is_none
        let mut data = Vec::new();
        OffsetBasedVersionRange::OnlyEnd(123).encode_impl(&mut data, false);
        data.extend(&[0; 4]); // Insufficient u64 + value
        assert!(matches!(
            HistoryIndices::<Box<[u8]>>::decode(&data),
            Err(DecodeError::IncorrectLength)
        ));
    }
}
