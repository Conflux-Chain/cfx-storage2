use std::borrow::Cow;

use super::super::{DecResult, Decode, DecodeError, Encode};

/// - None -> [0x00]
/// - Some(value) -> [0x01, ...encoded value...]
pub fn encode_option<T: Encode>(opt: &Option<T>) -> Cow<[u8]> {
    match opt {
        None => Cow::Borrowed(&[0x00]),
        Some(ref val) => {
            let encoded_val = val.encode();
            let mut vec = Vec::with_capacity(1 + encoded_val.len());
            vec.push(0x01);
            vec.extend_from_slice(encoded_val.as_ref());
            Cow::Owned(vec)
        }
    }
}

/// - `[0x00]` -> `Ok(None)`
/// - `[0x01, ...encoded T...]` -> `Ok(Some(T))`
/// - otherwise -> `Err`
pub fn decode_option<T: Clone + Decode>(input: &[u8]) -> DecResult<Option<Cow<T>>> {
    if input.is_empty() {
        return Err(DecodeError::IncorrectLength);
    }

    match input[0] {
        0x00 => {
            if input.len() > 1 {
                Err(DecodeError::IncorrectLength)
            } else {
                Ok(None)
            }
        }
        0x01 => {
            let decoded_val = T::decode(&input[1..])?;
            Ok(Some(decoded_val))
        }
        _ => Err(DecodeError::Custom("Invalid option prefix")),
    }
}

pub fn decode_option_owned<T: Clone + Decode + ToOwned<Owned = T>>(
    input: Vec<u8>,
) -> DecResult<Option<T>> {
    let result_with_cow = decode_option::<T>(&input)?;

    let result_owned = result_with_cow.map(|cow| cow.into_owned());

    Ok(result_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    // A simple type implementing Encode + Decode for testing.
    // Encoding: big-endian u8 (single byte)
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct OneByte(u8);

    impl Encode for OneByte {
        fn encode(&self) -> Cow<[u8]> {
            Cow::Owned(vec![self.0])
        }
    }

    impl Decode for OneByte {
        fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
            if input.len() != 1 {
                return Err(DecodeError::IncorrectLength);
            }
            Ok(Cow::Owned(OneByte(input[0])))
        }
    }

    #[test]
    fn encode_option_none() {
        let opt: Option<OneByte> = None;
        let encoded = encode_option(&opt);
        assert_eq!(encoded.as_ref(), &[0x00]);
    }

    #[test]
    fn encode_option_some() {
        let opt = Some(OneByte(0xAB));
        let encoded = encode_option(&opt);
        assert_eq!(encoded.as_ref(), &[0x01, 0xAB]);
    }

    #[test]
    fn decode_option_none_ok() {
        let decoded = decode_option::<OneByte>(&[0x00]).unwrap();
        assert!(decoded.is_none());
    }

    #[test]
    fn decode_option_none_error_extra_bytes() {
        let err = decode_option::<OneByte>(&[0x00, 0xFF]).unwrap_err();
        match err {
            DecodeError::IncorrectLength => {}
            e => panic!("unexpected error: {:?}", e),
        }
    }

    #[test]
    fn decode_option_empty_input_error() {
        let err = decode_option::<OneByte>(&[]).unwrap_err();
        match err {
            DecodeError::IncorrectLength => {}
            e => panic!("unexpected error: {:?}", e),
        }
    }

    #[test]
    fn decode_option_invalid_prefix_error() {
        let err = decode_option::<OneByte>(&[0x02]).unwrap_err();
        match err {
            DecodeError::Custom(msg) => assert_eq!(msg, "Invalid option prefix"),
            e => panic!("unexpected error: {:?}", e),
        }
    }

    #[test]
    fn decode_option_some_ok() {
        let decoded = decode_option::<OneByte>(&[0x01, 0x7F]).unwrap();
        let inner = decoded.expect("should be Some");
        assert_eq!(inner.as_ref(), &OneByte(0x7F));
    }

    #[test]
    fn decode_option_some_incorrect_inner_length() {
        // For OneByte, inner length must be exactly 1; here it's 0.
        let err = decode_option::<OneByte>(&[0x01]).unwrap_err();
        match err {
            DecodeError::IncorrectLength => {}
            e => panic!("unexpected error: {:?}", e),
        }
    }

    #[test]
    fn decode_option_owned_none_ok() {
        let owned = decode_option_owned::<OneByte>(vec![0x00]).unwrap();
        assert!(owned.is_none());
    }

    #[test]
    fn decode_option_owned_some_ok() {
        let owned = decode_option_owned::<OneByte>(vec![0x01, 0xCC]).unwrap();
        assert_eq!(owned, Some(OneByte(0xCC)));
    }

    #[test]
    fn decode_option_owned_invalid_prefix_error() {
        let err = decode_option_owned::<OneByte>(vec![0xFF]).unwrap_err();
        match err {
            DecodeError::Custom(msg) => assert_eq!(msg, "Invalid option prefix"),
            e => panic!("unexpected error: {:?}", e),
        }
    }

    #[test]
    fn roundtrip_none() {
        let opt: Option<OneByte> = None;
        let enc = encode_option(&opt);
        let dec = decode_option_owned::<OneByte>(enc.into_owned()).unwrap();
        assert_eq!(dec, opt);
    }

    #[test]
    fn roundtrip_some() {
        let opt = Some(OneByte(0x42));
        let enc = encode_option(&opt);
        let dec = decode_option_owned::<OneByte>(enc.into_owned()).unwrap();
        assert_eq!(dec, opt);
    }
}
