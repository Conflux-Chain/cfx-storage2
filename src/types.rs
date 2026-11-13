use std::borrow::Cow;

use crate::{
    backends::serde::{Decode, Encode},
    errors::{DecResult, DecodeError},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueEntry<T> {
    Value(T),
    Deleted,
}

impl<T> From<ValueEntry<T>> for Option<T> {
    fn from(value: ValueEntry<T>) -> Self {
        value.into_option()
    }
}

impl<T> ValueEntry<T> {
    pub fn from_option(value: Option<T>) -> Self {
        match value {
            Some(v) => ValueEntry::Value(v),
            None => ValueEntry::Deleted,
        }
    }
    pub fn into_option(self) -> Option<T> {
        match self {
            ValueEntry::Value(v) => Some(v),
            ValueEntry::Deleted => None,
        }
    }

    pub fn as_opt_ref(&self) -> Option<&T> {
        match self {
            ValueEntry::Value(v) => Some(v),
            ValueEntry::Deleted => None,
        }
    }
}

impl<T: Clone> ValueEntry<T> {
    pub fn to_option(&self) -> Option<T> {
        match self {
            ValueEntry::Value(v) => Some(v.clone()),
            ValueEntry::Deleted => None,
        }
    }
}

impl<T: Encode + Clone> Encode for ValueEntry<T> {
    fn encode(&self) -> Cow<[u8]> {
        match self {
            ValueEntry::Deleted => Cow::Borrowed(&[0x00]),
            ValueEntry::Value(value) => {
                let encoded_val = value.encode();
                let mut vec = Vec::with_capacity(1 + encoded_val.len());
                vec.push(0x01); // 'Value' tag
                vec.extend_from_slice(encoded_val.as_ref());
                Cow::Owned(vec)
            }
        }
    }
}

impl<T: Decode + Clone + ToOwned<Owned = T>> Decode for ValueEntry<T> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.is_empty() {
            return Err(DecodeError::IncorrectLength);
        }

        let tag = input[0];
        let data = &input[1..];

        match tag {
            0x00 => {
                if !data.is_empty() {
                    return Err(DecodeError::IncorrectLength);
                }
                Ok(Cow::Owned(ValueEntry::Deleted))
            }
            0x01 => {
                let value = T::decode(data)?;
                Ok(Cow::Owned(ValueEntry::Value(value.into_owned())))
            }
            _ => Err(DecodeError::Custom("Invalid ValueEntry variant prefix")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    // Helper to build boxed byte slices quickly
    fn b(bs: &[u8]) -> Box<[u8]> {
        Box::<[u8]>::from(bs)
    }

    #[test]
    fn valueentry_from_option() {
        // Some -> Value
        let ve: ValueEntry<Box<[u8]>> = ValueEntry::from_option(Some(b(b"hello")));
        assert!(matches!(ve, ValueEntry::Value(_)));

        // None -> Deleted
        let ve: ValueEntry<Box<[u8]>> = ValueEntry::from_option(None);
        assert!(matches!(ve, ValueEntry::Deleted));
    }

    #[test]
    fn valueentry_into_option() {
        // Value -> Some
        let ve = ValueEntry::Value(b(b"abc"));
        let opt: Option<Box<[u8]>> = ve.into_option();
        assert_eq!(opt.as_deref(), Some(&b"abc"[..]));

        // Deleted -> None
        let ve: ValueEntry<Box<[u8]>> = ValueEntry::Deleted;
        let opt: Option<Box<[u8]>> = ve.into_option();
        assert!(opt.is_none());
    }

    #[test]
    fn valueentry_as_opt_ref() {
        let ve = ValueEntry::Value(b(b"\x00\x01"));
        let r = ve.as_opt_ref();
        assert!(r.is_some());
        assert_eq!(&**r.unwrap(), b"\x00\x01");

        let del: ValueEntry<Box<[u8]>> = ValueEntry::Deleted;
        assert!(del.as_opt_ref().is_none());
    }

    #[test]
    fn valueentry_to_option_clone() {
        // Ensure cloning works and content equals
        let ve = ValueEntry::Value(b(b"clone-me"));
        let opt = ve.to_option();
        assert_eq!(opt.as_deref(), Some(&b"clone-me"[..]));

        let del: ValueEntry<Box<[u8]>> = ValueEntry::Deleted;
        assert!(del.to_option().is_none());
    }

    #[test]
    fn encode_deleted() {
        let ve: ValueEntry<Box<[u8]>> = ValueEntry::Deleted;
        let enc = ve.encode();
        // Deleted encodes to a single 0x00 byte with no payload
        assert_eq!(enc.as_ref(), &[0x00]);
    }

    #[test]
    fn encode_value() {
        // Expected format: 0x01 || encode(T)
        // Since T = Box<[u8]>, and you said its Encode is already implemented,
        // we only check tag and that payload is appended.
        let payload = b(b"\xAA\xBB\xCC");
        let ve: ValueEntry<Box<[u8]>> = ValueEntry::Value(payload.clone());
        let enc = ve.encode();

        // Must start with 0x01
        assert!(!enc.is_empty());
        assert_eq!(enc[0], 0x01);

        // The rest should equal T::encode(payload)
        // Roundtrip via T's decode to verify correctness generically.
        let rest = &enc[1..];
        let decoded_t: Cow<Box<[u8]>> = <Box<[u8]> as Decode>::decode(rest).expect("decode T");
        assert_eq!(decoded_t.as_ref().as_ref(), payload.as_ref());
    }

    #[test]
    fn decode_deleted_exact() {
        let bytes = [0x00u8];
        let decoded: Cow<ValueEntry<Box<[u8]>>> =
            <ValueEntry<Box<[u8]>> as Decode>::decode(&bytes).expect("decode deleted");
        assert!(matches!(*decoded, ValueEntry::Deleted));
    }

    #[test]
    fn decode_deleted_with_trailing_data_is_error() {
        // Deleted must be exactly one byte (0x00), any trailing data is invalid
        let bytes = [0x00u8, 0xFF];
        let res = <ValueEntry<Box<[u8]>> as Decode>::decode(&bytes);
        assert!(matches!(res, Err(DecodeError::IncorrectLength)));
    }

    #[test]
    fn decode_value_roundtrip() {
        let payload = b(b"roundtrip");
        // Compose bytes: 0x01 || encode(T)
        let mut encoded = vec![0x01];
        let enc_t = <Box<[u8]> as Encode>::encode(&payload);
        encoded.extend_from_slice(enc_t.as_ref());

        let decoded: Cow<ValueEntry<Box<[u8]>>> =
            <ValueEntry<Box<[u8]>> as Decode>::decode(&encoded).expect("decode value");

        match decoded.as_ref() {
            ValueEntry::Value(bx) => assert_eq!(bx.as_ref(), payload.as_ref()),
            _ => panic!("expected Value variant"),
        }
    }

    #[test]
    fn decode_empty_is_error() {
        let bytes: [u8; 0] = [];
        let res = <ValueEntry<Box<[u8]>> as Decode>::decode(&bytes);
        assert!(matches!(res, Err(DecodeError::IncorrectLength)));
    }

    #[test]
    fn decode_invalid_tag_is_error() {
        let bytes = [0xFFu8];
        let res = <ValueEntry<Box<[u8]>> as Decode>::decode(&bytes);
        assert!(matches!(res, Err(DecodeError::Custom(_))));
    }

    #[test]
    fn from_impl_for_option() {
        // Verify From<ValueEntry<T>> for Option<T>
        let ve = ValueEntry::Value(b(b"xyz"));
        let opt: Option<Box<[u8]>> = ve.into();
        assert_eq!(opt.as_deref(), Some(&b"xyz"[..]));

        let ve: ValueEntry<Box<[u8]>> = ValueEntry::Deleted;
        let opt: Option<Box<[u8]>> = ve.into();
        assert!(opt.is_none());
    }

    #[test]
    fn encode_value_capacity_prefix() {
        // Ensure encode allocates exactly 1 + len(T::encode()) when Owned
        let payload = b(b"\x01\x02\x03\x04\x05");
        let ve: ValueEntry<Box<[u8]>> = ValueEntry::Value(payload.clone());
        let enc_t = <Box<[u8]> as Encode>::encode(&payload);
        let enc = ve.encode();
        assert_eq!(enc.len(), 1 + enc_t.len());
        assert_eq!(enc[0], 0x01);
        assert_eq!(&enc[1..], enc_t.as_ref());
    }
}
