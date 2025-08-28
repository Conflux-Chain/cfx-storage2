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
pub fn decode_option<'a, T: Clone + Decode>(input: &'a [u8]) -> DecResult<Option<Cow<'a, T>>> {
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
