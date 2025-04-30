use std::borrow::Cow;

use super::HistoryIndexKey;
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
