use std::borrow::Cow;

use super::{HistoryIndexKey, HistoryIndices};
use crate::backends::serde::{Decode, Encode, EncodeSubKey, FixedLengthEncoded};
use crate::errors::{DecResult, DecodeError};
use crate::middlewares::{decode_history_number_rev, encode_history_number_rev, HistoryNumber};

impl<K: Clone + Encode> Encode for HistoryIndexKey<K> {
    fn encode(&self) -> Cow<[u8]> {
        let mut ans = self.0.encode().into_owned();
        ans.extend_from_slice(&encode_history_number_rev(self.1));
        Cow::Owned(ans)
    }
}

impl<K: Clone + FixedLengthEncoded> FixedLengthEncoded for HistoryIndexKey<K> {
    const LENGTH: usize = K::LENGTH + std::mem::size_of::<HistoryNumber>();
}

impl<K: Clone + Encode + ToOwned<Owned = K>> EncodeSubKey for HistoryIndexKey<K> {
    const HAVE_SUBKEY: bool = true;

    fn encode_subkey(&self) -> (Cow<[u8]>, Cow<[u8]>) {
        (
            self.0.encode(),
            Cow::Owned(encode_history_number_rev(self.1.to_owned()).to_vec()),
        )
    }

    fn encode_subkey_owned(input: <Self as ToOwned>::Owned) -> (Vec<u8>, Vec<u8>) {
        (
            K::encode_owned(input.0),
            encode_history_number_rev(input.1).to_vec(),
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
        let version = decode_history_number_rev(version_raw);
        let key = K::decode(key_raw)?;
        Ok(Cow::Owned(HistoryIndexKey(key.into_owned(), version)))
    }

    fn decode_owned(mut input: Vec<u8>) -> DecResult<Self> {
        const BYTES: usize = std::mem::size_of::<HistoryNumber>();
        if input.len() < BYTES {
            return Err(DecodeError::IncorrectLength);
        }

        let version_raw = input.split_off(input.len() - BYTES);
        let key_raw = input;
        let version = decode_history_number_rev(&version_raw);
        let key = K::decode_owned(key_raw)?;
        Ok(HistoryIndexKey(key, version))
    }
}

const EMPTY: &[u8] = &[];
impl Encode for HistoryIndices {
    fn encode(&self) -> Cow<[u8]> {
        Cow::Borrowed(EMPTY)
    }
}

impl Decode for HistoryIndices {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if !input.is_empty() {
            return Err(DecodeError::IncorrectLength);
        }
        Ok(Cow::Owned(HistoryIndices))
    }
}
