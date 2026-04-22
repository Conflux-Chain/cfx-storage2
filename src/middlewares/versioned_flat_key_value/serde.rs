use std::borrow::Cow;

use super::HistoryIndexKey;
use crate::backends::serde::{Decode, Encode, EncodeSubKey, FixedLengthEncoded};
use crate::errors::{DecResult, DecodeError};
use crate::middlewares::HistoryNumber;

// Encode the history number reversed: this ensures that, under the table's
// seek-geq iterator, the first record for `(key, v_query)` is the record with
// the largest `v' <= v_query` for that key. The 4-byte key length prefix
// preserves correct ordering when keys can have varying lengths.
fn encode_history_number_rev(n: HistoryNumber) -> [u8; 8] {
    (u64::MAX - n).to_be_bytes()
}

fn decode_history_number_rev(raw: &[u8]) -> DecResult<HistoryNumber> {
    if raw.len() != 8 {
        return Err(DecodeError::IncorrectLength);
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(raw);
    Ok(u64::MAX - u64::from_be_bytes(arr))
}

impl<K: Clone + Encode> Encode for HistoryIndexKey<K> {
    fn encode(&self) -> Cow<[u8]> {
        let encoded_key = self.0.encode();
        let key_len = (encoded_key.len() as u32).to_be_bytes();
        let encoded_version = encode_history_number_rev(self.1);

        let mut out = Vec::with_capacity(4 + encoded_key.len() + encoded_version.len());
        out.extend_from_slice(&key_len);
        out.extend_from_slice(encoded_key.as_ref());
        out.extend_from_slice(&encoded_version);

        Cow::Owned(out)
    }
}

impl<K: Clone + FixedLengthEncoded> FixedLengthEncoded for HistoryIndexKey<K> {
    const LENGTH: usize = 4 + K::LENGTH + std::mem::size_of::<HistoryNumber>();
}

impl<K: Clone + Encode + ToOwned<Owned = K>> EncodeSubKey for HistoryIndexKey<K> {
    const HAVE_SUBKEY: bool = true;

    fn encode_subkey(&self) -> (Cow<[u8]>, Cow<[u8]>) {
        let encoded_key = self.0.encode();
        let key_len = (encoded_key.len() as u32).to_be_bytes();

        let mut prefix = Vec::with_capacity(4 + encoded_key.len());
        prefix.extend_from_slice(&key_len);
        prefix.extend_from_slice(encoded_key.as_ref());

        let encoded_version = encode_history_number_rev(self.1);
        (Cow::Owned(prefix), Cow::Owned(encoded_version.to_vec()))
    }

    fn encode_subkey_owned(input: <Self as ToOwned>::Owned) -> (Vec<u8>, Vec<u8>) {
        let encoded_key = K::encode_owned(input.0);
        let key_len = (encoded_key.len() as u32).to_be_bytes();

        let mut prefix = Vec::with_capacity(4 + encoded_key.len());
        prefix.extend_from_slice(&key_len);
        prefix.extend_from_slice(&encoded_key);

        (prefix, encode_history_number_rev(input.1).to_vec())
    }
}

impl<K: Clone + Decode + ToOwned<Owned = K>> Decode for HistoryIndexKey<K> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        const LEN_SIZE: usize = 4;
        const VER_SIZE: usize = std::mem::size_of::<HistoryNumber>();

        if input.len() < LEN_SIZE + VER_SIZE {
            return Err(DecodeError::IncorrectLength);
        }

        let len_bytes: [u8; 4] = input[0..4].try_into().unwrap();
        let key_len = u32::from_be_bytes(len_bytes) as usize;

        if input.len() != LEN_SIZE + key_len + VER_SIZE {
            return Err(DecodeError::IncorrectLength);
        }

        let key_raw = &input[LEN_SIZE..LEN_SIZE + key_len];
        let version_raw = &input[LEN_SIZE + key_len..];

        let key = K::decode(key_raw)?;
        let version = decode_history_number_rev(version_raw)?;
        Ok(Cow::Owned(HistoryIndexKey(key.into_owned(), version)))
    }

    fn decode_owned(mut input: Vec<u8>) -> DecResult<Self> {
        const LEN_SIZE: usize = 4;
        const VER_SIZE: usize = std::mem::size_of::<HistoryNumber>();

        if input.len() < LEN_SIZE + VER_SIZE {
            return Err(DecodeError::IncorrectLength);
        }

        let len_bytes: [u8; 4] = input[0..4].try_into().unwrap();
        let key_len = u32::from_be_bytes(len_bytes) as usize;

        if input.len() != LEN_SIZE + key_len + VER_SIZE {
            return Err(DecodeError::IncorrectLength);
        }

        let version_raw = input.split_off(input.len() - VER_SIZE);
        let key_raw = input.split_off(LEN_SIZE);

        let key = K::decode_owned(key_raw)?;
        let version = decode_history_number_rev(&version_raw)?;
        Ok(HistoryIndexKey(key, version))
    }
}
