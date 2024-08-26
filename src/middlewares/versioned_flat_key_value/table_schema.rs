use std::borrow::Cow;

use crate::backends::serde::{Decode, Encode, EncodeSubKey, FixedLengthEncoded};
use crate::backends::{TableKey, TableName, TableSchema, TableValue, VersionedKVName};
use crate::errors::{DecResult, DecodeError};
use crate::middlewares::{decode_history_number_rev, encode_history_number_rev, ChangeKey, HistoryNumber};

use super::{HistoryIndices, KeyHistory};

pub trait VersionedKeyValueSchema: 'static + Copy + Send + Sync
where
    ChangeKey<HistoryNumber, Self::Key>: TableKey,
    KeyHistory<Self::Key>: TableKey,
{
    const NAME: VersionedKVName;
    type Key: TableKey + ToOwned<Owned = Self::Key> + Clone;
    type Value: TableValue + Clone;
}

#[derive(Clone, Copy)]
pub struct ChangeHistorySchema<T: VersionedKeyValueSchema>(T);

impl<T: VersionedKeyValueSchema> TableSchema for ChangeHistorySchema<T> {
    const NAME: TableName = TableName::ChangeHistory(<T as VersionedKeyValueSchema>::NAME);
    type Key = ChangeKey<HistoryNumber, <T as VersionedKeyValueSchema>::Key>;
    type Value = <T as VersionedKeyValueSchema>::Value;
}

#[derive(Clone, Copy)]
pub struct HistoryIndicesSchema<T: VersionedKeyValueSchema>(T);

impl<T: VersionedKeyValueSchema> TableSchema for HistoryIndicesSchema<T> {
    const NAME: TableName = TableName::HistoryIndex(<T as VersionedKeyValueSchema>::NAME);
    type Key = KeyHistory<<T as VersionedKeyValueSchema>::Key>;
    type Value = HistoryIndices;
}

impl<K: Clone + Encode> Encode for KeyHistory<K> {
    fn encode(&self) -> Cow<[u8]> {
        let mut ans = self.0.encode().into_owned();
        ans.extend_from_slice(&encode_history_number_rev(self.1));
        Cow::Owned(ans)
    }
}

impl<K: Clone + FixedLengthEncoded> FixedLengthEncoded for KeyHistory<K> {
    const LENGTH: usize = K::LENGTH + std::mem::size_of::<HistoryNumber>();
}

impl<K: Clone + Encode + ToOwned<Owned = K>> EncodeSubKey for KeyHistory<K> {
    const HAVE_SUBKEY: bool = true;

    fn encode_subkey(&self) -> (Cow<[u8]>, Cow<[u8]>) {
        (self.0.encode(),Cow::Owned(encode_history_number_rev(self.1.to_owned()).to_vec()))
    }

    fn encode_subkey_owned(input: <Self as ToOwned>::Owned) -> (Vec<u8>, Vec<u8>) {
        (K::encode_owned(input.0), encode_history_number_rev(input.1).to_vec())
    }
}

impl<K: Clone + Decode + ToOwned<Owned = K>> Decode for KeyHistory<K> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        const BYTES: usize = std::mem::size_of::<HistoryNumber>() as usize;
        if input.len() < BYTES {
            return Err(DecodeError::IncorrectLength);
        }

        let (key_raw, version_raw) = input.split_at(input.len() - BYTES);
        let version = decode_history_number_rev(version_raw);
        let key = K::decode(key_raw)?;
        Ok(Cow::Owned(KeyHistory(key.into_owned(), version)))
    }

    fn decode_owned(mut input: Vec<u8>) -> DecResult<Self> {
        const BYTES: usize = std::mem::size_of::<HistoryNumber>() as usize;
        if input.len() < BYTES {
            return Err(DecodeError::IncorrectLength);
        }

        let version_raw = input.split_off(input.len() - BYTES);
        let key_raw = input;
        let version = decode_history_number_rev(&version_raw);
        let key = K::decode_owned(key_raw)?;
        Ok(KeyHistory(key, version))
    }
}

impl Encode for HistoryIndices {
    fn encode(&self) -> Cow<[u8]> {
        todo!()
    }
}

impl Decode for HistoryIndices {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        todo!()
    }
}
