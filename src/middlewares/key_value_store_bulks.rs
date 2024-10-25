use std::borrow::Cow;

use crate::{
    backends::{
        serde::{Decode, Encode, EncodeSubKey, FixedLengthEncoded},
        TableReader, TableSchema, WriteSchemaTrait,
    },
    errors::{DecResult, Result},
    traits::KeyValueStoreBulksTrait,
};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct ChangeKey<C: Copy, K: Clone>(C, K);

pub struct KeyValueStoreBulks<'db, T: TableSchema>(TableReader<'db, T>);

impl<'db, T: TableSchema> KeyValueStoreBulks<'db, T> {
    pub(crate) fn new(db: TableReader<'db, T>) -> Self {
        Self(db)
    }
}

impl<'a, K, V, C, T> KeyValueStoreBulksTrait<K, V, C> for KeyValueStoreBulks<'a, T>
where
    T: TableSchema<Key = ChangeKey<C, K>, Value = V>,
    C: Copy,
    K: Clone,
    V: Clone,
{
    fn commit(
        &self,
        commit: C,
        bulk: impl Iterator<Item = (K, Option<V>)>,
        write_schema: &impl WriteSchemaTrait,
    ) -> Result<()> {
        let table_op =
            bulk.map(|(k, v)| (Cow::Owned(ChangeKey(commit, k)), v.map(|x| Cow::Owned(x))));
        write_schema.write_batch::<T>(table_op);
        Ok(())
    }

    fn get_versioned_key(&self, commit: &C, key: &K) -> Result<Option<V>> {
        let loaded = self.0.get(&ChangeKey(*commit, key.clone()))?;
        Ok(loaded.map(|x| x.into_owned()))
    }

    fn gc_commit(
        &self,
        changes: impl Iterator<Item = (C, K, Option<V>)>,
        write_schema: &impl WriteSchemaTrait,
    ) -> Result<()> {
        let table_op = changes
            .map(|(commit, k, v)| (Cow::Owned(ChangeKey(commit, k)), v.map(|x| Cow::Owned(x))));
        write_schema.write_batch::<T>(table_op);
        Ok(())
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<C, K> Encode for ChangeKey<C, K>
where
    C: Copy + FixedLengthEncoded,
    K: Clone + Encode,
{
    fn encode(&self) -> Cow<[u8]> {
        let encoded_commit = self.0.encode();
        let encoded_key = self.1.encode();

        Cow::Owned([encoded_commit.as_ref(), encoded_key.as_ref()].concat())
    }
}

impl<C, K> EncodeSubKey for ChangeKey<C, K>
where
    C: Copy + FixedLengthEncoded + ToOwned<Owned = C>,
    K: Clone + Encode + ToOwned<Owned = K>,
{
    const HAVE_SUBKEY: bool = true;
    fn encode_subkey(&self) -> (Cow<[u8]>, Cow<[u8]>) {
        (self.0.encode(), self.1.encode())
    }

    fn encode_subkey_owned(input: <Self as ToOwned>::Owned) -> (Vec<u8>, Vec<u8>) {
        (C::encode_owned(input.0), K::encode_owned(input.1))
    }
}

impl<C, K> Decode for ChangeKey<C, K>
where
    C: Copy + FixedLengthEncoded + Decode + ToOwned<Owned = C>,
    K: Clone + Decode + ToOwned<Owned = K>,
{
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        let (raw_commit, raw_key) = input.split_at(C::LENGTH);
        let (commit, key) = (C::decode(raw_commit)?, K::decode(raw_key)?);
        Ok(Cow::Owned(ChangeKey(commit.into_owned(), key.into_owned())))
    }
}
