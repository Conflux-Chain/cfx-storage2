use std::hash::Hash;

use crate::{
    backends::{TableKey, TableName, TableSchema, TableValue, VersionedKVName},
    traits::KeyValueStoreRead,
};

use super::{HistoryChangeKey, HistoryIndexKey, HistoryIndices};

pub trait VersionedKeyValueSchema: 'static + Copy + Send + Sync
where
    HistoryChangeKey<Self::Key>: TableKey,
    HistoryIndexKey<Self::Key>: TableKey,
{
    const NAME: VersionedKVName;
    type Key: TableKey + ToOwned<Owned = Self::Key> + Clone + Hash;
    type Value: TableValue + Clone;
}

#[derive(Clone, Copy)]
pub struct HistoryChangeTable<T: VersionedKeyValueSchema>(T);

impl<T: VersionedKeyValueSchema> TableSchema for HistoryChangeTable<T> {
    const NAME: TableName = TableName::HistoryChange(T::NAME);
    type Key = HistoryChangeKey<T::Key>;
    type Value = T::Value;
}

#[derive(Clone, Copy)]
pub struct HistoryIndicesTable<T: VersionedKeyValueSchema>(T);

impl<T: VersionedKeyValueSchema> TableSchema for HistoryIndicesTable<T> {
    const NAME: TableName = TableName::HistoryIndex(T::NAME);
    type Key = HistoryIndexKey<T::Key>;
    type Value = HistoryIndices;
}

pub type KeyValueSnapshotRead<'a, T> = dyn 'a
    + KeyValueStoreRead<<T as VersionedKeyValueSchema>::Key, <T as VersionedKeyValueSchema>::Value>;
