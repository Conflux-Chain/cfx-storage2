use std::{fmt::Debug, hash::Hash};

use crate::{
    backends::{
        serde::{Decode, Encode},
        HistoricalTableName, TableKey, TableSchema, TableValue, VersionedKVName,
    },
    traits::{KeyValueStoreIterable, KeyValueStoreRead},
};

use super::{history_indices::HistoryIndices, HistoryChangeKey, HistoryIndexKey};

pub trait VersionedKeyValueSchema: 'static + Copy + Send + Sync + Debug
where
    HistoryChangeKey<Self::Key>: TableKey,
    HistoryIndexKey<Self::Key>: TableKey,
{
    const NAME: VersionedKVName;
    type Key: TableKey + ToOwned<Owned = Self::Key> + Clone + Hash;
    type Value: TableValue + ToOwned<Owned = Self::Value> + Clone + Eq + Encode + Decode;
}

#[derive(Clone, Copy)]
pub struct HistoryChangeTable<T: VersionedKeyValueSchema>(T);

impl<T: VersionedKeyValueSchema> TableSchema for HistoryChangeTable<T> {
    type TableName = HistoricalTableName;
    const NAME: HistoricalTableName = HistoricalTableName::HistoryChange(T::NAME);
    type Key = HistoryChangeKey<T::Key>;
    type Value = T::Value;
}

#[derive(Clone, Copy)]
pub struct HistoryIndicesTable<T: VersionedKeyValueSchema>(T);

impl<T: VersionedKeyValueSchema> TableSchema for HistoryIndicesTable<T> {
    type TableName = HistoricalTableName;
    const NAME: HistoricalTableName = HistoricalTableName::HistoryIndex(T::NAME);
    type Key = HistoryIndexKey<T::Key>;
    type Value = HistoryIndices<T::Value>;
}

pub type KeyValueSnapshotRead<'a, T> = dyn 'a
    + KeyValueStoreRead<<T as VersionedKeyValueSchema>::Key, <T as VersionedKeyValueSchema>::Value>;

pub type KeyValueSnapshotIterable<'a, T> = dyn 'a
    + KeyValueStoreIterable<
        <T as VersionedKeyValueSchema>::Key,
        <T as VersionedKeyValueSchema>::Value,
    >;
