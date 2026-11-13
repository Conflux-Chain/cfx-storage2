use crate::{
    backends::{DatabaseTrait, PendingTableName, TableNameTrait, WriteSchemaTrait},
    errors::Result,
};

pub trait KeyValueStoreRead<K, V>
where
    K: 'static,
    V: 'static,
{
    fn get(&self, key: &K) -> Result<Option<V>>;
}

pub trait KeyValueStoreIterable<K, V>: KeyValueStoreRead<K, V>
where
    K: 'static,
    V: 'static,
{
    fn iter(&self) -> Result<impl Iterator<Item = (K, V)>>;
}

pub type NeedNext = bool;
pub type IsCompleted = bool;

pub trait KeyValueStoreManager<K, V, C, P>
where
    K: 'static,
    V: 'static,
    C: 'static,
    P: DatabaseTrait<PendingTableName>,
{
    type Store<'a>: 'a + KeyValueStoreRead<K, V>
    where
        Self: 'a;

    /// Get the key value store after the commit of given id
    fn get_versioned_store<'s>(
        &'s self,
        commit: &C,
        checkout_current: bool,
    ) -> Result<Self::Store<'s>>;

    /// Start from the given commit, and iter changes backforward
    #[allow(clippy::type_complexity)]
    fn iter_historical_changes(
        &self,
        accept: impl FnMut(&C, &K, Option<&V>) -> NeedNext,
        commit_id: &C,
        key: &K,
    ) -> Result<IsCompleted>;

    /// make commit the unique child of its parent
    /// do nothing if commit is in history or if commit is pending root
    fn discard(&mut self, commit: C, pending_write_schema: &P::WriteSchema) -> Result<()>;

    fn get_versioned_key(&self, commit: &C, key: &K) -> Result<Option<V>>;
}

pub trait KeyValueStoreBulksTrait<K, V, C, TN: TableNameTrait> {
    /// Commit a bundle of key-values, with provided commit version
    fn commit(
        &self,
        commit: C,
        bulk: impl Iterator<Item = (K, Option<V>)>,
        write_schema: &impl WriteSchemaTrait<TN>,
    ) -> Result<()>;

    /// Get with the given commit version and key.
    fn get_versioned_key(&self, commit: &C, key: &K) -> Result<Option<V>>;

    /// Commit changes for garbage collection only
    fn gc_commit(
        &self,
        changes: impl Iterator<Item = (C, K, Option<V>)>,
        write_schema: &impl WriteSchemaTrait<TN>,
    ) -> Result<()>;
}
