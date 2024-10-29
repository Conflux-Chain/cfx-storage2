use crate::{backends::WriteSchemaTrait, errors::Result};

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
    fn iter<'a>(&'a self, key: &K) -> Result<impl 'a + Iterator<Item = (&K, &V)>>;
}

pub trait KeyValueStoreCommit<K, V, C>: KeyValueStoreRead<K, V>
where
    K: 'static,
    V: 'static,
    C: 'static,
{
    fn commit(self, commit: C, changes: impl Iterator<Item = (K, V)>);
}

pub trait KeyValueStoreManager<K, V, C>
where
    K: 'static,
    V: 'static,
    C: 'static,
{
    type Store: KeyValueStoreRead<K, V> + KeyValueStoreCommit<K, V, C>;

    /// Get the key value store after the commit of given id
    fn get_versioned_store(&self, commit: &C) -> Result<Self::Store>;

    /// Start from the given commit, and iter changes backforward
    #[allow(clippy::type_complexity)]
    fn iter_historical_changes<'a>(
        &'a self,
        commit_id: &C,
        key: &'a K,
    ) -> Result<Box<dyn 'a + Iterator<Item = (C, &K, Option<V>)>>>;

    fn discard(&mut self, commit: C) -> Result<()>;

    fn get_versioned_key(&self, commit: &C, key: &K) -> Result<Option<V>>;
}

pub trait KeyValueStoreBulksTrait<K, V, C> {
    /// Commit a bundle of key-values, with provided commit version
    fn commit(
        &self,
        commit: C,
        bulk: impl Iterator<Item = (K, Option<V>)>,
        write_schema: &impl WriteSchemaTrait,
    ) -> Result<()>;

    /// Get with the given commit version and key.
    fn get_versioned_key(&self, commit: &C, key: &K) -> Result<Option<V>>;

    /// Commit changes for garbage collection only
    fn gc_commit(
        &self,
        changes: impl Iterator<Item = (C, K, Option<V>)>,
        write_schema: &impl WriteSchemaTrait,
    ) -> Result<()>;
}
