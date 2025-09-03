use std::{path::Path, sync::Arc};

use crate::{
    backends::{
        DatabaseTrait, HistoricalTableName, PendingTableName, VersionedKVName, WrappedInMemoryDb,
    },
    errors::Result,
    middlewares::{
        primitives_initialize_empty_schema, primitives_verify_schema_is_empty,
        table_schema::VersionedKeyValueSchema, CommitID, PendingKeyValueConfig, VersionedStore,
        VersionedStoreCache,
    },
    traits::KeyValueStoreManager,
};
use ethereum_types::H256;
use parking_lot::Mutex;
use static_assertions::assert_impl_all;

pub struct Storage {
    db: Arc<WrappedInMemoryDb<HistoricalTableName>>,
    cache: Arc<Mutex<VersionedStoreCache<FlatKeyValue, WrappedInMemoryDb<PendingTableName>>>>,
}

impl Storage {
    pub fn new_empty(log_path: impl AsRef<Path>) -> Result<Self> {
        let cache_persistence = Arc::new(WrappedInMemoryDb::empty());
        primitives_verify_schema_is_empty::<
            PendingKeyValueConfig<FlatKeyValue, CommitID>,
            WrappedInMemoryDb<PendingTableName>,
        >(&cache_persistence)
        .unwrap();
        let write_schema = WrappedInMemoryDb::write_schema();
        let tree_with_tracker = primitives_initialize_empty_schema::<
            PendingKeyValueConfig<FlatKeyValue, CommitID>,
        >(&write_schema, None, 0)
        .unwrap();

        Ok(Self {
            db: WrappedInMemoryDb::empty().into(),
            cache: Mutex::new(VersionedStoreCache::from_initialized_state(
                cache_persistence,
                tree_with_tracker,
            ))
            .into(),
        })
    }

    pub fn as_manager(
        &mut self,
    ) -> Result<VersionedStore<'_, FlatKeyValue, WrappedInMemoryDb<PendingTableName>>> {
        VersionedStore::new(self.db.clone(), self.cache.clone())
    }
}

assert_impl_all!(VersionedStore<'_, FlatKeyValue, WrappedInMemoryDb<PendingTableName>>: KeyValueStoreManager<Box<[u8]>, Box<[u8]>, H256, WrappedInMemoryDb<PendingTableName>>);

#[derive(Clone, Copy, Debug)]
pub struct FlatKeyValue;

impl VersionedKeyValueSchema for FlatKeyValue {
    const NAME: VersionedKVName = VersionedKVName::FlatKV;

    type Key = Box<[u8]>;
    type Value = Box<[u8]>;
}
