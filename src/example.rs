use std::sync::Arc;

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

pub struct FlatStore<'db> {
    pending_persistence_backend: Arc<WrappedInMemoryDb<PendingTableName>>,
    key_value_store: VersionedStore<'db, FlatKeyValue, WrappedInMemoryDb<PendingTableName>>,
}

pub struct Storage {
    historical_db: Arc<WrappedInMemoryDb<HistoricalTableName>>,
    pending_db: Arc<WrappedInMemoryDb<PendingTableName>>,
    cache: Arc<Mutex<VersionedStoreCache<FlatKeyValue, WrappedInMemoryDb<PendingTableName>>>>,
}

impl Storage {
    pub fn new_empty() -> Result<Self> {
        let pending_db = Arc::new(WrappedInMemoryDb::empty());
        primitives_verify_schema_is_empty::<
            PendingKeyValueConfig<FlatKeyValue, CommitID>,
            WrappedInMemoryDb<PendingTableName>,
        >(&pending_db)
        .unwrap();
        let write_schema = WrappedInMemoryDb::write_schema();
        let tree_with_tracker = primitives_initialize_empty_schema::<
            PendingKeyValueConfig<FlatKeyValue, CommitID>,
        >(&write_schema, None, 0)
        .unwrap();

        Ok(Self {
            historical_db: WrappedInMemoryDb::empty().into(),
            pending_db,
            cache: Mutex::new(VersionedStoreCache::from_initialized_state(
                tree_with_tracker,
            ))
            .into(),
        })
    }

    pub fn as_manager(&mut self) -> Result<FlatStore<'_>> {
        let key_value_store = VersionedStore::new(self.historical_db.clone(), self.cache.clone())?;
        Ok(FlatStore {
            pending_persistence_backend: self.pending_db.clone(),
            key_value_store,
        })
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
