use std::sync::Arc;

use crate::{
    backends::{
        DatabaseTrait, HistoricalTableName, PendingTableName, VersionedKVName, WrappedInMemoryDb,
    },
    errors::Result,
    middlewares::{
        primitives_gc_until_height, primitives_initialize_empty_schema,
        primitives_verify_schema_is_empty, table_schema::VersionedKeyValueSchema, CommitID,
        PendingKeyValueConfig, VersionedStore, VersionedStoreCache,
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
        >(&write_schema, None, 0);

        Ok(Self {
            historical_db: WrappedInMemoryDb::empty().into(),
            pending_db,
            cache: Mutex::new(VersionedStoreCache::from_initialized_state(
                tree_with_tracker,
            ))
            .into(),
        })
    }

    pub fn as_manager(&self) -> Result<FlatStore<'_>> {
        let key_value_store = VersionedStore::new(self.historical_db.clone(), self.cache.clone())?;
        Ok(FlatStore {
            pending_persistence_backend: self.pending_db.clone(),
            key_value_store,
        })
    }

    // TODO: The durable_height should be obtained from self.historical_part, but this is just a demo.
    fn get_durable_height(&self) -> u64 {
        let height_of_pending_root = self.cache.lock().get_height_of_root();
        let safety_height_diff = 5;
        if height_of_pending_root > safety_height_diff {
            height_of_pending_root - safety_height_diff
        } else {
            0
        }
    }

    /// Performs background garbage collection on the pending persistence layer.
    ///
    /// This method acts as the "upper-level application".
    /// It orchestrates the cleanup by:
    /// 1. Fetching the `durable_height` from the historical database.
    /// 2. Calling the `gc_until_height` primitive from the `persistence` module.
    /// 3. Committing the changes to the pending database to ensure atomicity.
    pub fn background_cleanup(&self) -> Result<()> {
        // Step 1: Get durable_height.
        let durable_height = self.get_durable_height();

        if durable_height == 0 {
            return Ok(());
        }

        // Step 2: Prepare the write batch/schema for the pending DB.
        let write_schema = WrappedInMemoryDb::<PendingTableName>::write_schema();

        // Step 3: Call the low-level primitive to populate the write schema.
        // Here we only have one table (`FlatKeyValue`), but if you had more,
        // you would call `gc_until_height` for each of them here, using the same write_schema.
        primitives_gc_until_height::<PendingKeyValueConfig<FlatKeyValue, CommitID>, _>(
            &self.pending_db,
            &write_schema,
            durable_height,
        )?;

        // Step 4: Commit the changes atomically. The primitive itself doesn't commit.
        self.pending_db.commit(write_schema)?;

        Ok(())
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
