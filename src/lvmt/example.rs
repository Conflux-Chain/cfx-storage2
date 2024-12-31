use std::sync::Arc;

use crate::{
    backends::DatabaseTrait,
    errors::Result,
    middlewares::{
        confirmed_pending_to_history, CommitID, KeyValueStoreBulks, VersionedStore,
        VersionedStoreCache,
    },
};

use super::{
    auth_changes::AuthChangeTable,
    storage::LvmtStore,
    table_schema::{AmtNodes, FlatKeyValue, SlotAllocations},
};

pub struct LvmtStorage<D: DatabaseTrait> {
    backend: D,
    key_value_cache: VersionedStoreCache<FlatKeyValue>,
    amt_node_cache: VersionedStoreCache<AmtNodes>,
    slot_alloc_cache: VersionedStoreCache<SlotAllocations>,
}

impl<D: DatabaseTrait> LvmtStorage<D> {
    pub fn new(backend: D) -> Result<Self> {
        Ok(Self {
            backend,
            key_value_cache: VersionedStoreCache::new_empty(),
            amt_node_cache: VersionedStoreCache::new_empty(),
            slot_alloc_cache: VersionedStoreCache::new_empty(),
        })
    }

    pub fn as_manager(&mut self) -> Result<LvmtStore<'_, '_>> {
        let key_value_store = VersionedStore::new(&self.backend, &mut self.key_value_cache)?;
        let amt_node_store = VersionedStore::new(&self.backend, &mut self.amt_node_cache)?;
        let slot_alloc_store = VersionedStore::new(&self.backend, &mut self.slot_alloc_cache)?;
        let auth_changes =
            KeyValueStoreBulks::new(Arc::new(self.backend.view::<AuthChangeTable>()?));

        Ok(LvmtStore::new(
            key_value_store,
            amt_node_store,
            slot_alloc_store,
            auth_changes,
        ))
    }

    pub fn commit_auth(&mut self, write_schema: <D as DatabaseTrait>::WriteSchema) -> Result<()> {
        self.backend.commit(write_schema)
    }

    pub fn confirmed_pending_to_history(&mut self, new_root_commit_id: CommitID) -> Result<()> {
        confirmed_pending_to_history(
            &mut self.backend,
            &mut self.key_value_cache,
            new_root_commit_id,
            true,
        )?;

        confirmed_pending_to_history(
            &mut self.backend,
            &mut self.amt_node_cache,
            new_root_commit_id,
            false,
        )?;

        confirmed_pending_to_history(
            &mut self.backend,
            &mut self.slot_alloc_cache,
            new_root_commit_id,
            false,
        )?;

        Ok(())
    }
}
