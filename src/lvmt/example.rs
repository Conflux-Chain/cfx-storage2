use std::sync::Arc;

use parking_lot::Mutex;

use crate::{
    backends::DatabaseTrait,
    errors::Result,
    middlewares::{
        confirm_ids_to_history, confirm_maps_to_history, CommitID, KeyValueStoreBulks,
        VersionedStore, VersionedStoreCache,
    },
};

use super::{
    auth_changes::AuthChangeTable,
    storage::LvmtStore,
    table_schema::{AmtNodes, FlatKeyValue, SlotAllocations},
};

pub struct LvmtStorage<D: DatabaseTrait> {
    backend: Arc<Mutex<D>>,
    key_value_cache: Arc<Mutex<VersionedStoreCache<FlatKeyValue>>>,
    amt_node_cache: Arc<Mutex<VersionedStoreCache<AmtNodes>>>,
    slot_alloc_cache: Arc<Mutex<VersionedStoreCache<SlotAllocations>>>,
}

impl<D: DatabaseTrait> LvmtStorage<D> {
    pub fn new(backend: Arc<Mutex<D>>) -> Result<Self> {
        Ok(Self {
            backend: backend.clone(),
            key_value_cache: Mutex::new(VersionedStoreCache::new_empty()).into(),
            amt_node_cache: Mutex::new(VersionedStoreCache::new_empty()).into(),
            slot_alloc_cache: Mutex::new(VersionedStoreCache::new_empty()).into(),
        })
    }

    pub fn as_manager(&mut self) -> Result<LvmtStore<'_>> {
        let key_value_store =
            VersionedStore::new(self.backend.clone(), self.key_value_cache.clone())?;
        let amt_node_store =
            VersionedStore::new(self.backend.clone(), self.amt_node_cache.clone())?;
        let slot_alloc_store =
            VersionedStore::new(self.backend.clone(), self.slot_alloc_cache.clone())?;
        let auth_changes =
            KeyValueStoreBulks::new(Arc::new(D::view::<AuthChangeTable>(&self.backend)?));

        Ok(LvmtStore::new(
            key_value_store,
            amt_node_store,
            slot_alloc_store,
            auth_changes,
        ))
    }

    pub fn commit(&mut self, write_schema: <D as DatabaseTrait>::WriteSchema) -> Result<()> {
        D::commit(self.backend, write_schema)
    }

    pub fn confirmed_pending_to_history(
        &mut self,
        new_root_commit_id: CommitID,
        write_schema: &D::WriteSchema,
    ) -> Result<()> {
        let mut key_value_cache = self.key_value_cache.lock();
        let mut amt_node_cache = self.amt_node_cache.lock();
        let mut slot_alloc_cache = self.slot_alloc_cache.lock();

        let key_value_confirmed_path = key_value_cache.change_root(new_root_commit_id)?;
        let amt_node_confirmed_path = amt_node_cache.change_root(new_root_commit_id)?;
        let slot_alloc_confirmed_path = slot_alloc_cache.change_root(new_root_commit_id)?;

        assert!(key_value_confirmed_path.is_same_path(&amt_node_confirmed_path));
        assert!(key_value_confirmed_path.is_same_path(&slot_alloc_confirmed_path));

        let start_height = key_value_confirmed_path.start_height;
        let commit_ids = &key_value_confirmed_path.commit_ids;

        confirm_ids_to_history::<D>(self.backend.clone(), start_height, commit_ids, write_schema)?;

        confirm_maps_to_history::<D, FlatKeyValue>(
            self.backend.clone(),
            start_height,
            key_value_confirmed_path.key_value_maps,
            write_schema,
        )?;
        confirm_maps_to_history::<D, AmtNodes>(
            self.backend.clone(),
            start_height,
            amt_node_confirmed_path.key_value_maps,
            write_schema,
        )?;
        confirm_maps_to_history::<D, SlotAllocations>(
            self.backend.clone(),
            start_height,
            slot_alloc_confirmed_path.key_value_maps,
            write_schema,
        )?;

        Ok(())
    }
}
