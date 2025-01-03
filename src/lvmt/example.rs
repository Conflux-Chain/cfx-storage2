use std::sync::Arc;

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

    pub fn confirmed_pending_to_history(
        &mut self,
        new_root_commit_id: CommitID,
        write_schema: &D::WriteSchema,
    ) -> Result<()> {
        // old root..=new root's parent
        let (
            key_value_to_confirm_start_height,
            key_value_to_confirm_ids,
            key_value_to_confirm_maps,
        ) = self.key_value_cache.change_root(new_root_commit_id)?;

        let (amt_node_to_confirm_start_height, amt_node_to_confirm_ids, amt_node_to_confirm_maps) =
            self.amt_node_cache.change_root(new_root_commit_id)?;

        let (
            slot_alloc_to_confirm_start_height,
            slot_alloc_to_confirm_ids,
            slot_alloc_to_confirm_maps,
        ) = self.slot_alloc_cache.change_root(new_root_commit_id)?;

        assert_eq!(
            key_value_to_confirm_start_height,
            amt_node_to_confirm_start_height
        );
        assert_eq!(
            key_value_to_confirm_start_height,
            slot_alloc_to_confirm_start_height
        );

        assert_eq!(key_value_to_confirm_ids, amt_node_to_confirm_ids);
        assert_eq!(key_value_to_confirm_ids, slot_alloc_to_confirm_ids);

        let to_confirm_start_height = key_value_to_confirm_start_height;
        let to_confirm_ids = key_value_to_confirm_ids;

        confirm_ids_to_history::<D>(
            &self.backend,
            to_confirm_start_height,
            &to_confirm_ids,
            write_schema,
        )?;

        confirm_maps_to_history::<D, FlatKeyValue>(
            &self.backend,
            to_confirm_start_height,
            key_value_to_confirm_maps,
            write_schema,
        )?;
        confirm_maps_to_history::<D, AmtNodes>(
            &self.backend,
            to_confirm_start_height,
            amt_node_to_confirm_maps,
            write_schema,
        )?;
        confirm_maps_to_history::<D, SlotAllocations>(
            &self.backend,
            to_confirm_start_height,
            slot_alloc_to_confirm_maps,
            write_schema,
        )?;

        Ok(())
    }
}
