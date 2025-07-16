use std::sync::Arc;

use crate::{
    backends::DatabaseTrait,
    errors::Result,
    middlewares::{
        confirm_ids_to_history, confirm_maps_to_history, CommitID, HistoryNumber,
        HistoryNumberSchema, KeyValueStoreBulks, VersionedStore, VersionedStoreCache,
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
    pub fn iter_view(&self) {
        let view = self.backend.view::<HistoryNumberSchema>().unwrap();
        for i in view.iter_from_start().unwrap() {
            let (history_number, commit) = i.unwrap();
        }
        drop(view);
    }

    pub fn get_lastest_commit(&self, history_number: HistoryNumber) -> CommitID {
        let view = self.backend.view::<HistoryNumberSchema>().unwrap();
        let next = view.get(&(history_number + 1)).unwrap();
        assert!(next.is_none());
        view.get(&history_number).unwrap().unwrap().into_owned()
    }

    pub fn new(backend: D) -> Result<Self> {
        Ok(Self {
            backend,
            key_value_cache: VersionedStoreCache::new_empty(),
            amt_node_cache: VersionedStoreCache::new_empty(),
            slot_alloc_cache: VersionedStoreCache::new_empty(),
        })
    }

    pub fn new_nonempty(
        backend: D,
        parent_of_root: Option<CommitID>,
        height_of_root: usize,
    ) -> Result<Self> {
        Ok(Self {
            backend,
            key_value_cache: VersionedStoreCache::new(parent_of_root, height_of_root),
            amt_node_cache: VersionedStoreCache::new(parent_of_root, height_of_root),
            slot_alloc_cache: VersionedStoreCache::new(parent_of_root, height_of_root),
        })
    }

    pub fn as_manager(&mut self) -> Result<LvmtStore<'_, '_>> {
        let key_value_store = VersionedStore::new(&self.backend, &mut self.key_value_cache)?;
        let amt_node_store = VersionedStore::new(&self.backend, &mut self.amt_node_cache)?;
        let slot_alloc_store = VersionedStore::new(&self.backend, &mut self.slot_alloc_cache)?;
        let auth_changes =
            KeyValueStoreBulks::new(Arc::from(self.backend.view::<AuthChangeTable>()?));

        Ok(LvmtStore::new(
            key_value_store,
            amt_node_store,
            slot_alloc_store,
            auth_changes,
        ))
    }

    pub fn commit(&mut self, write_schema: <D as DatabaseTrait>::WriteSchema) -> Result<()> {
        self.backend.commit(write_schema)
    }

    pub fn confirmed_pending_to_history(
        &mut self,
        new_root_commit_id: CommitID,
        write_schema: &D::WriteSchema,
    ) -> Result<()> {
        let key_value_confirmed_path = self.key_value_cache.change_root(new_root_commit_id)?;
        let amt_node_confirmed_path = self.amt_node_cache.change_root(new_root_commit_id)?;
        let slot_alloc_confirmed_path = self.slot_alloc_cache.change_root(new_root_commit_id)?;

        assert!(key_value_confirmed_path.is_same_path(&amt_node_confirmed_path));
        assert!(key_value_confirmed_path.is_same_path(&slot_alloc_confirmed_path));

        let start_height = key_value_confirmed_path.start_height;
        let commit_ids = &key_value_confirmed_path.commit_ids;

        confirm_ids_to_history::<D>(&self.backend, start_height, commit_ids, write_schema)?;

        confirm_maps_to_history::<D, FlatKeyValue>(
            &self.backend,
            start_height,
            key_value_confirmed_path.key_value_maps,
            write_schema,
        )?;
        confirm_maps_to_history::<D, AmtNodes>(
            &self.backend,
            start_height,
            amt_node_confirmed_path.key_value_maps,
            write_schema,
        )?;
        confirm_maps_to_history::<D, SlotAllocations>(
            &self.backend,
            start_height,
            slot_alloc_confirmed_path.key_value_maps,
            write_schema,
        )?;

        Ok(())
    }

    pub fn remove_pending_to_history(
        &mut self,
        new_root_commit_id: CommitID,
        write_schema: &D::WriteSchema,
    ) -> Result<()> {
        let key_value_confirmed_path = self.key_value_cache.remove_root(new_root_commit_id)?;
        let amt_node_confirmed_path = self.amt_node_cache.remove_root(new_root_commit_id)?;
        let slot_alloc_confirmed_path = self.slot_alloc_cache.remove_root(new_root_commit_id)?;

        assert!(key_value_confirmed_path.is_same_path(&amt_node_confirmed_path));
        assert!(key_value_confirmed_path.is_same_path(&slot_alloc_confirmed_path));

        let start_height = key_value_confirmed_path.start_height;
        let commit_ids = &key_value_confirmed_path.commit_ids;

        confirm_ids_to_history::<D>(&self.backend, start_height, commit_ids, write_schema)?;

        confirm_maps_to_history::<D, FlatKeyValue>(
            &self.backend,
            start_height,
            key_value_confirmed_path.key_value_maps,
            write_schema,
        )?;
        confirm_maps_to_history::<D, AmtNodes>(
            &self.backend,
            start_height,
            amt_node_confirmed_path.key_value_maps,
            write_schema,
        )?;
        confirm_maps_to_history::<D, SlotAllocations>(
            &self.backend,
            start_height,
            slot_alloc_confirmed_path.key_value_maps,
            write_schema,
        )?;

        Ok(())
    }
}
