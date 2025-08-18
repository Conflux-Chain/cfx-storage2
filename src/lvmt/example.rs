use std::{path::Path, sync::Arc};

use parking_lot::Mutex;

use crate::{
    backends::{DatabaseTrait, TableRead},
    errors::Result,
    middlewares::{
        confirm_ids_to_history, confirm_maps_to_history, history_number_to_height, CommitID,
        CommitIDSchema, HistoryNumberSchema, KeyValueStoreBulks, VersionedStore,
        VersionedStoreCache,
    },
};

use super::{
    auth_changes::AuthChangeTable,
    storage::LvmtStore,
    table_schema::{AmtNodes, FlatKeyValue, SlotAllocations},
};

pub struct LvmtStorage<D: DatabaseTrait> {
    backend: Arc<D>,
    key_value_cache: Arc<Mutex<VersionedStoreCache<FlatKeyValue>>>,
    amt_node_cache: Arc<Mutex<VersionedStoreCache<AmtNodes>>>,
    slot_alloc_cache: Arc<Mutex<VersionedStoreCache<SlotAllocations>>>,
}

impl<D: DatabaseTrait> LvmtStorage<D> {
    pub fn new(backend: Arc<D>, log_path: impl AsRef<Path>) -> Result<Self> {
        let history_number_table = Arc::new(backend.view::<HistoryNumberSchema>()?);
        let (parent_of_root_commit_id, history_number_of_root) =
            match history_number_table.iter_rev_from_end()?.next() {
                Some(latest) => {
                    let (parent_of_root_history_number, parent_of_root_cid) = latest?;
                    (
                        Some(*parent_of_root_cid.as_ref()),
                        parent_of_root_history_number.as_ref() + 1,
                    )
                }
                None => (None, 0),
            };
        let height_of_root = history_number_to_height(history_number_of_root);

        Ok(Self {
            backend,
            key_value_cache: Mutex::new(VersionedStoreCache::new(
                &log_path,
                parent_of_root_commit_id,
                height_of_root,
            )?)
            .into(),
            amt_node_cache: Mutex::new(VersionedStoreCache::new(
                &log_path,
                parent_of_root_commit_id,
                height_of_root,
            )?)
            .into(),
            slot_alloc_cache: Mutex::new(VersionedStoreCache::new(
                log_path,
                parent_of_root_commit_id,
                height_of_root,
            )?)
            .into(),
        })
    }

    pub fn get_backend(&self) -> Arc<D> {
        self.backend.clone()
    }

    pub fn as_manager(&self) -> Result<LvmtStore<'_>> {
        let key_value_store =
            VersionedStore::new(self.backend.clone(), self.key_value_cache.clone())?;
        let amt_node_store =
            VersionedStore::new(self.backend.clone(), self.amt_node_cache.clone())?;
        let slot_alloc_store =
            VersionedStore::new(self.backend.clone(), self.slot_alloc_cache.clone())?;
        let auth_changes =
            KeyValueStoreBulks::new(Arc::new(self.backend.view::<AuthChangeTable>()?));

        Ok(LvmtStore::new(
            key_value_store,
            amt_node_store,
            slot_alloc_store,
            auth_changes,
        ))
    }

    pub fn commit(&mut self, write_schema: <D as DatabaseTrait>::WriteSchema) -> Result<()> {
        let backend =
            Arc::get_mut(&mut self.backend).expect("Exclusive access to backend required");
        backend.commit(write_schema)
    }

    // check whether `commit_id` is already in historical part
    // TODO: this function should be invoked in many interfaces
    fn is_in_historical_part(&self, commit_id: CommitID) -> Result<bool> {
        let commit_id_table = Arc::new(self.backend.view::<CommitIDSchema>()?);
        if commit_id_table.get(&commit_id)?.is_some() {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// This function discards the siblings of the nodes from the root (excluded) to `commit_id` (included).
    /// If there is at least one node discarded, return `Ok(true)`; otherwise, return `Ok(false)`.
    /// The input `commit_id` may be already in historical part, so the first thing is to check this.
    pub fn make_pivot(&self, commit_id: CommitID) -> Result<bool> {
        if self.is_in_historical_part(commit_id)? {
            return Ok(false);
        }

        let mut key_value_cache = self.key_value_cache.lock();
        let mut amt_node_cache = self.amt_node_cache.lock();
        let mut slot_alloc_cache = self.slot_alloc_cache.lock();

        let key_value_has_discarded_nodes = key_value_cache.make_pivot(commit_id)?;
        let amt_node_has_discarded_nodes = amt_node_cache.make_pivot(commit_id)?;
        let slot_alloc_has_discarded_nodes = slot_alloc_cache.make_pivot(commit_id)?;

        if (key_value_has_discarded_nodes != amt_node_has_discarded_nodes)
            || (key_value_has_discarded_nodes != slot_alloc_has_discarded_nodes)
        {
            return Err(crate::StorageError::ConsistencyCheckFailure);
        }

        Ok(key_value_has_discarded_nodes)
    }

    pub fn is_newer_than_pending_root(&self, height: u64) -> bool {
        let key_value_cache = self.key_value_cache.lock();
        let height_of_root = key_value_cache.get_height_of_root();
        height > height_of_root
    }

    /// `new_root_height` and `pivot_commit_id` should be in the pending part.
    /// `new_root_height` should not be newer than `pivot_commit_id`.
    /// This function make the ancestor of `pivot_commit_id` at `new_root_height` to be the new pending root.
    pub fn confirmed_pending_to_history_with_height(
        &self,
        new_root_height: u64,
        pivot_commit_id: CommitID,
        write_schema: &D::WriteSchema,
    ) -> Result<()> {
        let key_value_cache = self.key_value_cache.lock();
        let new_root_commit_id =
            key_value_cache.get_ancestor_commit_at_height(new_root_height, pivot_commit_id)?;
        drop(key_value_cache);

        self.confirmed_pending_to_history_with_commit_id(new_root_commit_id, write_schema)
    }

    /// The `new_root_commit_id` should be in the pending part, otherwise, an error will be returned.
    pub fn confirmed_pending_to_history_with_commit_id(
        &self,
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
