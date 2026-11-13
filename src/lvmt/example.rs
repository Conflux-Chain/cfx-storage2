use std::sync::Arc;

use parking_lot::Mutex;

use crate::{
    backends::{DatabaseTrait, HistoricalTableName, PendingTableName, TableRead},
    errors::Result,
    middlewares::{
        confirm_ids_to_history, confirm_maps_to_history, history_number_to_height,
        primitives_initialize_empty_schema, primitives_recover_schema,
        primitives_verify_schema_is_empty, CommitID, CommitIDSchema, HistoryNumberSchema,
        KeyValueStoreBulks, PendingKeyValueConfig, VersionedStore, VersionedStoreCache,
    },
};

use super::{
    auth_changes::AuthChangeTable,
    storage::LvmtStore,
    table_schema::{AmtNodes, FlatKeyValue, SlotAllocations},
};

pub struct LvmtStorage<D: DatabaseTrait<HistoricalTableName>, P: DatabaseTrait<PendingTableName>> {
    historical_db: Arc<D>,

    pending_db: Arc<P>,

    key_value_cache: Arc<Mutex<VersionedStoreCache<FlatKeyValue, P>>>,
    amt_node_cache: Arc<Mutex<VersionedStoreCache<AmtNodes, P>>>,
    slot_alloc_cache: Arc<Mutex<VersionedStoreCache<SlotAllocations, P>>>,
}

fn get_latest_from_history<D: DatabaseTrait<HistoricalTableName>>(
    historical_db: &Arc<D>,
) -> Result<(Option<CommitID>, u64)> {
    let history_number_table = Arc::new(historical_db.view::<HistoryNumberSchema>()?);
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
    Ok((parent_of_root_commit_id, height_of_root))
}

fn todo_fn() {
    unimplemented!()
}

impl<D: DatabaseTrait<HistoricalTableName>, P: DatabaseTrait<PendingTableName>> LvmtStorage<D, P> {
    /// Creates a new LvmtStorage instance, opening databases and running the recovery process.
    pub fn new_from_recovery(historical_db: Arc<D>, pending_db: Arc<P>) -> Result<Self> {
        let (expected_parent_of_root, expected_height_of_root) =
            get_latest_from_history(&historical_db)?;

        // Create a single WriteSchema for the entire atomic recovery operation.
        let pending_write_schema = P::write_schema();

        // Run the recovery process for each schema type.
        // All database modifications will be collected in the *same* write_schema.
        let kv_tree_with_tracker =
            primitives_recover_schema::<PendingKeyValueConfig<FlatKeyValue, CommitID>, P>(
                &pending_db,
                &pending_write_schema,
                expected_parent_of_root,
                expected_height_of_root,
            )?;
        let amt_tree_with_tracker =
            primitives_recover_schema::<PendingKeyValueConfig<AmtNodes, CommitID>, P>(
                &pending_db,
                &pending_write_schema,
                expected_parent_of_root,
                expected_height_of_root,
            )?;
        let slot_tree_with_tracker =
            primitives_recover_schema::<PendingKeyValueConfig<SlotAllocations, CommitID>, P>(
                &pending_db,
                &pending_write_schema,
                expected_parent_of_root,
                expected_height_of_root,
            )?;

        // Atomically commit all changes (cleanups, initial snapshots) to the pending DB.
        pending_db.commit(pending_write_schema)?;

        // TODO: verify consistency of three tree_with_tracker and the pending_db
        todo_fn();

        // Initialization is complete. Now, create the in-memory VersionedMap instances.
        let key_value_cache = Arc::new(Mutex::new(VersionedStoreCache::from_initialized_state(
            kv_tree_with_tracker,
        )));
        let amt_node_cache = Arc::new(Mutex::new(VersionedStoreCache::from_initialized_state(
            amt_tree_with_tracker,
        )));
        let slot_alloc_cache = Arc::new(Mutex::new(VersionedStoreCache::from_initialized_state(
            slot_tree_with_tracker,
        )));

        Ok(Self {
            historical_db,
            pending_db,
            key_value_cache,
            amt_node_cache,
            slot_alloc_cache,
        })
    }

    /// Creates a new LvmtStorage instance. It is the caller's responsibility to ensure that the `pending_db` is empty.
    pub fn new_from_empty_pending(historical_db: Arc<D>, pending_db: Arc<P>) -> Result<Self> {
        let (expected_parent_of_root, expected_height_of_root) =
            get_latest_from_history(&historical_db)?;

        // Verify the `pending_db` is empty for each schema type.
        primitives_verify_schema_is_empty::<PendingKeyValueConfig<FlatKeyValue, CommitID>, P>(
            &pending_db,
        )?;
        primitives_verify_schema_is_empty::<PendingKeyValueConfig<AmtNodes, CommitID>, P>(
            &pending_db,
        )?;
        primitives_verify_schema_is_empty::<PendingKeyValueConfig<SlotAllocations, CommitID>, P>(
            &pending_db,
        )?;

        // Create a single WriteSchema for the entire atomic bootstrap operation.
        let pending_write_schema = P::write_schema();

        // Run the boostrap process for each schema type.
        // All database modifications will be collected in the *same* write_schema.
        let kv_tree_with_tracker =
            primitives_initialize_empty_schema::<PendingKeyValueConfig<FlatKeyValue, CommitID>>(
                &pending_write_schema,
                expected_parent_of_root,
                expected_height_of_root,
            )?;
        let amt_tree_with_tracker =
            primitives_initialize_empty_schema::<PendingKeyValueConfig<AmtNodes, CommitID>>(
                &pending_write_schema,
                expected_parent_of_root,
                expected_height_of_root,
            )?;
        let slot_tree_with_tracker =
            primitives_initialize_empty_schema::<PendingKeyValueConfig<SlotAllocations, CommitID>>(
                &pending_write_schema,
                expected_parent_of_root,
                expected_height_of_root,
            )?;

        // Atomically commit all changes (initial snapshots) to the pending DB.
        pending_db.commit(pending_write_schema)?;

        // TODO: verify consistency of three tree_with_tracker and the pending_db
        // todo_fn();

        // Initialization is complete. Now, create the in-memory VersionedMap instances.
        let key_value_cache = Arc::new(Mutex::new(VersionedStoreCache::from_initialized_state(
            kv_tree_with_tracker,
        )));
        let amt_node_cache = Arc::new(Mutex::new(VersionedStoreCache::from_initialized_state(
            amt_tree_with_tracker,
        )));
        let slot_alloc_cache = Arc::new(Mutex::new(VersionedStoreCache::from_initialized_state(
            slot_tree_with_tracker,
        )));

        Ok(Self {
            historical_db,
            pending_db,
            key_value_cache,
            amt_node_cache,
            slot_alloc_cache,
        })
    }

    pub fn get_backend(&self) -> Arc<D> {
        self.historical_db.clone()
    }

    pub fn as_manager(&self) -> Result<LvmtStore<'_, P>> {
        let key_value_store =
            VersionedStore::new(self.historical_db.clone(), self.key_value_cache.clone())?;
        let amt_node_store =
            VersionedStore::new(self.historical_db.clone(), self.amt_node_cache.clone())?;
        let slot_alloc_store =
            VersionedStore::new(self.historical_db.clone(), self.slot_alloc_cache.clone())?;
        let auth_changes =
            KeyValueStoreBulks::new(Arc::new(self.historical_db.view::<AuthChangeTable>()?));

        Ok(LvmtStore::new(
            self.pending_db.clone(),
            key_value_store,
            amt_node_store,
            slot_alloc_store,
            auth_changes,
        ))
    }

    pub fn commit(
        &self,
        write_schema: <D as DatabaseTrait<HistoricalTableName>>::WriteSchema,
    ) -> Result<()> {
        self.historical_db.commit(write_schema)
    }

    fn commit_to_pending_db(&self, pending_write_schema: P::WriteSchema) -> Result<()> {
        self.pending_db.commit(pending_write_schema)
    }

    // check whether `commit_id` is already in historical part
    // TODO: this function should be invoked in many interfaces
    fn is_in_historical_part(&self, commit_id: CommitID) -> Result<bool> {
        let commit_id_table = Arc::new(self.historical_db.view::<CommitIDSchema>()?);
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

        let pending_write_schema = P::write_schema();

        let mut key_value_cache = self.key_value_cache.lock();
        let mut amt_node_cache = self.amt_node_cache.lock();
        let mut slot_alloc_cache = self.slot_alloc_cache.lock();

        let key_value_has_discarded_nodes =
            key_value_cache.make_pivot(commit_id, &pending_write_schema)?;
        let amt_node_has_discarded_nodes =
            amt_node_cache.make_pivot(commit_id, &pending_write_schema)?;
        let slot_alloc_has_discarded_nodes =
            slot_alloc_cache.make_pivot(commit_id, &pending_write_schema)?;

        if (key_value_has_discarded_nodes != amt_node_has_discarded_nodes)
            || (key_value_has_discarded_nodes != slot_alloc_has_discarded_nodes)
        {
            return Err(crate::StorageError::ConsistencyCheckFailure);
        }

        self.commit_to_pending_db(pending_write_schema)?;

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

        let pending_write_schema = P::write_schema();

        let maybe_key_value_confirmed_path =
            key_value_cache.change_root(new_root_commit_id, &pending_write_schema)?;
        if let Some(key_value_confirmed_path) = maybe_key_value_confirmed_path {
            let amt_node_confirmed_path = amt_node_cache
                .change_root(new_root_commit_id, &pending_write_schema)?
                .expect("AMT node cache should have changed root if key-value cache did");
            let slot_alloc_confirmed_path = slot_alloc_cache
                .change_root(new_root_commit_id, &pending_write_schema)?
                .expect("Slot alloc cache should have changed root if key-value cache did");

            assert!(key_value_confirmed_path.is_same_path(&amt_node_confirmed_path));
            assert!(key_value_confirmed_path.is_same_path(&slot_alloc_confirmed_path));

            let start_height = key_value_confirmed_path.start_height;
            let commit_ids = &key_value_confirmed_path.commit_ids;

            self.commit_to_pending_db(pending_write_schema)?;

            confirm_ids_to_history::<D>(
                self.historical_db.clone(),
                start_height,
                commit_ids,
                write_schema,
            )?;

            confirm_maps_to_history::<D, FlatKeyValue>(
                self.historical_db.clone(),
                start_height,
                key_value_confirmed_path.key_value_maps,
                write_schema,
            )?;
            confirm_maps_to_history::<D, AmtNodes>(
                self.historical_db.clone(),
                start_height,
                amt_node_confirmed_path.key_value_maps,
                write_schema,
            )?;
            confirm_maps_to_history::<D, SlotAllocations>(
                self.historical_db.clone(),
                start_height,
                slot_alloc_confirmed_path.key_value_maps,
                write_schema,
            )?;
        }

        Ok(())
    }
}
