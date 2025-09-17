use std::sync::Arc;

use parking_lot::Mutex;

use crate::{
    backends::{DatabaseTrait, HistoricalTableName, PendingTableName, TableRead},
    errors::Result,
    middlewares::{
        confirm_ids_to_history, confirm_maps_to_history, history_number_to_height,
        primitives_gc_until_height, primitives_initialize_empty_schema, primitives_recover_schema,
        primitives_verify_no_newer_records, primitives_verify_schema_is_empty, CommitID,
        CommitIDSchema, HistoryNumberSchema, KeyValueStoreBulks, PendingKeyValueConfig,
        TreeWithTracker, VersionedStore, VersionedStoreCache,
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

fn verify_recovery_consistency<P: DatabaseTrait<PendingTableName>>(
    kv_tree_with_tracker: &TreeWithTracker<PendingKeyValueConfig<FlatKeyValue, CommitID>>,
    amt_tree_with_tracker: &TreeWithTracker<PendingKeyValueConfig<AmtNodes, CommitID>>,
    slot_tree_with_tracker: &TreeWithTracker<PendingKeyValueConfig<SlotAllocations, CommitID>>,
    pending_db: &Arc<P>,
    expected_height_of_root: u64,
) -> Result<()> {
    if kv_tree_with_tracker.tracker != amt_tree_with_tracker.tracker
        || kv_tree_with_tracker.tracker != slot_tree_with_tracker.tracker
    {
        return Err(crate::StorageError::InconsistentPendingFromRecovery);
    }

    let tracker = &kv_tree_with_tracker.tracker;
    primitives_verify_no_newer_records::<PendingKeyValueConfig<FlatKeyValue, CommitID>, P>(
        pending_db,
        expected_height_of_root,
        tracker,
    )?;
    primitives_verify_no_newer_records::<PendingKeyValueConfig<AmtNodes, CommitID>, P>(
        pending_db,
        expected_height_of_root,
        tracker,
    )?;
    primitives_verify_no_newer_records::<PendingKeyValueConfig<SlotAllocations, CommitID>, P>(
        pending_db,
        expected_height_of_root,
        tracker,
    )?;

    Ok(())
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

        // Verify consistency of three tree_with_tracker and the pending_db
        verify_recovery_consistency(
            &kv_tree_with_tracker,
            &amt_tree_with_tracker,
            &slot_tree_with_tracker,
            &pending_db,
            expected_height_of_root,
        )?;

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
            );
        let amt_tree_with_tracker =
            primitives_initialize_empty_schema::<PendingKeyValueConfig<AmtNodes, CommitID>>(
                &pending_write_schema,
                expected_parent_of_root,
                expected_height_of_root,
            );
        let slot_tree_with_tracker =
            primitives_initialize_empty_schema::<PendingKeyValueConfig<SlotAllocations, CommitID>>(
                &pending_write_schema,
                expected_parent_of_root,
                expected_height_of_root,
            );

        // Atomically commit all changes (initial snapshots) to the pending DB.
        pending_db.commit(pending_write_schema)?;

        // Verify consistency of three tree_with_tracker and the pending_db
        verify_recovery_consistency(
            &kv_tree_with_tracker,
            &amt_tree_with_tracker,
            &slot_tree_with_tracker,
            &pending_db,
            expected_height_of_root,
        )?;

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

    fn get_durable_height(&self) -> Result<u64> {
        let (_, height_of_pending_root) = get_latest_from_history(&self.historical_db)?;
        // TODO: safety_height_diff should be a parameter
        let safety_height_diff = 5;
        let durable_height = if height_of_pending_root > safety_height_diff {
            height_of_pending_root - safety_height_diff
        } else {
            0
        };
        Ok(durable_height)
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
        let durable_height = self.get_durable_height()?;

        if durable_height == 0 {
            return Ok(());
        }

        // Step 2: Prepare the write batch/schema for the pending DB.
        let write_schema = P::write_schema();

        // Step 3: Call the low-level primitive to populate the write schema.
        // Multiple tables should use the same write_schema.
        primitives_gc_until_height::<PendingKeyValueConfig<FlatKeyValue, CommitID>, _>(
            &self.pending_db,
            &write_schema,
            durable_height,
        )?;

        primitives_gc_until_height::<PendingKeyValueConfig<AmtNodes, CommitID>, _>(
            &self.pending_db,
            &write_schema,
            durable_height,
        )?;

        primitives_gc_until_height::<PendingKeyValueConfig<SlotAllocations, CommitID>, _>(
            &self.pending_db,
            &write_schema,
            durable_height,
        )?;

        // Step 4: Commit the changes atomically. The primitive itself doesn't commit.
        self.pending_db.commit(write_schema)?;

        Ok(())
    }

    pub fn commit_to_historical_db(
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

    /// Promotes an ancestor of `pivot_commit_id` at `new_root_height` to become the new pending root.
    ///
    /// This is a convenience function that determines the new root commit ID based on the
    /// provided `new_root_height` and `pivot_commit_id`, then calls the underlying
    /// `confirmed_pending_to_history_with_commit_id` to execute the full operation.
    ///
    /// **Crucially, this function handles the entire persistence process internally.** Upon
    /// successful return, changes to both the pending and historical components have been
    /// committed to their respective databases. The caller does not need to manage any
    /// database transactions.
    ///
    /// Preconditions:
    /// - `pivot_commit_id` and its ancestor at `new_root_height` must exist in the pending part.
    /// - `new_root_height` must not be greater than the height of `pivot_commit_id`.
    ///
    /// Note: The database commits for the pending and historical parts are not atomic and
    /// a failure between them could lead to an inconsistent state.
    pub fn confirmed_pending_to_history_with_height(
        &self,
        new_root_height: u64,
        pivot_commit_id: CommitID,
    ) -> Result<()> {
        let key_value_cache = self.key_value_cache.lock();
        let new_root_commit_id =
            key_value_cache.get_ancestor_commit_at_height(new_root_height, pivot_commit_id)?;
        drop(key_value_cache);

        self.confirmed_pending_to_history_with_commit_id(new_root_commit_id)
    }

    /// Promotes the given `new_root_commit_id` to be the new root of the pending component.
    ///
    /// This process involves two main steps:
    /// 1.  The pending component's in-memory state is updated to reflect the new root, and these
    ///     changes are persisted to the `pending_db`.
    /// 2.  The path of nodes from the old root to the new root's parent is pruned from the
    ///     pending component and moved to the `historical_db`.
    ///
    /// All database write schemas (`pending_write_schema` and `historical_write_schema`) are
    /// created and committed internally within this function.
    ///
    /// An error will be returned if the `new_root_commit_id` does not exist in the pending part.
    ///
    /// **Note**: The commits to the `pending_db` and `historical_db` are not atomic. A failure
    /// between these two operations could result in an inconsistent state.
    pub fn confirmed_pending_to_history_with_commit_id(
        &self,
        new_root_commit_id: CommitID,
    ) -> Result<()> {
        let mut key_value_cache = self.key_value_cache.lock();
        let mut amt_node_cache = self.amt_node_cache.lock();
        let mut slot_alloc_cache = self.slot_alloc_cache.lock();

        // pending part
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

            // historical part
            let historical_write_schema = D::write_schema();

            confirm_ids_to_history::<D>(
                self.historical_db.clone(),
                start_height,
                commit_ids,
                &historical_write_schema,
            )?;

            confirm_maps_to_history::<D, FlatKeyValue>(
                self.historical_db.clone(),
                start_height,
                key_value_confirmed_path.key_value_maps,
                &historical_write_schema,
            )?;
            confirm_maps_to_history::<D, AmtNodes>(
                self.historical_db.clone(),
                start_height,
                amt_node_confirmed_path.key_value_maps,
                &historical_write_schema,
            )?;
            confirm_maps_to_history::<D, SlotAllocations>(
                self.historical_db.clone(),
                start_height,
                slot_alloc_confirmed_path.key_value_maps,
                &historical_write_schema,
            )?;

            self.commit_to_historical_db(historical_write_schema)?;
        } // else: nothing is changed and no records are in pending_write_schema, so skip directly

        Ok(())
    }
}
