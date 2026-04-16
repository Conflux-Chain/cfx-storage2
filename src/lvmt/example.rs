use std::{fs, path::Path, sync::Arc};

use crate::{
    backends::{
        impls::kvdb_rocksdb::WrappedRocksDb, DatabaseTrait, HistoricalTableName, PendingTableName,
        TableRead,
    },
    errors::Result,
    middlewares::{
        confirm_ids_to_history, confirm_maps_to_history, history_number_to_height,
        primitives_clear_pending_schema, primitives_gc_until_height,
        primitives_initialize_empty_schema, primitives_recover_schema,
        primitives_verify_no_newer_records, primitives_verify_schema_is_empty, CommitID,
        HistoryNumberSchema, KeyValueStoreBulks, PendingKeyValueConfig, TreeWithTracker,
        VersionedStore, VersionedStoreCache, VersionedStoreReader,
    },
    StorageError,
};

use super::{
    auth_changes::AuthChangeTable,
    storage::{LvmtStore, LvmtStoreReader},
    table_schema::{AmtNodes, FlatKeyValue, SlotAllocations},
};

pub struct LvmtStorage<D: DatabaseTrait<HistoricalTableName>, P: DatabaseTrait<PendingTableName>> {
    pub(super) historical_db: Arc<D>,

    pending_db: Arc<P>,

    pub(super) key_value_cache: VersionedStoreCache<FlatKeyValue, P>,
    pub(super) amt_node_cache: VersionedStoreCache<AmtNodes, P>,
    pub(super) slot_alloc_cache: VersionedStoreCache<SlotAllocations, P>,

    last_gc_height: u64,
}

impl LvmtStorage<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>> {
    /// Creates a new `Arc<LvmtStorage>` instance directly from file paths for RocksDB.
    ///
    /// This is a convenience constructor for the most common use case. It handles:
    /// - Creating the database directories if they don't exist.
    /// - **Validating that the historical and pending paths are not the same.**
    /// - Opening the RocksDB instances.
    /// - Calling the generic `new` constructor for initialization/recovery.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - The paths are identical.
    /// - Directory creation fails.
    /// - The database files cannot be opened.
    /// - The internal recovery/initialization logic fails.
    pub fn new_from_paths<P: AsRef<Path>>(
        historical_db_path: P,
        pending_db_path: P,
    ) -> Result<Self> {
        fs::create_dir_all(historical_db_path.as_ref()).map_err(|e| {
            StorageError::DbInitError(format!("Failed to create historical db path: {}", e))
        })?;
        fs::create_dir_all(pending_db_path.as_ref()).map_err(|e| {
            StorageError::DbInitError(format!("Failed to create pending db path: {}", e))
        })?;

        let historical_canon_path = fs::canonicalize(historical_db_path.as_ref()).map_err(|e| {
            StorageError::DbInitError(format!("Failed to canonicalize historical path: {}", e))
        })?;
        let pending_canon_path = fs::canonicalize(pending_db_path.as_ref()).map_err(|e| {
            StorageError::DbInitError(format!("Failed to canonicalize pending path: {}", e))
        })?;
        if historical_canon_path == pending_canon_path {
            return Err(StorageError::InvalidConfig(
                "Historical and pending database paths cannot be the same.".to_string(),
            ));
        }

        let historical_db = WrappedRocksDb::open(historical_db_path).map_err(|e| {
            StorageError::DbInitError(format!("Failed to open historical db: {}", e))
        })?;
        let pending_db = WrappedRocksDb::open(pending_db_path)
            .map_err(|e| StorageError::DbInitError(format!("Failed to open pending db: {}", e)))?;

        let storage = Self::new(Arc::new(historical_db), Arc::new(pending_db))?;

        Ok(storage)
    }
}

impl<D: DatabaseTrait<HistoricalTableName>, P: DatabaseTrait<PendingTableName>> LvmtStorage<D, P> {
    /// Creates a new LvmtStorage instance, automatically handling recovery or initialization.
    ///
    /// This function first checks the state of the `pending_db`.
    /// - If `pending_db` is empty, it initializes from the latest state of `historical_db`.
    /// - If `pending_db` is not empty, it attempts to recover the state from it.
    /// - If recovery fails, it automatically falls back to bootstrap mode: clearing the `pending_db`
    ///   and re-initializing it. In this case, all unconfirmed commits in the `pending_db` will be lost.
    pub fn new(historical_db: Arc<D>, pending_db: Arc<P>) -> Result<Self> {
        if Self::is_pending_db_empty(&pending_db)? {
            // Scenario 1: The pending_db is empty, so initialize directly using the 'from_empty' logic.
            Self::from_empty_pending(historical_db, pending_db)
        } else {
            // Scenario 2: The pending_db is not empty, so attempt recovery.
            // Note: We clone the Arcs here because if 'from_recovery' fails, we need to
            // pass ownership of the Arcs to 'from_bootstrap'. Cloning an Arc is cheap
            // (it only increments the reference count).
            match Self::from_recovery(historical_db.clone(), pending_db.clone()) {
                Ok(instance) => Ok(instance),
                Err(e) => {
                    // Scenario 3: Recovery failed, fall back to bootstrap mode.
                    // Log a critical error. This situation is severe because it implies data loss
                    // (unconfirmed commits) and suggests a potential data corruption or a bug that
                    // should not happen in principle.
                    // We use `error!` as it's the highest severity level in the standard `log` crate.
                    // We don't `panic!` because the system is designed to recover by bootstrapping,
                    // and panicking would prevent this automatic recovery.
                    log::error!(
                        "Failed to recover from pending database: {}. This is a critical event. \
                        Falling back to bootstrap mode. ALL UNCONFIRMED COMMITS in the pending database will be LOST.",
                        e
                    );
                    Self::from_bootstrap(historical_db, pending_db)
                }
            }
        }
    }

    /// Checks if the `pending_db` is empty.
    ///
    /// It is considered empty if all relevant data schemas contain no entries.
    fn is_pending_db_empty(pending_db: &Arc<P>) -> Result<bool> {
        let kv_is_empty = primitives_verify_schema_is_empty::<
            PendingKeyValueConfig<FlatKeyValue, CommitID>,
            P,
        >(pending_db)
        .is_ok();
        let amt_is_empty = primitives_verify_schema_is_empty::<
            PendingKeyValueConfig<AmtNodes, CommitID>,
            P,
        >(pending_db)
        .is_ok();
        let slot_is_empty = primitives_verify_schema_is_empty::<
            PendingKeyValueConfig<SlotAllocations, CommitID>,
            P,
        >(pending_db)
        .is_ok();

        // The entire pending_db is considered empty only if all its component schemas are empty.
        // If a situation arises where some schemas are empty and others are not, this indicates
        // an inconsistent state. In such a case, this function will return `false`, correctly
        // triggering the recovery process (and potentially a bootstrap if recovery fails).
        Ok(kv_is_empty && amt_is_empty && slot_is_empty)
    }

    #[cfg(fuzzing)]
    pub fn from_recovery_for_fuzzing(historical_db: Arc<D>, pending_db: Arc<P>) -> Result<Self> {
        Self::from_recovery(historical_db, pending_db)
    }

    /// Creates a new LvmtStorage instance, opening databases and running the recovery process.
    pub(super) fn from_recovery(historical_db: Arc<D>, pending_db: Arc<P>) -> Result<Self> {
        let (expected_parent_of_root, expected_height_of_root) =
            Self::get_latest_from_history(&historical_db)?;

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
        Self::verify_recovery_consistency(
            &kv_tree_with_tracker,
            &amt_tree_with_tracker,
            &slot_tree_with_tracker,
            &pending_db,
            expected_height_of_root,
        )?;

        // Initialization is complete. Now, create the in-memory VersionedMap instances.
        let key_value_cache = VersionedStoreCache::from_initialized_state(kv_tree_with_tracker);
        let amt_node_cache = VersionedStoreCache::from_initialized_state(amt_tree_with_tracker);
        let slot_alloc_cache = VersionedStoreCache::from_initialized_state(slot_tree_with_tracker);

        Ok(Self {
            historical_db,
            pending_db,
            key_value_cache,
            amt_node_cache,
            slot_alloc_cache,
            last_gc_height: 0,
        })
    }

    #[cfg(fuzzing)]
    pub fn from_bootstrap_for_fuzzing(historical_db: Arc<D>, pending_db: Arc<P>) -> Result<Self> {
        Self::from_bootstrap(historical_db, pending_db)
    }

    /// Performs a bootstrap recovery.
    ///
    /// This function is used in scenarios where `from_recovery` fails (e.g., when the pending_db
    /// state lags behind the historical_db, making it untrustworthy). It completely clears the
    /// `pending_db`, and then creates a new, clean pending component based on the current state
    /// of `historical_db`.
    ///
    /// **Warning**: This operation will destroy all unconfirmed commits in `pending_db`.
    pub(super) fn from_bootstrap(historical_db: Arc<D>, pending_db: Arc<P>) -> Result<Self> {
        // 1. Clear the entire pending database.
        // Create a single WriteSchema for the entire atomic clearup operation.
        let pending_write_schema = P::write_schema();
        primitives_clear_pending_schema::<PendingKeyValueConfig<FlatKeyValue, CommitID>, P>(
            &pending_db,
            &pending_write_schema,
        )?;
        primitives_clear_pending_schema::<PendingKeyValueConfig<AmtNodes, CommitID>, P>(
            &pending_db,
            &pending_write_schema,
        )?;
        primitives_clear_pending_schema::<PendingKeyValueConfig<SlotAllocations, CommitID>, P>(
            &pending_db,
            &pending_write_schema,
        )?;
        pending_db.commit(pending_write_schema)?;

        // 2. After clearing, this is equivalent to starting from an empty pending_db.
        Self::from_empty_pending(historical_db, pending_db)
    }

    #[cfg(fuzzing)]
    pub fn from_empty_pending_for_fuzzing(historical_db: Arc<D>, pending_db: Arc<P>) -> Result<Self> {
        Self::from_empty_pending(historical_db, pending_db)
    }

    /// Creates a new LvmtStorage instance. It is the caller's responsibility to ensure that the `pending_db` is empty.
    pub(super) fn from_empty_pending(historical_db: Arc<D>, pending_db: Arc<P>) -> Result<Self> {
        let (expected_parent_of_root, expected_height_of_root) =
            Self::get_latest_from_history(&historical_db)?;

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
        Self::verify_recovery_consistency(
            &kv_tree_with_tracker,
            &amt_tree_with_tracker,
            &slot_tree_with_tracker,
            &pending_db,
            expected_height_of_root,
        )?;

        // Initialization is complete. Now, create the in-memory VersionedMap instances.
        let key_value_cache = VersionedStoreCache::from_initialized_state(kv_tree_with_tracker);
        let amt_node_cache = VersionedStoreCache::from_initialized_state(amt_tree_with_tracker);
        let slot_alloc_cache = VersionedStoreCache::from_initialized_state(slot_tree_with_tracker);

        Ok(Self {
            historical_db,
            pending_db,
            key_value_cache,
            amt_node_cache,
            slot_alloc_cache,
            last_gc_height: 0,
        })
    }

    fn get_latest_from_history(historical_db: &Arc<D>) -> Result<(Option<CommitID>, u64)> {
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

    fn verify_recovery_consistency(
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
}

impl<D: DatabaseTrait<HistoricalTableName>, P: DatabaseTrait<PendingTableName>> LvmtStorage<D, P> {
    fn get_durable_height(&self) -> Result<u64> {
        let (_, height_of_pending_root) = Self::get_latest_from_history(&self.historical_db)?;
        // TODO: safety_height_diff should be a parameter
        let safety_height_diff = 5;
        let durable_height = height_of_pending_root.saturating_sub(safety_height_diff);
        Ok(durable_height)
    }

    /// Performs background garbage collection on the pending persistence layer.
    ///
    /// This method acts as the "upper-level application".
    /// It orchestrates the cleanup by:
    /// 1. Fetching the `durable_height` from the historical database.
    /// 2. Calling the `gc_until_height` primitive from the `persistence` module.
    /// 3. Committing the changes to the pending database to ensure atomicity.
    pub fn background_cleanup(&mut self) -> Result<()> {
        // Step 1: Get durable_height.
        let durable_height = self.get_durable_height()?;

        if durable_height == 0 {
            return Ok(());
        }

        // Skip if there's nothing new to clean since last GC.
        if self.last_gc_height >= durable_height {
            return Ok(());
        }

        let from_height = self.last_gc_height;

        // Step 2: Prepare the write batch/schema for the pending DB.
        let write_schema = P::write_schema();

        // Step 3: Call the low-level primitive to populate the write schema.
        // Multiple tables should use the same write_schema.
        primitives_gc_until_height::<PendingKeyValueConfig<FlatKeyValue, CommitID>, _>(
            &self.pending_db,
            &write_schema,
            durable_height,
            from_height,
        )?;

        primitives_gc_until_height::<PendingKeyValueConfig<AmtNodes, CommitID>, _>(
            &self.pending_db,
            &write_schema,
            durable_height,
            from_height,
        )?;

        primitives_gc_until_height::<PendingKeyValueConfig<SlotAllocations, CommitID>, _>(
            &self.pending_db,
            &write_schema,
            durable_height,
            from_height,
        )?;

        // Step 4: Commit the changes atomically. The primitive itself doesn't commit.
        self.pending_db.commit(write_schema)?;

        // Step 5: Update last_gc_height after successful cleanup.
        self.last_gc_height = durable_height;

        Ok(())
    }
}

impl<D: DatabaseTrait<HistoricalTableName>, P: DatabaseTrait<PendingTableName>> LvmtStorage<D, P> {
    /// Creates a read-only view of the storage.
    ///
    /// Unlike [`as_manager`], this only requires `&self`, enabling concurrent
    /// read access when the storage is protected by an `RwLock`.
    pub fn as_reader(&self) -> Result<LvmtStoreReader<'_, '_, P>> {
        let key_value_reader =
            VersionedStoreReader::new(self.historical_db.clone(), &self.key_value_cache)?;

        Ok(LvmtStoreReader::new(
            self.pending_db.clone(),
            key_value_reader,
        ))
    }
}

impl<D: DatabaseTrait<HistoricalTableName>, P: DatabaseTrait<PendingTableName>> LvmtStorage<D, P> {
    pub fn as_manager(&mut self) -> Result<LvmtStore<'_, '_, P>> {
        let key_value_store =
            VersionedStore::new(self.historical_db.clone(), &mut self.key_value_cache)?;
        let amt_node_store =
            VersionedStore::new(self.historical_db.clone(), &mut self.amt_node_cache)?;
        let slot_alloc_store =
            VersionedStore::new(self.historical_db.clone(), &mut self.slot_alloc_cache)?;
        let auth_changes =
            KeyValueStoreBulks::new(Arc::new(self.pending_db.view::<AuthChangeTable>()?));

        Ok(LvmtStore::new(
            self.pending_db.clone(),
            key_value_store,
            amt_node_store,
            slot_alloc_store,
            auth_changes,
        ))
    }
}

impl<D: DatabaseTrait<HistoricalTableName>, P: DatabaseTrait<PendingTableName>> LvmtStorage<D, P> {
    pub(super) fn commit_to_pending_db(&self, pending_write_schema: P::WriteSchema) -> Result<()> {
        self.pending_db.commit(pending_write_schema)
    }

    pub(super) fn commit_to_historical_db(
        &self,
        write_schema: <D as DatabaseTrait<HistoricalTableName>>::WriteSchema,
    ) -> Result<()> {
        self.historical_db.commit(write_schema)
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
        &mut self,
        new_root_height: u64,
        pivot_commit_id: CommitID,
    ) -> Result<()> {
        let new_root_commit_id = self
            .key_value_cache
            .get_ancestor_commit_at_height(new_root_height, pivot_commit_id)?;

        self.confirmed_pending_to_history_with_commit_id_inner(new_root_commit_id)
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
    pub fn confirmed_pending_to_history_with_commit_id_inner(
        &mut self,
        new_root_commit_id: CommitID,
    ) -> Result<()> {
        // pending part
        let pending_write_schema = P::write_schema();

        let maybe_key_value_confirmed_path = self
            .key_value_cache
            .change_root(new_root_commit_id, &pending_write_schema)?;
        if let Some(key_value_confirmed_path) = maybe_key_value_confirmed_path {
            let amt_node_confirmed_path = self
                .amt_node_cache
                .change_root(new_root_commit_id, &pending_write_schema)?
                .expect("AMT node cache should have changed root if key-value cache did");
            let slot_alloc_confirmed_path = self
                .slot_alloc_cache
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
