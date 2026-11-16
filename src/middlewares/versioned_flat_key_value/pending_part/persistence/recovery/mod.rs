use thiserror::Error;

/// Errors that can occur during the state recovery process from the database.
#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryError {
    #[error(
        "No valid snapshot found: the pending db does not reach the specified recovery height."
    )]
    NoValidSnapshotFound,
    #[error("Inconsistent snapshot state: the snapshots and WAL do not lead to the recovery height and parent of root.")]
    InconsistentSnapshotState,

    #[error("First snapshot record is not a Meta record")]
    FirstRecordIsNotMeta,

    #[error("Inconsistent WAL record sequence: {0}")]
    InconsistentWalRecord(&'static str),
    #[error("WAL map value count mismatch: expected {expected}, but got {got}.")]
    WalMapValueCountMismatch { expected: u64, got: u64 },
    #[error("Unexpected WAL record found. A new modification should start with a Meta record.")]
    UnexpectedWalRecord,

    #[error("The db after recovery contains newer invalid records.")]
    DirtyRecoveryResult,
}

pub mod primitives {
    //! Contains low-level, generic primitives for state recovery from the persistence layer.
    //!
    //! **Warning**: These functions are low-level building blocks. You should generally
    //! prefer the high-level, encapsulated recovery functions provided by a concrete
    //! application (e.g., `lvmt`), unless you are building a new composite application
    //! yourself.

    mod read_tree_snapshot;
    mod wal_player;

    use log::warn;
    pub use read_tree_snapshot::SnapshotReadError;

    use std::sync::Arc;

    use crate::backends::WriteSchemaTrait;

    use super::RecoveryError;

    use super::super::{
        primitive::delete_wal_by_snapshot_id, DatabaseTrait, ModificationId, PendingKeyValueSchema,
        PendingTableName, PersistenceTracker, RecoverMap, Result, SnapshotId, SnapshotKey,
        SnapshotMapValue, SnapshotNodeDataType, SnapshotRecordType, SnapshotValue, SnapshotsTable,
        TableItem, TableIter, TableRead, Tree, TreeSnapshot, TreeSnapshotNode, TreeWithTracker,
        WalKey, WalKeySpecificPart, WalTable, WalValue,
    };

    /// Executes the recovery logic for a single pending schema from the database.
    /// Returns the recovered in-memory Tree and its corresponding persistence state.
    /// Any required DB modifications are added to the provided write_schema.
    ///
    /// This is a low-level primitive. See the [module-level documentation](self) for usage guidelines.
    pub fn recover_schema<S: PendingKeyValueSchema, P: DatabaseTrait<PendingTableName>>(
        db: &Arc<P>,
        write_schema: &P::WriteSchema,
        parent_of_root: Option<S::CommitId>,
        height_of_root: u64,
    ) -> Result<TreeWithTracker<S>> {
        let snapshots_view = Arc::new(db.view::<SnapshotsTable<S>>()?);
        let wal_view = Arc::new(db.view::<WalTable<S>>()?);

        // Snapshots are written only once every few `change_root`s (see
        // `CHANGE_ROOTS_PER_SNAPSHOT`), so recovery starts from the latest snapshot at
        // or below `height_of_root` and replays the `change_root`s logged after it.
        // Snapshot records are ordered by height with each snapshot's meta record
        // first, so jump from one snapshot's meta record to the next.
        let mut base_height = None;
        let mut has_newer_snapshot = false;
        let mut next_height = Some(0u64);
        while let Some(height) = next_height {
            let seek_key = SnapshotKey::seek_key_for_height(height);
            let Some(item) = snapshots_view.iter(&seek_key.key)?.next() else {
                break;
            };
            let found = item?.0.as_ref().snapshot_root_height;
            if found > height_of_root {
                has_newer_snapshot = true;
                break;
            }
            base_height = Some(found);
            next_height = found.checked_add(1);
        }
        let Some(base_height) = base_height else {
            if has_newer_snapshot {
                Err(RecoveryError::InconsistentSnapshotState)?
            }
            Err(RecoveryError::NoValidSnapshotFound)?
        };

        let seek_key = SnapshotKey::seek_key_for_height(base_height);
        let mut iter = snapshots_view.iter(&seek_key.key)?;
        let (_, snap_value_cow) = iter
            .next()
            .expect("the snapshot was just found at this height")?;
        let (snap_parent_cid, snapshot_id, nodes_count) = match snap_value_cow.as_ref() {
            SnapshotValue::MetaValue {
                parent_of_root,
                snapshot_id,
                nodes_count,
            } => (*parent_of_root, *snapshot_id, *nodes_count),
            SnapshotValue::MapValue(_) => Err(RecoveryError::FirstRecordIsNotMeta)?,
        };

        // Rebuild the tree from the snapshot's base info.
        let (mut tree, maybe_first_invalid_snapshot_item) =
            read_tree_snapshot::read_tree_from_snapshot_iter(
                base_height,
                snap_parent_cid,
                snapshot_id,
                nodes_count,
                &mut iter,
            )?;

        // Replay WAL records in a loop until the replayer signals to stop.
        //
        // A `change_root` that moves on to a new snapshot ID may leave the snapshot to be
        // written later, or never (see `DeferredSnapshot`), while operations after it are
        // logged under the new ID. So once the records of an ID run out right after a
        // completed `change_root`, the replay goes on with the records of the next ID,
        // if any. This does not depend on `CHANGE_ROOTS_PER_SNAPSHOT`, which may differ
        // from the one the records were written with.
        let mut snapshot_id = snapshot_id;
        let mut mod_id = 0;
        let mut change_roots_since_snapshot = 0;
        let mut last_is_change_root = false;
        loop {
            let status = wal_player::replay_one_modification(
                &*wal_view,
                &mut tree,
                snapshot_id,
                ModificationId(mod_id),
            )?;

            match status {
                // A standard operation was applied. Continue to the next modification.
                wal_player::ReplayStatus::Applied => {
                    mod_id += 1;
                    last_is_change_root = false;
                }
                // No more WAL records for this snapshot ID.
                wal_player::ReplayStatus::NoMoreModifications => {
                    // Only a `change_root` moves on to a new snapshot ID, and a snapshot
                    // written for it would be at or below `height_of_root`, so it would be
                    // the base. Records under the next ID otherwise belong to a newer,
                    // invalid snapshot, deleted below.
                    let next_snapshot_id = SnapshotId(snapshot_id.0 + 1);
                    if !last_is_change_root || !has_wal_records(&*wal_view, next_snapshot_id)? {
                        // Replay is cleanly finished.
                        break;
                    }
                    snapshot_id = next_snapshot_id;
                    mod_id = 0;
                    change_roots_since_snapshot = 0;
                    last_is_change_root = false;
                }
                wal_player::ReplayStatus::ChangeRootEncountered(new_root) => {
                    let Some(new_root_height) = tree.get_height_of_commit(&new_root) else {
                        Err(RecoveryError::InconsistentWalRecord(
                            "change_root to a commit not in the tree",
                        ))?
                    };
                    // The pending db is committed before the historical db, and a
                    // `change_root` is not started before the previous one is committed
                    // to both. So every `change_root` up to `height_of_root` completed,
                    // and one beyond it can only be the last, incomplete one.
                    if new_root_height <= height_of_root {
                        tree.change_root(new_root)?;
                        change_roots_since_snapshot += 1;
                        mod_id += 1;
                        last_is_change_root = true;
                        continue;
                    }

                    // --- Rollback Logic ---
                    // 1. Delete all WAL records for this failed modification.
                    let wal_seek_key =
                        WalKey::seek_key_for_snap_mod_id(snapshot_id, ModificationId(mod_id));
                    for wal_item in wal_view.iter(&wal_seek_key.key)? {
                        let (wal_key_cow, _) = wal_item?;
                        if wal_key_cow.as_ref().snapshot_id != snapshot_id
                            || wal_key_cow.as_ref().modification_id != ModificationId(mod_id)
                        {
                            break;
                        }

                        warn!(
                            "Rolling back WAL entry for incomplete 'change_root' transaction. \
                            schema_name={:?}, snapshot_id={}, modification_id={}, deleting_wal_key={:?}",
                            S::KV_NAME,
                            snapshot_id.0,
                            mod_id,
                            wal_key_cow.as_ref()
                        );

                        write_schema.write::<WalTable<S>>((wal_key_cow, None));
                    }

                    // 2. (Crucial Sanity Check) Verify no more WAL records exist for this snapshot_id.
                    // An incomplete `change_root` must be the final operation.
                    let next_mod_seek_key =
                        WalKey::seek_key_for_snap_mod_id(snapshot_id, ModificationId(mod_id + 1));
                    if let Some(next_item) = wal_view.iter(&next_mod_seek_key.key)?.next() {
                        let (next_key, _) = next_item?;
                        if next_key.as_ref().snapshot_id == snapshot_id {
                            Err(RecoveryError::InconsistentWalRecord(
                                "WAL data found after a change_root operation for the same snapshot.",
                            ))?;
                        }
                    }

                    // 3. Delete the WAL records under later snapshot IDs. They were
                    // logged after this `change_root` moved on to a new snapshot ID,
                    // which can be found here only if the historical db lost the
                    // `change_root` after the pending db kept them. If the new snapshot
                    // was written, it is deleted below as a newer, invalid snapshot.
                    let later_seek_key =
                        WalKey::seek_key_for_snapshot(SnapshotId(snapshot_id.0 + 1));
                    for wal_item in wal_view.iter(&later_seek_key.key)? {
                        let (wal_key_cow, _) = wal_item?;
                        write_schema.write::<WalTable<S>>((wal_key_cow, None));
                    }

                    // 4. Stop the replay loop. The `mod_id` is NOT incremented,
                    // because this modification was rolled back.
                    break;
                }
            }
        }

        // The replayed tree must end at the root recorded by the historical part. Below
        // it, the pending db is behind the historical db.
        if tree.get_height_of_root() < height_of_root {
            Err(RecoveryError::NoValidSnapshotFound)?
        }
        if tree.get_height_of_root() != height_of_root
            || tree.get_parent_of_root() != parent_of_root
        {
            Err(RecoveryError::InconsistentSnapshotState)?
        }

        // Clean up any newer, now-invalid snapshots and their WALs.
        if let Some(first_invalid_snapshot_item) = maybe_first_invalid_snapshot_item {
            let mut iter = std::iter::once(Ok(first_invalid_snapshot_item)).chain(iter);
            let mut last_invalid_height = None;
            warn!(
                "WAL replay finished for schema {:?}. Found newer, invalid snapshots above height {}. \
                Deleting them.",
                S::KV_NAME,
                base_height
            );
            for invalid_snapshot_item_res in &mut iter {
                let invalid_snapshot_item = invalid_snapshot_item_res?;

                // The first record of each snapshot is its meta record: delete the
                // snapshot's WAL records along with it.
                let invalid_height = invalid_snapshot_item.0.as_ref().snapshot_root_height;
                if last_invalid_height != Some(invalid_height) {
                    last_invalid_height = Some(invalid_height);
                    let SnapshotValue::MetaValue {
                        snapshot_id: invalid_snapshot_id,
                        ..
                    } = invalid_snapshot_item.1.as_ref()
                    else {
                        Err(RecoveryError::FirstRecordIsNotMeta)?
                    };
                    delete_wal_by_snapshot_id::<S, P>(
                        &wal_view,
                        write_schema,
                        *invalid_snapshot_id,
                    )?;
                }

                // Delete this record.
                write_schema.write::<SnapshotsTable<S>>((invalid_snapshot_item.0, None));
            }
        }

        let tracker = PersistenceTracker {
            snapshot_id,
            next_modification_id: ModificationId(mod_id),
            change_roots_since_snapshot,
        };

        Ok(TreeWithTracker { tree, tracker })
    }

    /// Whether the WAL holds any record under `snapshot_id`.
    fn has_wal_records<S: PendingKeyValueSchema>(
        wal_view: &impl TableRead<WalTable<S>>,
        snapshot_id: SnapshotId,
    ) -> Result<bool> {
        let seek_key = WalKey::seek_key_for_snapshot(snapshot_id);
        match wal_view.iter(&seek_key.key)?.next() {
            Some(item) => Ok(item?.0.as_ref().snapshot_id == snapshot_id),
            None => Ok(false),
        }
    }

    /// Verifies that no records newer than the recovery point exist in the database for a specific schema.
    ///
    /// The `recover_schema` function generates a `write_schema` containing operations to delete any
    /// records (e.g., in `SnapshotsTable` or `WalTable`) that are newer than the recovered snapshot.
    /// This function is intended to be called by a concrete application *after* it has committed
    /// that `write_schema` to the database.
    pub fn verify_no_newer_records<S: PendingKeyValueSchema, P: DatabaseTrait<PendingTableName>>(
        db: &Arc<P>,
        newest_height: u64,
        tracker: &PersistenceTracker,
    ) -> Result<()> {
        // check SnapshotsTable has no records newer than newest_height
        let snapshots_view = Arc::new(db.view::<SnapshotsTable<S>>()?);
        // Seek to the first snapshot with height > newest_height.
        let snapshot_seek_key = SnapshotKey::seek_key_for_height(newest_height + 1);
        if snapshots_view
            .iter(&snapshot_seek_key.key)?
            .next()
            .is_some()
        {
            Err(RecoveryError::DirtyRecoveryResult)?
        }

        // check WalTable has no records not older than (newest_snapshot_id, next_modification_id)
        let newest_snapshot_id = tracker.snapshot_id;
        let next_modification_id = tracker.next_modification_id;
        let wal_view = Arc::new(db.view::<WalTable<S>>()?);
        // Seek to the first WAL record with that >= (newest_snapshot_id, next_modification_id).
        let wal_seek_key =
            WalKey::seek_key_for_snap_mod_id(newest_snapshot_id, next_modification_id);
        if wal_view.iter(&wal_seek_key.key)?.next().is_some() {
            Err(RecoveryError::DirtyRecoveryResult)?
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use ethereum_types::H256;

    use super::super::{
        format::{SnapshotsTable, WalTable},
        test_util::{k, setup_db_with_snapshots_and_wals, TestSchema},
        DatabaseTrait, ModificationId, PendingTableName, PersistenceTracker, RecoverRecord,
        RecoveryError, SnapshotId, SnapshotKey, SnapshotKeyTreePart, SnapshotMapValue,
        SnapshotNodeDataType, SnapshotRecordType, SnapshotValue, StorageError, TableSchema,
        TreeWithTracker, ValueEntry, WalKey, WalKeySpecificPart, WalValue, WrappedInMemoryDb,
        WriteSchemaTrait,
    };
    use super::primitives::*;
    use crate::backends::serde::Decode;
    use std::{borrow::Cow, collections::HashMap, sync::Arc};

    // --------------------- recover_schema ---------------------

    #[test]
    fn test_recover_no_snapshot_found_on_empty_db() {
        // Arrange
        let db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());
        let write_schema = WrappedInMemoryDb::<PendingTableName>::write_schema();
        let parent_of_root = Some(H256::from_low_u64_be(99));
        let height_of_root = 100;

        // Act
        let result =
            recover_schema::<TestSchema, _>(&db, &write_schema, parent_of_root, height_of_root);

        // Assert
        assert!(matches!(
            result,
            Err(StorageError::RecoveryError(
                RecoveryError::NoValidSnapshotFound
            ))
        ));
    }

    #[test]
    fn test_recover_no_snapshot_found_on_edge_case() {
        // Arrange
        let db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());

        // An empty snapshot below the recovery height and nothing after it: the
        // pending db does not reach the recovery height.
        setup_db_with_snapshots_and_wals(&db, &[(99, 1, 0, 0)]);

        let write_schema = WrappedInMemoryDb::<PendingTableName>::write_schema();
        let parent_of_root = Some(H256::from_low_u64_be(99));
        let height_of_root = 100;

        // Act
        let result =
            recover_schema::<TestSchema, _>(&db, &write_schema, parent_of_root, height_of_root);

        // Assert
        assert!(matches!(
            result,
            Err(StorageError::RecoveryError(
                RecoveryError::NoValidSnapshotFound
            ))
        ));
    }

    #[test]
    fn test_recover_inconsistent_snapshot_state_cid() {
        // Arrange
        let db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());
        let write_schema_setup = WrappedInMemoryDb::<PendingTableName>::write_schema();

        let recovery_height = 10;
        let correct_parent_cid = H256::from_low_u64_be(1);
        let wrong_parent_cid = H256::from_low_u64_be(99);

        // Setup a snapshot with the wrong parent CID
        let meta_key = SnapshotKey::<TestSchema> {
            snapshot_root_height: recovery_height,
            record_type: SnapshotRecordType::Meta,
        };
        let meta_value = SnapshotValue::MetaValue {
            parent_of_root: Some(wrong_parent_cid),
            snapshot_id: SnapshotId(1),
            nodes_count: 0,
        };
        write_schema_setup.write::<SnapshotsTable<TestSchema>>((
            Cow::Owned(meta_key),
            Some(Cow::Owned(meta_value)),
        ));
        db.commit(write_schema_setup).unwrap();

        // Act
        let write_schema_recover = WrappedInMemoryDb::<PendingTableName>::write_schema();
        let result = recover_schema::<TestSchema, _>(
            &db,
            &write_schema_recover,
            Some(correct_parent_cid),
            recovery_height,
        );

        // Assert
        assert!(matches!(
            result,
            Err(StorageError::RecoveryError(
                RecoveryError::InconsistentSnapshotState
            ))
        ));
    }

    #[test]
    fn test_recover_inconsistent_snapshot_state_height() {
        // Arrange
        let db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());
        let write_schema_setup = WrappedInMemoryDb::<PendingTableName>::write_schema();

        let correct_recovery_height = 10;
        let wrong_recovery_height = 11;
        let parent_cid = H256::from_low_u64_be(1);

        // Setup a snapshot with the wrong parent CID
        let meta_key = SnapshotKey::<TestSchema> {
            snapshot_root_height: wrong_recovery_height,
            record_type: SnapshotRecordType::Meta,
        };
        let meta_value = SnapshotValue::MetaValue {
            parent_of_root: Some(parent_cid),
            snapshot_id: SnapshotId(1),
            nodes_count: 0,
        };
        write_schema_setup.write::<SnapshotsTable<TestSchema>>((
            Cow::Owned(meta_key),
            Some(Cow::Owned(meta_value)),
        ));
        db.commit(write_schema_setup).unwrap();

        // Act
        let write_schema_recover = WrappedInMemoryDb::<PendingTableName>::write_schema();
        let result = recover_schema::<TestSchema, _>(
            &db,
            &write_schema_recover,
            Some(parent_cid),
            correct_recovery_height,
        );

        // Assert
        assert!(matches!(
            result,
            Err(StorageError::RecoveryError(
                RecoveryError::InconsistentSnapshotState
            ))
        ));
    }

    #[test]
    fn test_recover_simple_snapshot_no_wal() {
        // Arrange
        let db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());
        let write_schema_setup = WrappedInMemoryDb::<PendingTableName>::write_schema();

        let height = 10;
        let parent_cid = Some(H256::from_low_u64_be(1));
        let snapshot_id = SnapshotId(5);
        let root_cid = H256::from_low_u64_be(100);

        // -- Create a snapshot representing a tree with one root node --
        // Meta record
        let meta_key = SnapshotKey::<TestSchema> {
            snapshot_root_height: height,
            record_type: SnapshotRecordType::Meta,
        };
        let meta_value = SnapshotValue::MetaValue {
            parent_of_root: parent_cid,
            snapshot_id,
            nodes_count: 1, // One node in the tree
        };
        write_schema_setup.write::<SnapshotsTable<TestSchema>>((
            Cow::Owned(meta_key),
            Some(Cow::Owned(meta_value)),
        ));

        // Node Meta record
        let node_meta_key = SnapshotKey::<TestSchema> {
            snapshot_root_height: height,
            record_type: SnapshotRecordType::Map(SnapshotKeyTreePart {
                node_height: height,
                node_commit_id: root_cid,
                node_data_type: SnapshotNodeDataType::NodeMeta,
            }),
        };
        let node_meta_value = SnapshotValue::MapValue(SnapshotMapValue::NodeMeta {
            parent_commit_id: parent_cid,
            modifications_count: 1, // One key-value pair in this node
        });
        write_schema_setup.write::<SnapshotsTable<TestSchema>>((
            Cow::Owned(node_meta_key),
            Some(Cow::Owned(node_meta_value)),
        ));

        // Node Map record
        let map_key_inner = k(b"key1");
        let map_value_inner = RecoverRecord {
            value: ValueEntry::Value(k(b"val1")),
            last_commit_id: None,
        };
        let node_map_key = SnapshotKey::<TestSchema> {
            snapshot_root_height: height,
            record_type: SnapshotRecordType::Map(SnapshotKeyTreePart {
                node_height: height,
                node_commit_id: root_cid,
                node_data_type: SnapshotNodeDataType::NodeMap {
                    key: map_key_inner.clone(),
                },
            }),
        };
        let node_map_value =
            SnapshotValue::MapValue(SnapshotMapValue::NodeMap(map_value_inner.clone()));
        write_schema_setup.write::<SnapshotsTable<TestSchema>>((
            Cow::Owned(node_map_key),
            Some(Cow::Owned(node_map_value)),
        ));

        db.commit(write_schema_setup).unwrap();

        // Act
        let write_schema_recover = WrappedInMemoryDb::<PendingTableName>::write_schema();
        let result =
            recover_schema::<TestSchema, _>(&db, &write_schema_recover, parent_cid, height)
                .unwrap();

        // Assert
        // Check tracker
        assert_eq!(result.tracker.snapshot_id, snapshot_id);
        assert_eq!(result.tracker.next_modification_id, ModificationId(0)); // No WALs replayed

        // Check tree structure
        assert_eq!(result.tree.get_parent_of_root(), parent_cid);
        assert_eq!(result.tree.get_height_of_root(), height);
        assert!(result.tree.contains_commit_id(&root_cid));
        let tree_snapshot = result.tree.export_snapshot();
        assert_eq!(tree_snapshot.height_of_root, height);
        assert_eq!(tree_snapshot.parent_of_root, parent_cid);
        assert_eq!(tree_snapshot.nodes.len(), 1);
        let tree_snapshot_node = &tree_snapshot.nodes[0];
        assert_eq!(tree_snapshot_node.node_height, height);
        assert_eq!(tree_snapshot_node.node_parent_commit_id, None);
        assert_eq!(tree_snapshot_node.node_commit_id, root_cid);
        let mut map_inner = HashMap::new();
        map_inner.insert(map_key_inner, map_value_inner);
        assert_eq!(tree_snapshot_node.modifications, map_inner);

        // Check that no cleanup was performed
        assert!(write_schema_recover.drain().is_empty());
    }

    #[test]
    fn test_recover_with_wal_replay_and_cleanup() {
        // Arrange: Setup a complex DB state
        let db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());
        let write_schema_setup = WrappedInMemoryDb::<PendingTableName>::write_schema();

        // -- Valid Snapshot (height=10, sid=5) --
        let height10 = 10;
        let parent10 = Some(H256::from_low_u64_be(10));
        let sid10 = SnapshotId(5);
        let cid1 = H256::from_low_u64_be(101); // Initial root in the snapshot

        // Snapshot Meta.
        write_schema_setup.write::<SnapshotsTable<TestSchema>>((
            Cow::Owned(SnapshotKey {
                snapshot_root_height: height10,
                record_type: SnapshotRecordType::Meta,
            }),
            Some(Cow::Owned(SnapshotValue::MetaValue {
                parent_of_root: parent10,
                snapshot_id: sid10,
                nodes_count: 1,
            })),
        ));

        // Snapshot Node (for cid1).
        let node_meta_key_1 = SnapshotKey::<TestSchema> {
            snapshot_root_height: height10,
            record_type: SnapshotRecordType::Map(SnapshotKeyTreePart {
                node_height: height10,
                node_commit_id: cid1,
                node_data_type: SnapshotNodeDataType::NodeMeta,
            }),
        };
        let node_meta_value_1 = SnapshotValue::MapValue(SnapshotMapValue::NodeMeta {
            // This node is the root of the snapshot tree, so it has no parent *within the tree*.
            // The tree's overall parent is `parent10`.
            parent_commit_id: None,
            // At the time of the snapshot, this node has no modifications recorded *in the snapshot itself*.
            modifications_count: 0,
        });
        write_schema_setup.write::<SnapshotsTable<TestSchema>>((
            Cow::Owned(node_meta_key_1),
            Some(Cow::Owned(node_meta_value_1)),
        ));

        // -- WALs for sid=5 --
        let cid2 = H256::from_low_u64_be(102); // New node to be added via WAL replay
                                               // Mod 0: add_non_root_node(cid2, parent=cid1)
        write_schema_setup.write::<WalTable<TestSchema>>((
            Cow::Owned(WalKey {
                snapshot_id: sid10,
                modification_id: ModificationId(0),
                operation_specific_parts: WalKeySpecificPart::AddNodeMeta,
            }),
            Some(Cow::Owned(WalValue::MetaValue {
                commit_id: cid2,
                maybe_parent_cid: Some(cid1),
                map_value_count: 0,
            })),
        ));

        let cid3 = H256::from_low_u64_be(103); // New node to be added via WAL replay
                                               // Mod 1: add_non_root_node(cid3, parent=cid1)
        write_schema_setup.write::<WalTable<TestSchema>>((
            Cow::Owned(WalKey {
                snapshot_id: sid10,
                modification_id: ModificationId(1),
                operation_specific_parts: WalKeySpecificPart::AddNodeMeta,
            }),
            Some(Cow::Owned(WalValue::MetaValue {
                commit_id: cid3,
                maybe_parent_cid: Some(cid1),
                map_value_count: 0,
            })),
        ));

        // Mod 2: make_pivot(cid2)
        write_schema_setup.write::<WalTable<TestSchema>>((
            Cow::Owned(WalKey {
                snapshot_id: sid10,
                modification_id: ModificationId(2),
                operation_specific_parts: WalKeySpecificPart::MakePivotMeta,
            }),
            Some(Cow::Owned(WalValue::MetaValue {
                commit_id: cid2,
                maybe_parent_cid: None,
                map_value_count: 0,
            })),
        ));

        // -- Invalid Snapshot (height=20, sid=6) that should be cleaned up --
        let height20 = 20;
        let sid20 = SnapshotId(6);
        let cid_invalid = H256::from_low_u64_be(201);

        // Invalid Snapshot Meta: Correct structure.
        write_schema_setup.write::<SnapshotsTable<TestSchema>>((
            Cow::Owned(SnapshotKey {
                snapshot_root_height: height20,
                record_type: SnapshotRecordType::Meta,
            }),
            Some(Cow::Owned(SnapshotValue::MetaValue {
                parent_of_root: Some(H256::zero()),
                snapshot_id: sid20,
                nodes_count: 1,
            })),
        ));

        // Invalid Snapshot Node: CORRECTED according to the new structure.
        let invalid_node_meta_key = SnapshotKey::<TestSchema> {
            snapshot_root_height: height20,
            record_type: SnapshotRecordType::Map(SnapshotKeyTreePart {
                node_height: height20,
                node_commit_id: cid_invalid,
                node_data_type: SnapshotNodeDataType::NodeMeta,
            }),
        };
        let invalid_node_meta_value = SnapshotValue::MapValue(SnapshotMapValue::NodeMeta {
            parent_commit_id: None,
            modifications_count: 0,
        });
        write_schema_setup.write::<SnapshotsTable<TestSchema>>((
            Cow::Owned(invalid_node_meta_key),
            Some(Cow::Owned(invalid_node_meta_value)),
        ));

        // WAL for sid=6: Correct structure.
        write_schema_setup.write::<WalTable<TestSchema>>((
            Cow::Owned(WalKey {
                snapshot_id: sid20,
                modification_id: ModificationId(0),
                operation_specific_parts: WalKeySpecificPart::DiscardMeta,
            }),
            Some(Cow::Owned(WalValue::MetaValue {
                commit_id: cid_invalid,
                maybe_parent_cid: None,
                map_value_count: 0,
            })),
        ));

        db.commit(write_schema_setup).unwrap();

        // Act
        let write_schema_recover = WrappedInMemoryDb::<PendingTableName>::write_schema();
        let result =
            recover_schema::<TestSchema, _>(&db, &write_schema_recover, parent10, height10)
                .unwrap();

        // Assert
        // 1. Check recovered tracker state
        assert_eq!(result.tracker.snapshot_id, sid10);
        assert_eq!(result.tracker.next_modification_id, ModificationId(3)); // Replayed mod 0, 1, and 2

        // 2. Check recovered tree state
        // The tree is restored from the snapshot (containing cid1), then WALs are replayed.
        // WAL 0 adds cid2. WAL 1 adds cid3. WAL 2 removes cid3. So, the final tree has two nodes.
        let tree_snapshot = result.tree.export_snapshot();
        assert_eq!(tree_snapshot.nodes.len(), 2);
        assert!(result.tree.contains_commit_id(&cid1));
        assert!(!result.tree.contains_commit_id(&cid3));
        let tree_node_2 = &tree_snapshot.nodes[1];
        assert_eq!(tree_node_2.node_commit_id, cid2);
        assert_eq!(tree_node_2.node_parent_commit_id, Some(cid1));

        // 3. Check cleanup operations
        let ops = write_schema_recover.drain();
        let deleted_snaps = ops
            .iter()
            .filter(|(table, _, val)| *table == SnapshotsTable::<TestSchema>::NAME && val.is_none())
            .count();
        let deleted_wals = ops
            .iter()
            .filter(|(table, _, val)| *table == WalTable::<TestSchema>::NAME && val.is_none())
            .count();

        // The invalid snapshot at height 20 has one Meta record and one Node record. Both should be deleted.
        assert_eq!(
            deleted_snaps, 2,
            "Should delete 2 invalid snapshot records (1 meta, 1 node)"
        );
        // The invalid snapshot had one associated WAL record. It should be deleted.
        assert_eq!(deleted_wals, 1, "Should delete 1 invalid WAL record");
    }

    #[test]
    fn test_recover_cleans_up_the_wal_of_every_newer_snapshot() {
        // A valid (empty) snapshot at the recovery height and two newer, invalid
        // ones. Format: (height, snapshot_id, num_wal_records, num_node_records).
        let db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());
        setup_db_with_snapshots_and_wals(&db, &[(10, 5, 0, 0), (20, 6, 2, 1), (30, 7, 3, 1)]);

        let write_schema = WrappedInMemoryDb::<PendingTableName>::write_schema();
        let result = recover_schema::<TestSchema, _>(&db, &write_schema, None, 10).unwrap();
        assert_eq!(result.tracker.snapshot_id, SnapshotId(5));

        // Each invalid snapshot's WAL records are deleted, and only those.
        let mut deleted_wals = HashMap::new();
        for (table, key, value) in write_schema.drain() {
            if table == WalTable::<TestSchema>::NAME {
                assert!(value.is_none());
                let key = WalKey::<TestSchema>::decode_owned(key).unwrap();
                *deleted_wals.entry(key.snapshot_id).or_insert(0) += 1;
            }
        }
        assert_eq!(
            deleted_wals,
            HashMap::from([(SnapshotId(6), 2), (SnapshotId(7), 3)])
        );
    }

    // --------------------- deferred snapshots ---------------------

    fn cid(i: u64) -> H256 {
        H256::from_low_u64_be(i + 1)
    }

    /// Logs a chain of commits 0..`commits` on an empty schema, then `change_root`s to
    /// heights 1..=`change_roots`, deferring the snapshots due and writing them iff
    /// `write_snapshots`, then commits `commits_after` more. Each operation is committed
    /// on its own. Returns the in-memory state.
    fn log_with_deferred_snapshots(
        db: &Arc<WrappedInMemoryDb<PendingTableName>>,
        commits: u64,
        change_roots: u64,
        commits_after: u64,
        write_snapshots: bool,
    ) -> TreeWithTracker<TestSchema> {
        type Db = WrappedInMemoryDb<PendingTableName>;
        let write_schema = Db::write_schema();
        let mut state =
            super::super::primitives_initialize_empty_schema::<TestSchema>(&write_schema, None, 0);
        db.commit(write_schema).unwrap();
        let modifications = |i: u64| {
            HashMap::from([(
                k(&i.to_be_bytes()),
                RecoverRecord {
                    value: ValueEntry::Value(k(b"v")),
                    last_commit_id: None,
                },
            )])
        };
        let add = |state: &mut TreeWithTracker<TestSchema>, i: u64| {
            let write_schema = Db::write_schema();
            if i == 0 {
                state
                    .add_root::<Db>(cid(0), modifications(0), &write_schema)
                    .unwrap();
            } else {
                state
                    .add_non_root_node::<Db>(cid(i), cid(i - 1), modifications(i), &write_schema)
                    .unwrap();
            }
            db.commit(write_schema).unwrap();
        };
        for i in 0..commits {
            add(&mut state, i);
        }
        for h in 1..=change_roots {
            let write_schema = Db::write_schema();
            let (_, snapshot) = state
                .change_root_deferring_snapshot::<Db>(cid(h), &write_schema)
                .unwrap()
                .unwrap();
            db.commit(write_schema).unwrap();
            if let Some(snapshot) = snapshot.filter(|_| write_snapshots) {
                assert_eq!(snapshot.height(), h);
                let write_schema = Db::write_schema();
                snapshot.write(&write_schema);
                db.commit(write_schema).unwrap();
            }
        }
        for i in commits..commits + commits_after {
            add(&mut state, i);
        }
        state
    }

    /// Recovers at the root of `expected` and checks it is recovered exactly, with
    /// nothing to delete, except for the tracker if `expected_tracker` is given.
    fn assert_recovers_to(
        db: &Arc<WrappedInMemoryDb<PendingTableName>>,
        expected: &TreeWithTracker<TestSchema>,
        expected_tracker: Option<PersistenceTracker>,
    ) {
        let expected_tracker = expected_tracker.as_ref().unwrap_or(&expected.tracker);
        let write_schema = WrappedInMemoryDb::<PendingTableName>::write_schema();
        let recovered = recover_schema::<TestSchema, _>(
            db,
            &write_schema,
            expected.tree.get_parent_of_root(),
            expected.tree.get_height_of_root(),
        )
        .unwrap();
        assert_eq!(&recovered.tracker, expected_tracker);
        assert_eq!(
            recovered.tree.export_snapshot(),
            expected.tree.export_snapshot()
        );
        assert!(write_schema.drain().is_empty());
        verify_no_newer_records::<TestSchema, _>(
            db,
            expected.tree.get_height_of_root(),
            &recovered.tracker,
        )
        .unwrap();
    }

    #[test]
    fn test_recover_replays_the_wal_of_a_snapshot_never_written() {
        let n = crate::middlewares::CHANGE_ROOTS_PER_SNAPSHOT;
        // The last change_root moves on to a new snapshot ID, then more commits are
        // logged under it; or a few more change_roots are as well.
        for (change_roots, commits_after) in [(n, 0), (n, 2), (n + 2, 2), (2 * n + 1, 1)] {
            let db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());
            let state = log_with_deferred_snapshots(&db, 3 * n, change_roots, commits_after, false);
            assert_eq!(state.tracker.snapshot_id, SnapshotId(change_roots / n));
            // Nothing logged under the new snapshot ID yet: recovery cannot tell it was
            // moved on to, and stays at the previous one, which the next change_root
            // moves on from.
            let expected_tracker = (commits_after == 0).then(|| PersistenceTracker {
                snapshot_id: SnapshotId(0),
                next_modification_id: ModificationId(3 * n + n),
                change_roots_since_snapshot: n,
            });
            assert_recovers_to(&db, &state, expected_tracker);
        }
    }

    #[test]
    fn test_recover_from_a_deferred_snapshot_written() {
        let n = crate::middlewares::CHANGE_ROOTS_PER_SNAPSHOT;
        for (change_roots, commits_after) in [(n, 0), (n, 2), (n + 2, 2), (2 * n + 1, 1)] {
            let db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());
            let state = log_with_deferred_snapshots(&db, 3 * n, change_roots, commits_after, true);
            assert_recovers_to(&db, &state, None);
        }
    }

    #[test]
    fn test_recover_rolls_back_a_change_root_and_the_wal_after_it() {
        // The pending db kept the change_root that moved on to a new snapshot ID, never
        // written, and commits logged after it, but the historical db lost it.
        let n = crate::middlewares::CHANGE_ROOTS_PER_SNAPSHOT;
        let db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());
        log_with_deferred_snapshots(&db, 3 * n, n, 2, false);
        let expected = log_with_deferred_snapshots(
            &Arc::new(WrappedInMemoryDb::<PendingTableName>::empty()),
            3 * n,
            n - 1,
            0,
            false,
        );

        let write_schema = WrappedInMemoryDb::<PendingTableName>::write_schema();
        let recovered =
            recover_schema::<TestSchema, _>(&db, &write_schema, Some(cid(n - 2)), n - 1).unwrap();
        assert_eq!(recovered.tracker, expected.tracker);
        assert_eq!(
            recovered.tree.export_snapshot(),
            expected.tree.export_snapshot()
        );

        // The change_root and the two commits after it are deleted.
        let mut deleted_wals = HashMap::new();
        for (table, key, value) in write_schema.drain() {
            assert_eq!(table, WalTable::<TestSchema>::NAME);
            assert!(value.is_none());
            let key = WalKey::<TestSchema>::decode_owned(key).unwrap();
            deleted_wals
                .entry(key.snapshot_id)
                .or_insert_with(Vec::new)
                .push(key.modification_id);
        }
        deleted_wals.values_mut().for_each(|ids| ids.dedup());
        assert_eq!(
            deleted_wals,
            HashMap::from([
                (SnapshotId(0), vec![recovered.tracker.next_modification_id]),
                (SnapshotId(1), vec![ModificationId(0), ModificationId(1)]),
            ])
        );
    }

    // --------------------- verify_no_newer_records ---------------------

    #[test]
    fn test_verify_no_newer_records_clean_ok() {
        // Arrange
        let db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());
        let write_schema = WrappedInMemoryDb::<PendingTableName>::write_schema();

        // Prepare a latest snapshot at height=10, sid=5.
        // Only an exact-height snapshot record exists; verify checks strictly greater height.
        let height = 10;
        let sid = SnapshotId(5);

        write_schema.write::<SnapshotsTable<TestSchema>>((
            Cow::Owned(SnapshotKey {
                snapshot_root_height: height,
                record_type: SnapshotRecordType::Meta,
            }),
            Some(Cow::Owned(SnapshotValue::MetaValue {
                parent_of_root: Some(H256::zero()),
                snapshot_id: sid,
                nodes_count: 0,
            })),
        ));
        db.commit(write_schema).unwrap();

        // Act
        let tracker = PersistenceTracker {
            snapshot_id: sid,
            next_modification_id: ModificationId(3),
            change_roots_since_snapshot: 0,
        };
        let res = verify_no_newer_records::<TestSchema, _>(&db, height, &tracker);

        // Assert
        assert!(res.is_ok());
    }

    #[test]
    fn test_verify_no_newer_records_detects_newer_snapshot() {
        // Arrange
        let db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());
        let ws = WrappedInMemoryDb::<PendingTableName>::write_schema();

        // Latest point: height=10, sid=5, next_mod_id=3
        let newest_height = 10;
        let newest_sid = SnapshotId(5);
        let next_mod_id = ModificationId(3);

        // Insert a snapshot at height=11 (> newest_height), which should trigger DirtyRecoveryResult.
        ws.write::<SnapshotsTable<TestSchema>>((
            Cow::Owned(SnapshotKey {
                snapshot_root_height: newest_height + 1,
                record_type: SnapshotRecordType::Meta,
            }),
            Some(Cow::Owned(SnapshotValue::MetaValue {
                parent_of_root: Some(H256::zero()),
                snapshot_id: SnapshotId(newest_sid.0 + 1),
                nodes_count: 0,
            })),
        ));
        db.commit(ws).unwrap();

        // Act
        let tracker = PersistenceTracker {
            snapshot_id: newest_sid,
            next_modification_id: next_mod_id,
            change_roots_since_snapshot: 0,
        };
        let res = verify_no_newer_records::<TestSchema, _>(&db, newest_height, &tracker);

        // Assert
        assert!(matches!(
            res,
            Err(StorageError::RecoveryError(
                RecoveryError::DirtyRecoveryResult
            ))
        ));
    }

    #[test]
    fn test_verify_no_newer_records_detects_newer_wal_same_snapshot_higher_or_equal_mod() {
        // Arrange
        let db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());
        let ws = WrappedInMemoryDb::<PendingTableName>::write_schema();

        let newest_height = 10;
        let newest_sid = SnapshotId(5);
        let next_mod_id = ModificationId(3);

        // Insert a WAL with the exact boundary key (sid=5, mod=3).
        // Since verify checks for >= boundary, this should be considered dirty.
        ws.write::<WalTable<TestSchema>>((
            Cow::Owned(WalKey {
                snapshot_id: newest_sid,
                modification_id: next_mod_id,
                operation_specific_parts: WalKeySpecificPart::DiscardMeta,
            }),
            Some(Cow::Owned(WalValue::MetaValue {
                commit_id: H256::zero(),
                maybe_parent_cid: None,
                map_value_count: 0,
            })),
        ));
        db.commit(ws).unwrap();

        // Act
        let tracker = PersistenceTracker {
            snapshot_id: newest_sid,
            next_modification_id: next_mod_id,
            change_roots_since_snapshot: 0,
        };
        let res = verify_no_newer_records::<TestSchema, _>(&db, newest_height, &tracker);

        // Assert
        assert!(matches!(
            res,
            Err(StorageError::RecoveryError(
                RecoveryError::DirtyRecoveryResult
            ))
        ));
    }

    #[test]
    fn test_verify_no_newer_records_detects_newer_wal_higher_snapshot_any_mod() {
        // Arrange
        let db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());
        let ws = WrappedInMemoryDb::<PendingTableName>::write_schema();

        let newest_height = 10;
        let newest_sid = SnapshotId(5);
        let next_mod_id = ModificationId(3);

        // Insert a WAL with higher snapshot_id (6). Any modification_id should be considered newer.
        ws.write::<WalTable<TestSchema>>((
            Cow::Owned(WalKey {
                snapshot_id: SnapshotId(newest_sid.0 + 1),
                modification_id: ModificationId(0),
                operation_specific_parts: WalKeySpecificPart::DiscardMeta,
            }),
            Some(Cow::Owned(WalValue::MetaValue {
                commit_id: H256::zero(),
                maybe_parent_cid: None,
                map_value_count: 0,
            })),
        ));
        db.commit(ws).unwrap();

        // Act
        let tracker = PersistenceTracker {
            snapshot_id: newest_sid,
            next_modification_id: next_mod_id,
            change_roots_since_snapshot: 0,
        };
        let res = verify_no_newer_records::<TestSchema, _>(&db, newest_height, &tracker);

        // Assert
        assert!(matches!(
            res,
            Err(StorageError::RecoveryError(
                RecoveryError::DirtyRecoveryResult
            ))
        ));
    }

    #[test]
    fn test_verify_no_newer_records_older_wal_is_ok() {
        // Arrange
        let db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());
        let ws = WrappedInMemoryDb::<PendingTableName>::write_schema();

        let newest_height = 10;
        let newest_sid = SnapshotId(5);
        let next_mod_id = ModificationId(3);

        // Insert an older WAL: same snapshot_id but modification_id smaller than boundary (2 < 3).
        ws.write::<WalTable<TestSchema>>((
            Cow::Owned(WalKey {
                snapshot_id: newest_sid,
                modification_id: ModificationId(next_mod_id.0 - 1),
                operation_specific_parts: WalKeySpecificPart::DiscardMeta,
            }),
            Some(Cow::Owned(WalValue::MetaValue {
                commit_id: H256::zero(),
                maybe_parent_cid: None,
                map_value_count: 0,
            })),
        ));
        db.commit(ws).unwrap();

        // Act
        let tracker = PersistenceTracker {
            snapshot_id: newest_sid,
            next_modification_id: next_mod_id,
            change_roots_since_snapshot: 0,
        };
        let res = verify_no_newer_records::<TestSchema, _>(&db, newest_height, &tracker);

        // Assert
        assert!(res.is_ok());
    }
}
