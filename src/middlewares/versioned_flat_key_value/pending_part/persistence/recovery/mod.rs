use thiserror::Error;

/// Errors that can occur during the state recovery process from the database.
#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryError {
    #[error("No valid snapshot found at or after the specified recovery height.")]
    NoValidSnapshotFound,
    #[error("Inconsistent snapshot state: Found a snapshot at or after the recovery height, but it's not a valid recovery point.")]
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

        // Seek to the first snapshot with height >= height_of_root.
        let seek_key = SnapshotKey::seek_key_for_height(height_of_root);
        let mut iter = snapshots_view.iter(&seek_key.key)?;
        match iter.next() {
            None => Err(RecoveryError::NoValidSnapshotFound)?,
            Some(item) => {
                let (snap_key_cow, snap_value_cow) = item?;
                let snap_height = snap_key_cow.as_ref().snapshot_root_height;
                let (snap_parent_cid, snapshot_id, nodes_count) = match snap_value_cow.as_ref() {
                    SnapshotValue::MetaValue {
                        parent_of_root,
                        snapshot_id,
                        nodes_count,
                    } => (*parent_of_root, *snapshot_id, *nodes_count),
                    SnapshotValue::MapValue(_) => Err(RecoveryError::FirstRecordIsNotMeta)?,
                };

                // Check if the found snapshot is a perfect match.
                if snap_height == height_of_root && snap_parent_cid == parent_of_root {
                    // --- Path A: Found a valid recovery point. Restore and replay WAL. ---

                    // Rebuild the tree from the snapshot's base info.
                    let (mut tree, maybe_first_invalid_snapshot_item) =
                        read_tree_snapshot::read_tree_from_snapshot_iter(
                            snap_height,
                            snap_parent_cid,
                            snapshot_id,
                            nodes_count,
                            &mut iter,
                        )?;

                    // Replay WAL records in a loop until the replayer returns false.
                    let mut mod_id = 0;
                    while wal_player::replay_one_modification(
                        &*wal_view,
                        &mut tree,
                        snapshot_id,
                        ModificationId(mod_id),
                    )? {
                        mod_id += 1;
                    }

                    // Clean up any newer, now-invalid snapshots and their WALs.
                    if let Some(first_invalid_snapshot_item) = maybe_first_invalid_snapshot_item {
                        // The `last_invalid_height` is used to determine whether the record is meta in subsequent traversals.
                        let mut last_invalid_height =
                            first_invalid_snapshot_item.0.as_ref().snapshot_root_height;

                        // The `first_invalid_snapshot_item` must be a meta.
                        let SnapshotValue::MetaValue {
                            snapshot_id: invalid_snapshot_id,
                            ..
                        } = first_invalid_snapshot_item.1.as_ref()
                        else {
                            Err(RecoveryError::FirstRecordIsNotMeta)?
                        };
                        // For the meta snapshot record, delete the corresponding WAL records.
                        delete_wal_by_snapshot_id::<S, P>(
                            &wal_view,
                            write_schema,
                            *invalid_snapshot_id,
                        )?;
                        // Delete this snapshot record.
                        write_schema
                            .write::<SnapshotsTable<S>>((first_invalid_snapshot_item.0, None));

                        // Continue to iter snapshot records.
                        for invalid_snapshot_item_res in iter {
                            let invalid_snapshot_item = invalid_snapshot_item_res?;

                            // If the record is a meta, delete the corresponding WAL records.
                            let invalid_height =
                                invalid_snapshot_item.0.as_ref().snapshot_root_height;
                            if invalid_height != last_invalid_height {
                                last_invalid_height = invalid_height;
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
                                    snapshot_id,
                                )?;
                            }

                            // Delete this record.
                            write_schema
                                .write::<SnapshotsTable<S>>((invalid_snapshot_item.0, None));
                        }
                    }

                    let tracker = PersistenceTracker {
                        snapshot_id,
                        next_modification_id: ModificationId(mod_id),
                    };

                    Ok(TreeWithTracker { tree, tracker })
                } else {
                    Err(RecoveryError::InconsistentSnapshotState)?
                }
            }
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
        ValueEntry, WalKey, WalKeySpecificPart, WalValue, WrappedInMemoryDb, WriteSchemaTrait,
    };
    use super::primitives::*;
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

        // No need to ensure the data is valid.
        setup_db_with_snapshots_and_wals(&db, &[(99, 1, 2, 1)]);

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
        };
        let res = verify_no_newer_records::<TestSchema, _>(&db, newest_height, &tracker);

        // Assert
        assert!(res.is_ok());
    }
}
