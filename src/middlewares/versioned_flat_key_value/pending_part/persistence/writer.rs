//! This module provides functions for writing `Tree` modifications to the persistence layer.
//!
//! It translates high-level tree operations into low-level key-value pairs suitable for
//! the Write-Ahead Log (WAL), and adds them to a given `WriteSchema`. This process is
//! the inverse of the logic found in the `recovery` module.

use std::borrow::Cow;

use super::{
    format::{
        SnapshotKey, SnapshotValue, SnapshotsTable, WalKey, WalKeySpecificPart, WalTable, WalValue,
    },
    PendingKeyValueSchema, PersistenceTracker, RecoverMap, WriteSchemaTrait,
};

/// Logs the addition of a new root node to the WAL.
pub fn log_add_root<S: PendingKeyValueSchema>(
    write_schema: &impl WriteSchemaTrait<super::PendingTableName>,
    tracker: &mut PersistenceTracker,
    commit_id: S::CommitId,
    modifications: &RecoverMap<S>,
) {
    log_add_node_internal(
        write_schema,
        tracker,
        commit_id,
        None, // A root node has no parent.
        modifications,
    );
}

/// Logs the addition of a non-root node to the WAL.
pub fn log_add_non_root_node<S: PendingKeyValueSchema>(
    write_schema: &impl WriteSchemaTrait<super::PendingTableName>,
    tracker: &mut PersistenceTracker,
    commit_id: S::CommitId,
    parent_commit_id: S::CommitId,
    modifications: &RecoverMap<S>,
) {
    log_add_node_internal(
        write_schema,
        tracker,
        commit_id,
        Some(parent_commit_id), // A non-root node must have a parent.
        modifications,
    );
}

/// Logs a `change_root` operation, which is a special case.
///
/// This function performs three key actions:
/// 1. Writes the `change_root` modification to the WAL under the *current* snapshot ID.
/// 2. Advances the `PersistenceTracker` to a new snapshot.
/// 3. Writes a new entry to the `SnapshotsTable` to persist this new snapshot.
pub fn log_change_root<S: PendingKeyValueSchema>(
    write_schema: &impl WriteSchemaTrait<super::PendingTableName>,
    tracker: &mut PersistenceTracker,
    new_root_cid: S::CommitId,
    new_height_of_root: u64,
    new_parent_of_root: S::CommitId,
) {
    // Step 1: Log the `change_root` action to the WAL using the generalized helper.
    log_single_record_operation(
        write_schema,
        tracker,
        new_root_cid,
        WalKeySpecificPart::<S>::ChangeRootMeta,
    );

    // Step 2: Advance the tracker to a new snapshot state.
    tracker.advance_to_next_snapshot();

    // Step 3: Write the new snapshot record to the database.
    let snapshot_key = SnapshotKey(new_height_of_root);
    let snapshot_value = SnapshotValue {
        parent_of_root: Some(new_parent_of_root),
        snapshot_id: tracker.snapshot_id, // Use the new snapshot_id
    };
    let op = (Cow::Owned(snapshot_key), Some(Cow::Owned(snapshot_value)));
    write_schema.write::<SnapshotsTable<S>>(op);
}

/// Logs a `make_pivot` operation to the WAL.
pub fn log_make_pivot<S: PendingKeyValueSchema>(
    write_schema: &impl WriteSchemaTrait<super::PendingTableName>,
    tracker: &mut PersistenceTracker,
    commit_id: S::CommitId,
) {
    log_single_record_operation(
        write_schema,
        tracker,
        commit_id,
        WalKeySpecificPart::<S>::MakePivotMeta,
    );
}

/// Logs a `discard` operation to the WAL.
pub fn log_discard<S: PendingKeyValueSchema>(
    write_schema: &impl WriteSchemaTrait<super::PendingTableName>,
    tracker: &mut PersistenceTracker,
    commit_id: S::CommitId,
) {
    log_single_record_operation(
        write_schema,
        tracker,
        commit_id,
        WalKeySpecificPart::<S>::DiscardMeta,
    );
}

// --- Private Helper Functions ---

/// Internal implementation for logging an `add_node` operation.
///
/// This consists of a "meta" record and a series of "map" records for modifications.
fn log_add_node_internal<S: PendingKeyValueSchema>(
    write_schema: &impl WriteSchemaTrait<super::PendingTableName>,
    tracker: &mut PersistenceTracker,
    commit_id: S::CommitId,
    parent_commit_id: Option<S::CommitId>,
    modifications: &RecoverMap<S>,
) {
    let snapshot_id = tracker.snapshot_id;
    let modification_id = tracker.next_modification_id;
    let map_value_count = modifications.len() as u64;

    // 1. Write the meta record.
    let meta_key = WalKey {
        snapshot_id,
        modification_id,
        operation_specific_parts: WalKeySpecificPart::AddNodeMeta,
    };
    let meta_value = WalValue::MetaValue {
        commit_id,
        maybe_parent_cid: parent_commit_id,
        map_value_count,
    };
    let meta_op = (Cow::Owned(meta_key), Some(Cow::Owned(meta_value)));
    write_schema.write::<WalTable<S>>(meta_op);

    // 2. Write each key-value modification as a separate map record.
    for (key, record) in modifications {
        let map_key = WalKey {
            snapshot_id,
            modification_id,
            operation_specific_parts: WalKeySpecificPart::AddNodeMapKey(key.clone()),
        };
        let map_value = WalValue::MapValue(record.clone());
        let map_op = (Cow::Owned(map_key), Some(Cow::Owned(map_value)));
        write_schema.write::<WalTable<S>>(map_op);
    }

    // 3. Advance the tracker for the next modification.
    tracker.advance_to_next_modification();
}

/// A generalized helper to log simple, single-record "meta" operations.
fn log_single_record_operation<S: PendingKeyValueSchema>(
    write_schema: &impl WriteSchemaTrait<super::PendingTableName>,
    tracker: &mut PersistenceTracker,
    commit_id: S::CommitId,
    specific_part: WalKeySpecificPart<S>,
) {
    let key = WalKey {
        snapshot_id: tracker.snapshot_id,
        modification_id: tracker.next_modification_id,
        operation_specific_parts: specific_part,
    };

    let value = WalValue::MetaValue {
        commit_id,
        maybe_parent_cid: None,
        map_value_count: 0, // Meta-only operations have no map values.
    };

    let op = (Cow::Owned(key), Some(Cow::Owned(value)));
    write_schema.write::<WalTable<S>>(op);

    // Advance the tracker for the next modification.
    tracker.advance_to_next_modification();
}

#[cfg(test)]
mod tests {
    use super::super::{
        format::{
            ModificationId, SnapshotId, SnapshotKey, SnapshotValue, SnapshotsTable, WalKey,
            WalKeySpecificPart, WalTable, WalValue,
        },
        test_util::{k, TestSchema},
        DatabaseTrait, Decode, PendingTableName, PersistenceTracker, RecoverMap, RecoverRecord,
        TableSchema, ValueEntry, WrappedInMemoryDb,
    };
    use super::*;
    use ethereum_types::H256;

    #[test]
    fn test_log_add_root() {
        // Arrange
        let db = WrappedInMemoryDb::<PendingTableName>::empty();
        let write_schema = WrappedInMemoryDb::<PendingTableName>::write_schema();
        let mut tracker = PersistenceTracker {
            snapshot_id: SnapshotId(0),
            next_modification_id: ModificationId(0),
        };

        let commit_id = H256::from_low_u64_be(100);
        let mut modifications = RecoverMap::<TestSchema>::new();
        modifications.insert(
            k(b"key1"),
            RecoverRecord {
                value: ValueEntry::Value(k(b"val1")),
                last_commit_id: None,
            },
        );

        // Act
        log_add_root::<TestSchema>(&write_schema, &mut tracker, commit_id, &modifications);

        // Assert
        // 1. Check tracker state
        assert_eq!(tracker.snapshot_id.0, 0, "Snapshot ID should not change");
        assert_eq!(
            tracker.next_modification_id.0, 1,
            "Next modification ID should be incremented"
        );

        // 2. Check written data
        let ops = write_schema.drain();
        assert_eq!(ops.len(), 2, "Should write one meta and one map record");

        let meta_op = ops
            .iter()
            .find(|op| {
                let key = WalKey::<TestSchema>::decode_owned(op.1.clone()).unwrap();
                matches!(
                    key.operation_specific_parts,
                    WalKeySpecificPart::AddNodeMeta
                )
            })
            .expect("Meta record not found");

        // Assert Meta record
        let meta_key = WalKey::<TestSchema>::decode_owned(meta_op.1.clone()).unwrap();
        let meta_value = WalValue::<TestSchema>::decode_owned(meta_op.2.clone().unwrap()).unwrap();

        assert_eq!(meta_key.snapshot_id.0, 0);
        assert_eq!(meta_key.modification_id.0, 0);
        if let WalValue::MetaValue {
            commit_id: cid,
            maybe_parent_cid,
            map_value_count,
        } = meta_value
        {
            assert_eq!(cid, commit_id);
            assert_eq!(maybe_parent_cid, None, "Root node should have no parent");
            assert_eq!(map_value_count, 1);
        } else {
            panic!("Expected MetaValue for the meta record");
        }
    }

    #[test]
    fn test_log_make_pivot() {
        // Arrange
        let db = WrappedInMemoryDb::<PendingTableName>::empty();
        let write_schema = WrappedInMemoryDb::<PendingTableName>::write_schema();
        let mut tracker = PersistenceTracker {
            snapshot_id: SnapshotId(3),
            next_modification_id: ModificationId(8),
        };
        let commit_id = H256::from_low_u64_be(300);

        // Act
        log_make_pivot::<TestSchema>(&write_schema, &mut tracker, commit_id);

        // Assert
        // 1. Check tracker state
        assert_eq!(tracker.snapshot_id.0, 3, "Snapshot ID should not change");
        assert_eq!(
            tracker.next_modification_id.0, 9,
            "Next modification ID should be incremented"
        );

        // 2. Check written data
        let ops = write_schema.drain();
        assert_eq!(ops.len(), 1, "Should write a single WAL record");
        let op = &ops[0];
        assert_eq!(op.0, WalTable::<TestSchema>::NAME);

        let key = WalKey::<TestSchema>::decode_owned(op.1.clone()).unwrap();
        let value = WalValue::<TestSchema>::decode_owned(op.2.clone().unwrap()).unwrap();

        // Assert Key
        assert_eq!(key.snapshot_id.0, 3);
        assert_eq!(key.modification_id.0, 8);
        assert_eq!(
            key.operation_specific_parts,
            WalKeySpecificPart::MakePivotMeta
        );

        // Assert Value
        if let WalValue::MetaValue {
            commit_id: cid,
            maybe_parent_cid,
            map_value_count,
        } = value
        {
            assert_eq!(cid, commit_id);
            assert_eq!(maybe_parent_cid, None);
            assert_eq!(map_value_count, 0);
        } else {
            panic!("Expected MetaValue");
        }
    }

    #[test]
    fn test_log_discard() {
        // Arrange
        let db = WrappedInMemoryDb::<PendingTableName>::empty();
        let write_schema = WrappedInMemoryDb::<PendingTableName>::write_schema();
        let mut tracker = PersistenceTracker {
            snapshot_id: SnapshotId(4),
            next_modification_id: ModificationId(0),
        };
        let commit_id = H256::from_low_u64_be(400);

        // Act
        log_discard::<TestSchema>(&write_schema, &mut tracker, commit_id);

        // Assert
        // 1. Check tracker state
        assert_eq!(tracker.snapshot_id.0, 4, "Snapshot ID should not change");
        assert_eq!(
            tracker.next_modification_id.0, 1,
            "Next modification ID should be incremented"
        );

        // 2. Check written data
        let ops = write_schema.drain();
        assert_eq!(ops.len(), 1, "Should write a single WAL record");
        let op = &ops[0];
        assert_eq!(op.0, WalTable::<TestSchema>::NAME);

        let key = WalKey::<TestSchema>::decode_owned(op.1.clone()).unwrap();
        let value = WalValue::<TestSchema>::decode_owned(op.2.clone().unwrap()).unwrap();

        // Assert Key
        assert_eq!(key.snapshot_id.0, 4);
        assert_eq!(key.modification_id.0, 0);
        assert_eq!(
            key.operation_specific_parts,
            WalKeySpecificPart::DiscardMeta
        );

        // Assert Value
        if let WalValue::MetaValue {
            commit_id: cid,
            maybe_parent_cid,
            map_value_count,
        } = value
        {
            assert_eq!(cid, commit_id);
            assert_eq!(maybe_parent_cid, None);
            assert_eq!(map_value_count, 0);
        } else {
            panic!("Expected MetaValue");
        }
    }

    #[test]
    fn test_log_add_non_root_node() {
        // Arrange
        let db = WrappedInMemoryDb::<PendingTableName>::empty();
        let write_schema = WrappedInMemoryDb::<PendingTableName>::write_schema();
        let mut tracker = PersistenceTracker {
            snapshot_id: SnapshotId(1),
            next_modification_id: ModificationId(5),
        };

        let commit_id = H256::from_low_u64_be(100);
        let parent_commit_id = H256::from_low_u64_be(99);
        let mut modifications = RecoverMap::<TestSchema>::new();
        modifications.insert(
            k(b"key1"),
            RecoverRecord {
                value: ValueEntry::Value(k(b"val1")),
                last_commit_id: None,
            },
        );

        modifications.insert(
            k(b"key2"),
            RecoverRecord {
                value: ValueEntry::Value(k(b"val2")),
                last_commit_id: Some(H256::default()),
            },
        );

        modifications.insert(
            k(b"key3"),
            RecoverRecord {
                value: ValueEntry::Deleted,
                last_commit_id: None,
            },
        );

        // Act
        log_add_non_root_node::<TestSchema>(
            &write_schema,
            &mut tracker,
            commit_id,
            parent_commit_id,
            &modifications,
        );

        // Assert
        // 1. Check tracker state
        assert_eq!(tracker.snapshot_id.0, 1, "Snapshot ID should not change");
        assert_eq!(
            tracker.next_modification_id.0, 6,
            "Next modification ID should be incremented"
        );

        // 2. Check written data
        let ops = write_schema.drain();
        assert_eq!(ops.len(), 4, "Should write one meta and three map records");

        let mut meta_found = false;
        let mut map_keys_found = 0;

        for op in ops {
            assert_eq!(op.0, WalTable::<TestSchema>::NAME); // All ops are for WalTable
            let key = WalKey::<TestSchema>::decode_owned(op.1).unwrap();
            let value = WalValue::<TestSchema>::decode_owned(op.2.unwrap()).unwrap();

            assert_eq!(key.snapshot_id.0, 1);
            assert_eq!(key.modification_id.0, 5);

            match key.operation_specific_parts {
                WalKeySpecificPart::AddNodeMeta => {
                    meta_found = true;
                    if let WalValue::MetaValue {
                        commit_id: cid,
                        maybe_parent_cid,
                        map_value_count,
                    } = value
                    {
                        assert_eq!(cid, commit_id);
                        assert_eq!(maybe_parent_cid, Some(parent_commit_id));
                        assert_eq!(map_value_count, 3);
                    } else {
                        panic!("Expected MetaValue");
                    }
                }
                WalKeySpecificPart::AddNodeMapKey(map_key) => {
                    map_keys_found += 1;
                    let map_value = modifications.get(&map_key).unwrap();
                    if let WalValue::MapValue(val) = value {
                        assert_eq!(&val, map_value);
                    } else {
                        panic!("Expected MapValue");
                    }
                }
                _ => panic!("Unexpected WAL operation type"),
            }
        }
        assert!(meta_found, "Meta record was not written");
        assert_eq!(map_keys_found, 3, "Map records were not written correctly");
    }

    #[test]
    fn test_log_change_root() {
        // Arrange
        let db = WrappedInMemoryDb::<PendingTableName>::empty();
        let write_schema = WrappedInMemoryDb::<PendingTableName>::write_schema();
        let mut tracker = PersistenceTracker {
            snapshot_id: SnapshotId(1),
            next_modification_id: ModificationId(5),
        };

        let new_root_cid = H256::from_low_u64_be(200);
        let new_height = 101;
        let new_parent = H256::from_low_u64_be(199);

        // Act
        log_change_root::<TestSchema>(
            &write_schema,
            &mut tracker,
            new_root_cid,
            new_height,
            new_parent,
        );

        // Assert
        // 1. Check tracker state
        assert_eq!(tracker.snapshot_id.0, 2, "Snapshot ID should be advanced");
        assert_eq!(
            tracker.next_modification_id.0, 0,
            "Next modification ID should be reset for new snapshot"
        );

        // 2. Check written data
        let ops = write_schema.drain();
        assert_eq!(ops.len(), 2, "Should write one WAL and one Snapshot record");

        let wal_op = ops
            .iter()
            .find(|op| op.0 == WalTable::<TestSchema>::NAME)
            .unwrap();
        let snap_op = ops
            .iter()
            .find(|op| op.0 == SnapshotsTable::<TestSchema>::NAME)
            .unwrap();

        // Assert WAL record
        let wal_key = WalKey::<TestSchema>::decode_owned(wal_op.1.clone()).unwrap();
        assert_eq!(
            wal_key.snapshot_id.0, 1,
            "WAL record should use the *old* snapshot ID"
        );
        assert_eq!(wal_key.modification_id.0, 5);
        assert_eq!(
            wal_key.operation_specific_parts,
            WalKeySpecificPart::ChangeRootMeta
        );

        // Assert Snapshot record
        let snap_key = SnapshotKey::decode_owned(snap_op.1.clone()).unwrap();
        let snap_value =
            SnapshotValue::<TestSchema>::decode_owned(snap_op.2.clone().unwrap()).unwrap();
        assert_eq!(snap_key.0, new_height);
        assert_eq!(
            snap_value.snapshot_id.0, 2,
            "Snapshot record should use the *new* snapshot ID"
        );
        assert_eq!(snap_value.parent_of_root, Some(new_parent));
    }
}
