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
