mod bootstrap;
mod cleanup;
mod format;
mod recovery;
mod writer;

// This module contains shared, low-level persistence operations.
// It is marked as `mod` to be private to the `persistence` module and its children.
mod primitive;

#[cfg(test)]
pub mod test_util;

use std::{borrow::Cow, iter};

pub use bootstrap::BootstrapError;
pub use recovery::{primitives::SnapshotReadError, RecoveryError};

pub use bootstrap::primitives::{
    clear_pending_schema as primitives_clear_pending_schema,
    initialize_empty_schema as primitives_initialize_empty_schema,
    verify_schema_is_empty as primitives_verify_schema_is_empty,
};
pub use cleanup::primitives::gc_until_height as primitives_gc_until_height;
pub use recovery::primitives::{
    recover_schema as primitives_recover_schema,
    verify_no_newer_records as primitives_verify_no_newer_records,
};

pub(super) use writer::{
    log_add_non_root_node, log_add_root, log_change_root, log_discard, log_make_pivot,
};

use self::format::{
    ModificationId, SnapshotId, SnapshotKeyTreePart, SnapshotMapValue, SnapshotNodeDataType,
    SnapshotRecordType, SnapshotValue, SnapshotsTable, WalKeySpecificPart, WalTable, WalValue,
};
use crate::{
    backends::{
        serde::{Decode, Encode, EncodeSubKey, FixedLengthEncoded},
        DatabaseTrait, PendingTableName, SeekKey, TableItem, TableIter, TableRead, TableSchema,
        WriteSchemaTrait,
    },
    middlewares::versioned_flat_key_value::pending_part::persistence::format::{
        SnapshotKey, WalKey,
    },
};

use super::{
    pending_schema::{PendingKeyValueSchema, RecoverMap, RecoverRecord},
    tree::{Tree, TreeSnapshot, TreeSnapshotNode},
    tree_with_tracker::TreeWithTracker,
};

use crate::{
    errors::{DecResult, DecodeError, Result},
    types::ValueEntry,
};

#[cfg(test)]
use crate::{
    backends::{VersionedKVName, WrappedInMemoryDb},
    errors::StorageError,
    middlewares::versioned_flat_key_value::{PendingKeyValueConfig, VersionedKeyValueSchema},
};

/// Tracks the sequence of state modifications to be logged for recovery.
///
/// This struct maintains the necessary counters to correctly sequence operations
/// for the write-ahead log (WAL). It ensures that each modification can be uniquely
/// identified relative to a base snapshot, which is crucial for reconstructing the
/// in-memory `tree` after a restart. It does not track completion of I/O, but rather
/// what the identity of the next submitted operation should be.
#[derive(Debug, PartialEq, Eq)]
pub struct PersistenceTracker {
    /// The identifier for the most recent snapshot that was created.
    /// Subsequent modifications are logged as a delta from this snapshot.
    snapshot_id: SnapshotId,
    /// The sequence ID to be assigned to the next modification record. This counter
    /// increments for each operation logged after the last snapshot.
    next_modification_id: ModificationId,
    /// The number of `change_root` operations logged since the last snapshot. A
    /// `change_root` writes a new snapshot only when this reaches
    /// [`CHANGE_ROOTS_PER_SNAPSHOT`]` - 1`; the others are logged to the WAL only.
    change_roots_since_snapshot: u64,
}

/// A `change_root` writes a snapshot of the whole tree after it once every this
/// many `change_root`s; the others only log a WAL record. Recovery starts from the
/// latest snapshot at or below the height of the pending root and replays the
/// `change_root`s logged after it, so a larger value makes `change_root` and the
/// cleanup of old snapshots cheaper and recovery slower.
pub const CHANGE_ROOTS_PER_SNAPSHOT: u64 = 8;

impl PersistenceTracker {
    /// Advances the tracker to a new snapshot.
    pub fn advance_to_next_snapshot(&mut self) {
        self.snapshot_id.0 += 1;
        self.next_modification_id = ModificationId(0);
        self.change_roots_since_snapshot = 0;
    }

    /// Increments the modification ID counter for the next operation.
    pub fn advance_to_next_modification(&mut self) {
        self.next_modification_id.0 += 1;
    }
}

fn write_tree_snapshot<S: PendingKeyValueSchema>(
    tree: &Tree<S>,
    snapshot_id: SnapshotId,
    write_schema: &impl WriteSchemaTrait<PendingTableName>,
) {
    write_exported_snapshot(tree.export_snapshot(), snapshot_id, write_schema);
}

/// Writes a snapshot already exported from a tree, see [`write_tree_snapshot`].
fn write_exported_snapshot<S: PendingKeyValueSchema>(
    snapshot: TreeSnapshot<S>,
    snapshot_id: SnapshotId,
    write_schema: &impl WriteSchemaTrait<PendingTableName>,
) {
    let TreeSnapshot {
        parent_of_root,
        height_of_root: snapshot_root_height,
        nodes,
    } = snapshot;

    // Emit meta record first.
    let meta_key = SnapshotKey::<S> {
        snapshot_root_height,
        record_type: SnapshotRecordType::Meta,
    };
    let meta_val = SnapshotValue::MetaValue {
        parent_of_root,
        snapshot_id,
        nodes_count: nodes.len() as u64,
    };
    let meta_op = iter::once((
        Cow::<SnapshotKey<S>>::Owned(meta_key),
        Some(Cow::<SnapshotValue<S>>::Owned(meta_val)),
    ));

    // Emit records for nodes.
    let map_iter = nodes.into_iter().flat_map(|n| {
        let node_height = n.node_height;
        let node_commit_id = n.node_commit_id;

        // NodeMeta
        let meta_k = SnapshotKey::<S> {
            snapshot_root_height,
            record_type: SnapshotRecordType::Map(SnapshotKeyTreePart {
                node_height,
                node_commit_id,
                node_data_type: SnapshotNodeDataType::NodeMeta,
            }),
        };
        let meta_v = SnapshotValue::MapValue(SnapshotMapValue::NodeMeta {
            parent_commit_id: n.node_parent_commit_id,
            modifications_count: n.modifications.len() as u64,
        });

        // NodeMap
        let maps = n.modifications.into_iter().map({
            move |(key, rec)| {
                let k = SnapshotKey::<S> {
                    snapshot_root_height,
                    record_type: SnapshotRecordType::Map(SnapshotKeyTreePart {
                        node_height,
                        node_commit_id,
                        node_data_type: SnapshotNodeDataType::NodeMap { key },
                    }),
                };
                let v = SnapshotValue::MapValue(SnapshotMapValue::NodeMap(rec));
                (Cow::Owned(k), Some(Cow::Owned(v)))
            }
        });

        std::iter::once((Cow::Owned(meta_k), Some(Cow::Owned(meta_v)))).chain(maps)
    });

    let snapshot_op_iter = meta_op.chain(map_iter);

    write_schema.write_batch::<SnapshotsTable<S>>(snapshot_op_iter);
}

/// Wall-clock time of each step of writing a snapshot of a tree, see
/// [`time_tree_snapshot`].
#[cfg(test)]
#[derive(Debug, Default, Clone, Copy)]
pub struct SnapshotTiming {
    /// Copying the tree out of memory.
    pub export: std::time::Duration,
    /// Encoding the copy into a write batch.
    pub encode: std::time::Duration,
    /// Committing that batch to the database.
    pub commit: std::time::Duration,
    /// Number of key-value records in the copy, over all nodes.
    pub records: usize,
}

/// Writes a snapshot of `tree` to `db`, a database used only for this, and times
/// each step. For benches deciding which steps of a snapshot could leave the lock
/// held during a root change.
#[cfg(test)]
pub(super) fn time_tree_snapshot<S: PendingKeyValueSchema, P: DatabaseTrait<PendingTableName>>(
    tree: &Tree<S>,
    db: &P,
) -> Result<SnapshotTiming> {
    use std::time::Instant;

    let start = Instant::now();
    let snapshot = tree.export_snapshot();
    let export = start.elapsed();
    let records = snapshot.nodes.iter().map(|n| n.modifications.len()).sum();

    let start = Instant::now();
    let write_schema = P::write_schema();
    write_exported_snapshot(snapshot, SnapshotId(0), &write_schema);
    let encode = start.elapsed();

    let start = Instant::now();
    db.commit(write_schema)?;
    let commit = start.elapsed();

    Ok(SnapshotTiming {
        export,
        encode,
        commit,
        records,
    })
}
