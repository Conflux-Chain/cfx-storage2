use thiserror::Error;

/// Errors that can occur during the state recovery process from the database.
#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryError {
    #[error("No valid snapshot found at or after the specified recovery height.")]
    NoValidSnapshotFound,
    #[error("Inconsistent snapshot state: Found a snapshot at or after the recovery height, but it's not a valid recovery point.")]
    InconsistentSnapshotState,

    #[error("Inconsistent WAL record sequence: {0}")]
    InconsistentWalRecord(&'static str),
    #[error("WAL map value count mismatch: expected {expected}, but got {got}.")]
    WalMapValueCountMismatch { expected: u64, got: u64 },
    #[error("Unexpected WAL record found. A new modification should start with a Meta record.")]
    UnexpectedWalRecord,
}

pub mod primitives {
    //! Contains low-level, generic primitives for state recovery from the persistence layer.
    //!
    //! **Warning**: These functions are low-level building blocks. You should generally
    //! prefer the high-level, encapsulated recovery functions provided by a concrete
    //! application (e.g., `lvmt`), unless you are building a new composite application
    //! yourself.

    mod wal_player;

    use std::sync::Arc;

    use super::RecoveryError;

    use super::super::{
        DatabaseTrait, ModificationId, PendingKeyValueSchema, PendingTableName, PersistenceTracker,
        RecoverMap, Result, SnapshotId, SnapshotKey, SnapshotValue, SnapshotsTable, TableRead,
        Tree, TreeWithTracker, WalKey, WalKeySpecificPart, WalTable, WalValue, WriteSchemaTrait,
    };

    /// Executes the recovery logic for a single pending schema from the database.
    /// Returns the recovered in-memory Tree and its corresponding persistence state.
    /// Any required DB modifications are added to the provided write_schema.
    ///
    /// This is a low-level primitive. See the [module-level documentation](self) for usage guidelines.
    pub fn recover_schema<S: PendingKeyValueSchema, P: DatabaseTrait<PendingTableName>>(
        db: &Arc<P>,
        write_schema: &impl WriteSchemaTrait<PendingTableName>,
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
                let snap_height = snap_key_cow.into_owned().0;
                let SnapshotValue {
                    parent_of_root: snap_parent_cid,
                    snapshot_id,
                } = snap_value_cow.as_ref().clone();

                // Check if the found snapshot is a perfect match.
                if snap_height == height_of_root && snap_parent_cid == parent_of_root {
                    // --- Path A: Found a valid recovery point. Restore and replay WAL. ---

                    // Rebuild the tree from the snapshot's base info.
                    let mut tree = Tree::new(parent_of_root, height_of_root);

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
                    for invalid_item in iter {
                        let (invalid_snap_key_cow, invalid_snap_value_cow) = invalid_item?;
                        let SnapshotValue {
                            snapshot_id: invalid_snapshot_id,
                            ..
                        } = invalid_snap_value_cow.as_ref().clone();

                        // Delete the invalid snapshot entry.
                        let op = (invalid_snap_key_cow, None);
                        write_schema.write::<SnapshotsTable<S>>(op);

                        // Delete all WAL entries for that invalid snapshot.
                        let invalid_wal_seek_key =
                            WalKey::seek_key_for_snapshot(invalid_snapshot_id);
                        let invalid_wal_iter = wal_view.iter(&invalid_wal_seek_key.key)?;

                        for invalid_wal_item in invalid_wal_iter {
                            let (invalid_wal_key_cow, _) = invalid_wal_item?;

                            if invalid_wal_key_cow.snapshot_id != invalid_snapshot_id {
                                break;
                            }

                            let op = (invalid_wal_key_cow, None);
                            write_schema.write::<WalTable<S>>(op);
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
}
