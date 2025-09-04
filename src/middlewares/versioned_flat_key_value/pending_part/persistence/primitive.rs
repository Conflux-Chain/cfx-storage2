use std::{borrow::Cow, sync::Arc};

use super::{
    DatabaseTrait, PendingKeyValueSchema, PendingTableName, Result, SnapshotKey, SnapshotValue,
    SnapshotsTable, TableRead, WalKey, WalTable, WriteSchemaTrait,
};

/// Deletes a snapshot and its corresponding WAL file using the snapshot ID.
///
/// This is a low-level primitive shared by recovery and cleanup logic.
///
/// The caller should provide the exact `snapshot_item`
/// to be used directly to avoid an extra lookup.
pub(super) fn delete_snapshot_and_wal_by_snapshot_id<
    S: PendingKeyValueSchema,
    P: DatabaseTrait<PendingTableName>,
>(
    wal_view: &Arc<impl TableRead<WalTable<S>> + Send + Sync>,
    write_schema: &P::WriteSchema,
    snapshot_item: (Cow<'_, SnapshotKey>, Cow<'_, SnapshotValue<S>>),
) -> Result<()> {
    let (snapshot_key_cow, snapshot_value_cow) = snapshot_item;
    let SnapshotValue { snapshot_id, .. } = snapshot_value_cow.as_ref().clone();

    // Delete the snapshot entry.
    let op = (snapshot_key_cow, None);
    write_schema.write::<SnapshotsTable<S>>(op);

    // Delete all WAL entries for that snapshot.
    let wal_seek_key = WalKey::seek_key_for_snapshot(snapshot_id);
    let wal_iter = wal_view.iter(&wal_seek_key.key)?;

    for wal_item in wal_iter {
        let (wal_key_cow, _) = wal_item?;

        if wal_key_cow.snapshot_id != snapshot_id {
            break;
        }

        let op = (wal_key_cow, None);
        write_schema.write::<WalTable<S>>(op);
    }

    Ok(())
}
