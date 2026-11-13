use std::sync::Arc;

use super::{
    format::SnapshotId, DatabaseTrait, PendingKeyValueSchema, PendingTableName, Result, TableRead,
    WalKey, WalTable, WriteSchemaTrait,
};

/// Deletes all WAL records of a snapshot using the snapshot ID.
///
/// This is a low-level primitive shared by recovery and cleanup logic.
///
/// The deletion of the snapshot records is beyond this function.
pub(super) fn delete_wal_by_snapshot_id<
    S: PendingKeyValueSchema,
    P: DatabaseTrait<PendingTableName>,
>(
    wal_view: &Arc<impl TableRead<WalTable<S>> + Send + Sync>,
    write_schema: &P::WriteSchema,
    snapshot_id: SnapshotId,
) -> Result<()> {
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
