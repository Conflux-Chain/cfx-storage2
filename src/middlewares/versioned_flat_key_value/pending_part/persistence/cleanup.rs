pub mod primitives {
    //! Contains low-level, generic primitives for the cleanup process.
    //!
    //! **Warning**: These functions are low-level building blocks. You should generally
    //! prefer the high-level, encapsulated cleanup functions provided by a concrete
    //! application (e.g., `lvmt`), unless you are building a new composite application
    //! yourself.

    use std::sync::Arc;

    use super::super::{
        primitive::delete_snapshot_and_wal_by_snapshot_id, DatabaseTrait, PendingKeyValueSchema,
        PendingTableName, Result, SnapshotsTable, TableRead, WalTable,
    };

    /// Background GC: Cleans up all snapshots and their WALs with a height < durable_height.
    ///
    /// Iteration method: Forward iteration, starting from the smallest height, and stopping when a height >= durable_height is encountered.
    ///
    /// Note:
    /// - Does not commit. The upper layer is responsible for a unified commit to ensure atomic consistency across multiple tables.
    /// - Requires durable_height to be provided by the historical part, representing a "fully persisted and safe" height.
    pub fn gc_until_height<S: PendingKeyValueSchema, P: DatabaseTrait<PendingTableName>>(
        db: &Arc<P>,
        write_schema: &P::WriteSchema,
        durable_height: u64,
    ) -> Result<()> {
        let snapshots_view = Arc::new(db.view::<SnapshotsTable<S>>()?);
        let wal_view = Arc::new(db.view::<WalTable<S>>()?);

        // Forward scan from the beginning of the snapshots table
        let iter = snapshots_view.iter_from_start()?;
        for old_snapshot_item_res in iter {
            let old_snapshot_item = old_snapshot_item_res?;

            let height = old_snapshot_item.0 .0;
            if height >= durable_height {
                break;
            }

            // Delete this snapshot and its WAL
            delete_snapshot_and_wal_by_snapshot_id::<S, P>(
                &wal_view,
                write_schema,
                old_snapshot_item,
            )?;
        }

        Ok(())
    }
}
