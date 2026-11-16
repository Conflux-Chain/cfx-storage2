pub mod primitives {
    //! Contains low-level, generic primitives for the cleanup process.
    //!
    //! **Warning**: These functions are low-level building blocks. You should generally
    //! prefer the high-level, encapsulated cleanup functions provided by a concrete
    //! application (e.g., `lvmt`), unless you are building a new composite application
    //! yourself.

    use std::sync::Arc;

    use log::info;

    use crate::backends::WriteSchemaTrait;

    use super::super::{
        primitive::delete_wal_by_snapshot_id, DatabaseTrait, PendingKeyValueSchema,
        PendingTableName, RecoveryError, Result, SnapshotKey, SnapshotValue, SnapshotsTable,
        TableRead, WalTable,
    };

    /// Background GC: deletes the snapshots, with their WALs, that recovery no longer
    /// needs once the pending root is above `durable_height`.
    ///
    /// Recovery starts from the latest snapshot at or below the height of the pending
    /// root (snapshots are written only once every few `change_root`s), so the latest
    /// snapshot at or below `durable_height` is kept even if it is below it, and only
    /// the older ones are deleted. Snapshots below `from_height` are not looked at.
    ///
    /// Returns the height of the kept snapshot, the `from_height` to pass next time,
    /// or `None` if there is no snapshot from `from_height` up to `durable_height`.
    ///
    /// Note:
    /// - Does not commit. The upper layer is responsible for a unified commit to ensure atomic consistency across multiple tables.
    /// - Requires durable_height to be provided by the historical part, representing a "fully persisted and safe" height.
    pub fn gc_until_height<S: PendingKeyValueSchema, P: DatabaseTrait<PendingTableName>>(
        db: &Arc<P>,
        write_schema: &P::WriteSchema,
        durable_height: u64,
        from_height: u64,
    ) -> Result<Option<u64>> {
        info!(
            "Starting background GC for pending schema {:?}. Cleaning up versions from height={} to durable_height={}.",
            S::KV_NAME,
            from_height,
            durable_height
        );

        let snapshots_view = Arc::new(db.view::<SnapshotsTable<S>>()?);
        let wal_view = Arc::new(db.view::<WalTable<S>>()?);

        // The snapshots from `from_height` up to `durable_height`. Snapshot records are
        // ordered by height with each snapshot's meta record first, so jump from one
        // snapshot's meta record to the next.
        let mut snapshots = Vec::new();
        let mut next_height = Some(from_height);
        while let Some(height) = next_height {
            let seek_key = SnapshotKey::seek_key_for_height(height);
            let Some(item) = snapshots_view.iter(&seek_key.key)?.next() else {
                break;
            };
            let (key, value) = item?;
            let found = key.as_ref().snapshot_root_height;
            if found > durable_height {
                break;
            }
            let SnapshotValue::MetaValue { snapshot_id, .. } = value.as_ref() else {
                Err(RecoveryError::FirstRecordIsNotMeta)?
            };
            snapshots.push((found, *snapshot_id));
            next_height = found.checked_add(1);
        }

        let Some((&(kept_height, _), to_delete)) = snapshots.split_last() else {
            return Ok(None);
        };
        for &(height, snapshot_id) in to_delete {
            delete_wal_by_snapshot_id::<S, P>(&wal_view, write_schema, snapshot_id)?;
            let seek_key = SnapshotKey::seek_key_for_height(height);
            for item in snapshots_view.iter(&seek_key.key)? {
                let (key, _) = item?;
                if key.as_ref().snapshot_root_height != height {
                    break;
                }
                write_schema.write::<SnapshotsTable<S>>((key, None));
            }
        }

        info!(
            "Finished background GC for schema {:?}. Deleted {} old snapshots, kept the one at height {}.",
            S::KV_NAME,
            to_delete.len(),
            kept_height,
        );

        Ok(Some(kept_height))
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        format::{SnapshotsTable, WalTable},
        test_util::{setup_db_with_snapshots_and_wals, TestSchema},
        DatabaseTrait, PendingTableName, TableSchema, WrappedInMemoryDb,
    };
    use super::primitives::*;
    use std::sync::Arc;

    // This is the mock data we will use for all GC tests.
    // Format: (height, snapshot_id, num_wal_records, num_snapshot_node_records)
    const TEST_DATA: &[(u64, u64, u64, u64)] = &[
        (10, 1, 2, 1), // height=10, sid=1, 2 WALs, 1 meta + 1 node record = 2 snapshot records
        (20, 2, 3, 2), // height=20, sid=2, 3 WALs, 1 meta + 2 node records = 3 snapshot records
        (30, 3, 1, 1), // height=30, sid=3, 1 WAL,  1 meta + 1 node record = 2 snapshot records
    ];

    /// Runs `gc_until_height` on `TEST_DATA` and checks the number of deleted snapshot
    /// and WAL records and the returned height of the kept snapshot.
    fn run_gc_test_case(
        durable_height: u64,
        from_height: u64,
        expected_deleted_snap_records: usize,
        expected_deleted_wal_records: usize,
        expected_kept_height: Option<u64>,
    ) {
        // Arrange
        let db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());
        let write_schema = WrappedInMemoryDb::<PendingTableName>::write_schema();

        // Populate the DB with our test data.
        setup_db_with_snapshots_and_wals(&db, TEST_DATA);

        // Act
        let kept_height =
            gc_until_height::<TestSchema, _>(&db, &write_schema, durable_height, from_height)
                .unwrap();

        // Assert
        let ops = write_schema.drain();
        let deleted_snaps = ops
            .iter()
            .filter(|(table_name, _, value)| {
                *table_name == SnapshotsTable::<TestSchema>::NAME && value.is_none()
            })
            .count();

        let deleted_wals = ops
            .iter()
            .filter(|(table_name, _, value)| {
                *table_name == WalTable::<TestSchema>::NAME && value.is_none()
            })
            .count();

        let case = format!("durable_height={durable_height}, from_height={from_height}");
        assert_eq!(
            deleted_snaps, expected_deleted_snap_records,
            "deleted snapshot records, {case}"
        );
        assert_eq!(
            deleted_wals, expected_deleted_wal_records,
            "deleted WAL records, {case}"
        );
        assert_eq!(kept_height, expected_kept_height, "kept snapshot, {case}");
    }

    // Snapshot records: 2 at h=10, 3 at h=20, 2 at h=30.
    // WAL records: 2 for sid=1, 3 for sid=2, 1 for sid=3.

    #[test]
    fn test_gc_nothing_at_or_below_durable_height() {
        run_gc_test_case(9, 0, 0, 0, None);
    }

    #[test]
    fn test_gc_keeps_the_only_snapshot_at_or_below_durable_height() {
        run_gc_test_case(10, 0, 0, 0, Some(10));
        run_gc_test_case(19, 0, 0, 0, Some(10));
    }

    #[test]
    fn test_gc_keeps_the_latest_snapshot_at_or_below_durable_height() {
        // h=20 is kept, h=10 deleted.
        run_gc_test_case(20, 0, 2, 2, Some(20));
        run_gc_test_case(25, 0, 2, 2, Some(20));
        // h=30 is kept, h=10 and h=20 deleted.
        run_gc_test_case(30, 0, 5, 5, Some(30));
        run_gc_test_case(100, 0, 5, 5, Some(30));
    }

    // --- Tests for the from_height parameter ---

    #[test]
    fn test_gc_from_height_skips_earlier_snapshots() {
        // h=10 is not looked at; h=20 is deleted, h=30 kept.
        run_gc_test_case(31, 15, 3, 3, Some(30));
        // h=20 is the only one looked at, so it is kept.
        run_gc_test_case(25, 15, 0, 0, Some(20));
    }

    #[test]
    fn test_gc_from_height_at_exact_snapshot_height() {
        // The kept snapshot of the previous run is looked at again.
        run_gc_test_case(25, 20, 0, 0, Some(20));
        run_gc_test_case(30, 20, 3, 3, Some(30));
    }

    #[test]
    fn test_gc_from_height_past_all_snapshots() {
        run_gc_test_case(100, 31, 0, 0, None);
    }

    #[test]
    fn test_gc_from_height_above_durable_height() {
        run_gc_test_case(25, 25, 0, 0, None);
        run_gc_test_case(25, 30, 0, 0, None);
    }
}
