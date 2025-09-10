pub mod primitives {
    //! Contains low-level, generic primitives for the cleanup process.
    //!
    //! **Warning**: These functions are low-level building blocks. You should generally
    //! prefer the high-level, encapsulated cleanup functions provided by a concrete
    //! application (e.g., `lvmt`), unless you are building a new composite application
    //! yourself.

    use std::sync::Arc;

    use crate::backends::WriteSchemaTrait;

    use super::super::{
        primitive::delete_wal_by_snapshot_id, DatabaseTrait, PendingKeyValueSchema,
        PendingTableName, Result, SnapshotValue, SnapshotsTable, TableRead, WalTable,
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
        let mut last_height = None;
        for old_snapshot_item_res in iter {
            let old_snapshot_item = old_snapshot_item_res?;

            let height = old_snapshot_item.0.snapshot_root_height;

            // End
            if height >= durable_height {
                break;
            }

            // This is a new snapshot
            if last_height != Some(height) {
                last_height = Some(height);

                // Get the SnapshotId
                let snapshot_id = match old_snapshot_item.1.as_ref() {
                    SnapshotValue::MetaValue {
                        parent_of_root,
                        snapshot_id,
                        nodes_count,
                    } => snapshot_id,
                    SnapshotValue::MapValue(_) => todo!(),
                };

                // Delete all WAL of this snapshot
                delete_wal_by_snapshot_id::<S, P>(&wal_view, write_schema, *snapshot_id)?;
            }

            // Delete this snapshot record
            write_schema.write::<SnapshotsTable<S>>((old_snapshot_item.0, None));
        }

        Ok(())
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

    /// Helper function: runs a complete GC test case.
    ///
    /// # Arguments
    ///
    /// * `durable_height` - The durable height passed to `gc_until_height`.
    /// * `expected_deleted_snap_records` - The number of snapshot records expected to be deleted.
    /// * `expected_deleted_wal_records` - The number of WAL records expected to be deleted.
    fn run_gc_test_case(
        durable_height: u64,
        expected_deleted_snap_records: usize,
        expected_deleted_wal_records: usize,
    ) {
        // Arrange
        let db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());
        let write_schema = WrappedInMemoryDb::<PendingTableName>::write_schema();

        // Populate the DB with our test data.
        setup_db_with_snapshots_and_wals(&db, TEST_DATA);

        // Act
        gc_until_height::<TestSchema, _>(&db, &write_schema, durable_height).unwrap();

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

        assert_eq!(
            deleted_snaps, expected_deleted_snap_records,
            "Mismatch in deleted snapshot records for durable_height={}",
            durable_height
        );
        assert_eq!(
            deleted_wals, expected_deleted_wal_records,
            "Mismatch in deleted WAL records for durable_height={}",
            durable_height
        );
    }

    #[test]
    fn test_gc_deletes_nothing_if_height_is_lte_first_snapshot() {
        // durable_height = 10, condition is `height < 10`, so nothing is deleted.
        run_gc_test_case(10, 0, 0);

        // durable_height = 9, condition is `height < 9`, so nothing is deleted.
        run_gc_test_case(9, 0, 0);
    }

    #[test]
    fn test_gc_deletes_up_to_boundary_height_21() {
        // durable_height = 21, deletes snapshots at heights 10 and 20.
        // Snap records: (1 meta + 1 node) from h=10 + (1 meta + 2 nodes) from h=20 = 2 + 3 = 5
        // WAL records: 2 from sid=1 + 3 from sid=2 = 5
        run_gc_test_case(21, 5, 5);
    }

    #[test]
    fn test_gc_deletes_up_to_mid_range_height_25() {
        // durable_height = 25, also deletes snapshots at heights 10 and 20.
        // Same expectation as for height 21.
        run_gc_test_case(25, 5, 5);
    }

    #[test]
    fn test_gc_deletes_up_to_boundary_height_30() {
        // durable_height = 30, deletes snapshots at heights 10 and 20.
        // The snapshot at height 30 is kept because 30 is not < 30.
        // Same expectation as for height 21.
        run_gc_test_case(30, 5, 5);
    }

    #[test]
    fn test_gc_deletes_all_if_height_is_greater_than_last_snapshot() {
        // durable_height = 31, deletes all snapshots (heights 10, 20, 30).
        // Snap records: 2 (h=10) + 3 (h=20) + 2 (h=30) = 7
        // WAL records: 2 (sid=1) + 3 (sid=2) + 1 (sid=3) = 6
        run_gc_test_case(31, 7, 6);
    }
}
