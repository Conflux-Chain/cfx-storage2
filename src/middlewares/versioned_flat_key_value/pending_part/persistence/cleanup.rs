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

#[cfg(test)]
mod tests {
    use super::super::{
        format::{SnapshotKey, SnapshotsTable, WalKey, WalTable},
        test_util::{setup_db_with_snapshots_and_wals, TestSchema},
        DatabaseTrait, Decode, PendingTableName, TableSchema, WrappedInMemoryDb,
    };
    use super::primitives::*;
    use std::{collections::HashSet, sync::Arc};

    fn get_ids_before_height(data: &[(u64, u64, u64)], durable_height: u64) -> HashSet<u64> {
        data.iter()
            .filter(|&&(h, _, _)| h < durable_height)
            .map(|&(_, id, _)| id)
            .collect()
    }

    // For simplicity, we create and manipulate mock data directly inside the test functions.

    /// Helper function: runs a complete GC test case.
    ///
    /// # Arguments
    ///
    /// * `durable_height` - The durable height passed to `gc_until_height`.
    /// * `expected_deleted_snaps` - The number of snapshot records expected to be deleted.
    fn run_gc_test_case(durable_height: u64, expected_deleted_snaps: usize) {
        // Arrange
        let db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());

        let snapshots_and_wals = &[(10, 1, 2), (20, 2, 2), (30, 3, 2)];
        let expected_snaps_to_be_deleted =
            get_ids_before_height(snapshots_and_wals, durable_height);
        setup_db_with_snapshots_and_wals(&db, snapshots_and_wals);

        let write_schema = WrappedInMemoryDb::<PendingTableName>::write_schema();

        // Act
        gc_until_height::<TestSchema, _>(&db, &write_schema, durable_height).unwrap();

        // Assert
        let ops = write_schema.drain();
        // Assume 2 WAL records per snapshot.
        let expected_deleted_wals = expected_deleted_snaps * 2;
        let expected_total_deletions = expected_deleted_snaps + expected_deleted_wals;

        assert_eq!(
            ops.len(),
            expected_total_deletions,
            "Incorrect total number of delete operations for durable_height = {}",
            durable_height
        );

        let mut deleted_snap_count = 0;
        let mut deleted_wal_count = 0;

        for (table_name, encoded_key, encoded_value) in ops {
            assert!(
                encoded_value.is_none(),
                "A GC operation should be a delete (value is None)"
            );
            if table_name == SnapshotsTable::<TestSchema>::NAME {
                deleted_snap_count += 1;

                let key = SnapshotKey::decode_owned(encoded_key.clone()).unwrap();
                assert!(key.0 < durable_height);
            } else if table_name == WalTable::<TestSchema>::NAME {
                deleted_wal_count += 1;

                let key = WalKey::<TestSchema>::decode_owned(encoded_key.clone()).unwrap();
                assert!(expected_snaps_to_be_deleted.contains(&key.snapshot_id.0));
            }
        }

        assert_eq!(
            deleted_snap_count, expected_deleted_snaps,
            "Incorrect number of deleted snapshot records for durable_height = {}",
            durable_height
        );
        assert_eq!(
            deleted_wal_count, expected_deleted_wals,
            "Incorrect number of deleted WAL records for durable_height = {}",
            durable_height
        );
    }

    #[test]
    fn test_gc_deletes_nothing_if_height_is_lte_first_snapshot() {
        // With durable_height = 10, the cleanup condition is `height < durable_height`,
        // so the snapshot at height 10 is not deleted.
        run_gc_test_case(10, 0);

        // A durable_height < 10 should also result in no deletions.
        run_gc_test_case(9, 0);
    }

    #[test]
    fn test_gc_deletes_up_to_boundary_height_21() {
        // durable_height = 21 should delete snapshots at heights 10 and 20.
        // The snapshot at height 30 (30 >= 21) is kept.
        run_gc_test_case(21, 2);
    }

    #[test]
    fn test_gc_deletes_up_to_mid_range_height_25() {
        // durable_height = 25 should delete snapshots at heights 10 and 20.
        // The snapshot at height 30 (30 >= 25) is kept.
        run_gc_test_case(25, 2);
    }

    #[test]
    fn test_gc_deletes_up_to_boundary_height_30() {
        // durable_height = 30 should delete snapshots at heights 10 and 20.
        // The snapshot at height 30 (30 >= 30) is kept.
        run_gc_test_case(30, 2);
    }

    #[test]
    fn test_gc_deletes_all_if_height_is_greater_than_last_snapshot() {
        // durable_height = 31 is greater than all snapshot heights (10, 20, 30),
        // so all snapshots are deleted.
        run_gc_test_case(31, 3);
    }
}
