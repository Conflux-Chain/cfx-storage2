pub trait TableNameTrait: Send + Sync + 'static + Copy + Into<u32> + Into<&'static str> + TryFrom<u32, Error = ()> {
    /// Returns the total number of Column Families in the Historical DB.
    /// This value can be used directly to configure a RocksDB instance.
    fn num_tables() -> u32;

    fn get_cache_size_for_col(cache_any: &dyn Any, col_id: u32) -> String;

    fn is_cacheable(col_id: u32) -> bool;

    /// Applies the appropriate cache update policy for a given column.
    fn apply_cache_update_policy(
        col_id: u32,
        op: &Box<dyn GenericWriteOperation<Self>>,
        cache_any: &Arc<dyn Any + Send + Sync>,
    ) where
        Self: Sized;
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum VersionedKVName {
    FlatKV,
    AmtNode,
    SlotAllocation,
}

use std::{any::Any, sync::Arc};

use lru::LruCache;
use parking_lot::Mutex;
// Use a `use` statement to simplify subsequent code.
use VersionedKVName::*;

use crate::{lvmt::table_schema::{AmtNodes, FlatKeyValue}, middlewares::{table_schema::{HistoryIndicesTable, VersionedKeyValueSchema}, CommitIDSchema, HistoryIndexKey}};

use super::{write_schema::GenericWriteOperation, TableSchema};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum HistoricalTableName {
    CommitID,
    HistoryNumber,
    HistoryChange(VersionedKVName),
    HistoryIndex(VersionedKVName),
}

type AmtHistoryKey = HistoryIndexKey<<AmtNodes as VersionedKeyValueSchema>::Key>;

impl TableNameTrait for HistoricalTableName {
    fn num_tables() -> u32 {
        8
    }

    fn get_cache_size_for_col(cache_any: &dyn Any, col_id: u32) -> String {
        match col_id {
            id if id == HistoricalTableName::CommitID.into() => cache_any.downcast_ref::<Mutex<LruCache<Box<<CommitIDSchema as TableSchema>::Key>, Option<Box<<CommitIDSchema as TableSchema>::Value>>>>>().map(|c| c.lock().len()).unwrap_or_default().to_string(),
            id if id == HistoricalTableName::HistoryIndex(FlatKV).into() => cache_any.downcast_ref::<Mutex<LruCache<Box<<HistoryIndicesTable<FlatKeyValue> as TableSchema>::Key>, Option<Box<<HistoryIndicesTable<FlatKeyValue> as TableSchema>::Value>>>>>().map(|c| c.lock().len()).unwrap_or_default().to_string(),
            id if id == HistoricalTableName::HistoryIndex(AmtNode).into() => cache_any.downcast_ref::<Mutex<LruCache<Box<<HistoryIndicesTable<AmtNodes> as TableSchema>::Key>, Option<Box<<HistoryIndicesTable<AmtNodes> as TableSchema>::Value>>>>>().map(|c| c.lock().len()).unwrap_or_default().to_string(),
            _ => "N/A".to_string(), 
        }
    }

    fn is_cacheable(col_id: u32) -> bool {
        matches!(
            HistoricalTableName::try_from(col_id),
            Ok(HistoricalTableName::CommitID) | Ok(HistoricalTableName::HistoryIndex(FlatKV)) | Ok(HistoricalTableName::HistoryIndex(AmtNode))
        )
    }

    fn apply_cache_update_policy(
        col_id: u32,
        op: &Box<dyn GenericWriteOperation<Self>>,
        cache_any: &Arc<dyn Any + Send + Sync>,
    ) where
        Self: Sized
    {
        let amt_history_index_col_id: u32 = HistoricalTableName::HistoryIndex(crate::backends::VersionedKVName::AmtNode).into();
        
        if col_id == amt_history_index_col_id {
            if let Some(structured_key) = op.structured_key_any().downcast_ref::<Box<AmtHistoryKey>>() {
                if structured_key.is_latest() {
                    op.apply_to_cache(cache_any);//, metrics);
                } else {
                    op.invalidate_in_cache(cache_any);//, metrics);
                }
            } else {
                unreachable!("Type mismatch for AmtNode HistoryIndex key. Expected Box<HistoryIndexKey<<AmtNodes as VersionedKeyValueSchema>::Key>>");
            }
        } else {
            op.invalidate_in_cache(cache_any);//, metrics);
            // ()
        }
    }
}

impl From<HistoricalTableName> for u32 {
    fn from(t: HistoricalTableName) -> Self {
        use HistoricalTableName::*;
        match t {
            // The index starts from 0 to align with RocksDB's column indices.
            CommitID => 0,
            HistoryNumber => 1,
            HistoryChange(FlatKV) => 2,
            HistoryIndex(FlatKV) => 3,
            HistoryChange(AmtNode) => 4,
            HistoryIndex(AmtNode) => 5,
            HistoryChange(SlotAllocation) => 6,
            HistoryIndex(SlotAllocation) => 7,
        }
    }
}

impl TryFrom<u32> for HistoricalTableName {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        use HistoricalTableName::*;
        match value {
            0 => Ok(CommitID),
            1 => Ok(HistoryNumber),
            2 => Ok(HistoryChange(FlatKV)),
            3 => Ok(HistoryIndex(FlatKV)),
            4 => Ok(HistoryChange(AmtNode)),
            5 => Ok(HistoryIndex(AmtNode)),
            6 => Ok(HistoryChange(SlotAllocation)),
            7 => Ok(HistoryIndex(SlotAllocation)),
            _ => Err(()),
        }
    }
}

impl From<HistoricalTableName> for &'static str {
    fn from(t: HistoricalTableName) -> Self {
        use HistoricalTableName::*;
        match t {
            CommitID => "commit_id",
            HistoryNumber => "history_number",
            HistoryChange(FlatKV) => "flat_kv_change_history",
            HistoryIndex(FlatKV) => "flat_kv_history_index",
            HistoryChange(AmtNode) => "amt_node_change_history",
            HistoryIndex(AmtNode) => "amt_node_history_index",
            HistoryChange(SlotAllocation) => "slot_alloc_change_history",
            HistoryIndex(SlotAllocation) => "slot_alloc_history_index",
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum PendingTableName {
    Snapshots(VersionedKVName),
    Wal(VersionedKVName),
    AuthNodeChange,
    StateRoot,
}

impl TableNameTrait for PendingTableName {
    fn num_tables() -> u32 {
        8
    }

    fn get_cache_size_for_col(_cache_any: &dyn Any, _col_id: u32) -> String {
        "N/A".to_string()
    }

    fn is_cacheable(_col_id: u32) -> bool {
        false
    }

    fn apply_cache_update_policy(
        _col_id: u32,
        _op: &Box<dyn GenericWriteOperation<Self>>,
        _cache_any: &Arc<dyn Any + Send + Sync>,
    ) where
        Self: Sized
    {
    }
}

impl From<PendingTableName> for u32 {
    fn from(t: PendingTableName) -> Self {
        use PendingTableName::*;
        match t {
            Snapshots(FlatKV) => 0,
            Wal(FlatKV) => 1,
            Snapshots(AmtNode) => 2,
            Wal(AmtNode) => 3,
            Snapshots(SlotAllocation) => 4,
            Wal(SlotAllocation) => 5,
            AuthNodeChange => 6,
            StateRoot => 7,
        }
    }
}

impl TryFrom<u32> for PendingTableName {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        use PendingTableName::*;
        match value {
            0 => Ok(Snapshots(FlatKV)),
            1 => Ok(Wal(FlatKV)),
            2 => Ok(Snapshots(AmtNode)),
            3 => Ok(Wal(AmtNode)),
            4 => Ok(Snapshots(SlotAllocation)),
            5 => Ok(Wal(SlotAllocation)),
            6 => Ok(AuthNodeChange),
            7 => Ok(StateRoot),
            _ => Err(()),
        }
    }
}

impl From<PendingTableName> for &'static str {
    fn from(t: PendingTableName) -> Self {
        use PendingTableName::*;
        match t {
            Snapshots(FlatKV) => "flat_kv_pending_snapshots",
            Wal(FlatKV) => "flat_kv_pending_wal",
            Snapshots(AmtNode) => "amt_node_pending_snapshots",
            Wal(AmtNode) => "amt_node_pending_wal",
            Snapshots(SlotAllocation) => "slot_alloc_pending_snapshots",
            Wal(SlotAllocation) => "slot_alloc_pending_wal",
            AuthNodeChange => "auth_node_change",
            StateRoot => "state_root",
        }
    }
}

#[cfg(test)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MockTableName {
    MockTable1,
    MockTable2,
    MockTable3,
}

#[cfg(test)]
impl TableNameTrait for MockTableName {
    fn num_tables() -> u32 {
        3
    }

    fn get_cache_size_for_col(_cache_any: &dyn Any, _col_id: u32) -> String {
        "N/A".to_string()
    }

    fn is_cacheable(_col_id: u32) -> bool {
        false
    }

    fn apply_cache_update_policy(
        _col_id: u32,
        _op: &Box<dyn GenericWriteOperation<Self>>,
        _cache_any: &Arc<dyn Any + Send + Sync>,
    ) where
        Self: Sized
    {
    }
}

#[cfg(test)]
impl From<MockTableName> for u32 {
    fn from(t: MockTableName) -> Self {
        use MockTableName::*;
        match t {
            MockTable1 => 0,
            MockTable2 => 1,
            MockTable3 => 2,
        }
    }
}

#[cfg(test)]
impl TryFrom<u32> for MockTableName {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        use MockTableName::*;
        match value {
            0 => Ok(MockTable1),
            1 => Ok(MockTable2),
            2 => Ok(MockTable3),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
impl From<MockTableName> for &'static str {
    fn from(t: MockTableName) -> Self {
        use MockTableName::*;
        match t {
            MockTable1 => "mock_table1",
            MockTable2 => "mock_table2",
            MockTable3 => "mock_table3",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_historical_is_cacheable_exhaustive() {
        for i in 0..HistoricalTableName::num_tables() {
            let table_name = HistoricalTableName::try_from(i).unwrap();
            
            let actual_is_cacheable = HistoricalTableName::is_cacheable(i);

            let expected_is_cacheable = match table_name {
                HistoricalTableName::CommitID => true,
                HistoricalTableName::HistoryIndex(FlatKV) => true,
                HistoricalTableName::HistoryIndex(AmtNode) => true,
                _ => false,
            };

            assert_eq!(
                actual_is_cacheable,
                expected_is_cacheable,
                "is_cacheable mismatch for HistoricalTableName::{:?} (col_id {})",
                table_name,
                i
            );
        }

        // Test invalid col_id
        assert_eq!(
            HistoricalTableName::is_cacheable(99),
            false,
            "is_cacheable should return false for an invalid col_id"
        );
    }

    #[test]
    fn test_pending_is_cacheable_exhaustive() {
        for i in 0..PendingTableName::num_tables() {
            let table_name = PendingTableName::try_from(i).unwrap();
            
            assert_eq!(
                PendingTableName::is_cacheable(i),
                false,
                "is_cacheable should be false for all PendingTableName variants, but was true for {:?} (col_id {})",
                table_name,
                i
            );
        }

        // Test invalid col_id
        assert_eq!(
            PendingTableName::is_cacheable(99),
            false,
            "is_cacheable should return false for an invalid col_id on PendingTableName"
        );
    }

    #[test]
    fn test_mock_is_cacheable_exhaustive() {
        // Test valid col_id
        for i in 0..MockTableName::num_tables() {
            let table_name = MockTableName::try_from(i).unwrap();

            assert_eq!(
                MockTableName::is_cacheable(i),
                false,
                "is_cacheable should be false for all MockTableName variants, but was true for {:?} (col_id {})",
                table_name,
                i
            );
        }

        // Test invalid col_id
        assert_eq!(
            MockTableName::is_cacheable(99),
            false,
            "is_cacheable should return false for an invalid col_id on MockTableName"
        );
    }
}