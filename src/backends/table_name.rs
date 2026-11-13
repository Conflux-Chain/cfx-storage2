pub trait TableNameTrait: Send + Sync + 'static + Copy + Into<u32> + Into<&'static str> {
    /// Returns the total number of Column Families in the Historical DB.
    /// This value can be used directly to configure a RocksDB instance.
    fn num_tables() -> u32;
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum VersionedKVName {
    FlatKV,
    AmtNode,
    SlotAllocation,
}

// Use a `use` statement to simplify subsequent code.
use VersionedKVName::*;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum HistoricalTableName {
    CommitID,
    HistoryNumber,
    HistoryChange(VersionedKVName),
    HistoryIndex(VersionedKVName),
    AuthNodeChange,
    StateRoot,
}

impl TableNameTrait for HistoricalTableName {
    fn num_tables() -> u32 {
        10
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
            AuthNodeChange => 8,
            StateRoot => 9,
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
            AuthNodeChange => "auth_node_change",
            StateRoot => "state_root",
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum PendingTableName {
    Snapshots(VersionedKVName),
    Wal(VersionedKVName),
}

impl TableNameTrait for PendingTableName {
    fn num_tables() -> u32 {
        6
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
