#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TableName {
    CommitID,
    HistoryNumber,
    HistoryChange(VersionedKVName),
    HistoryIndex(VersionedKVName),
    AuthNodeChange,
    StateRoot,
    #[cfg(test)]
    MockTable1,
    #[cfg(test)]
    MockTable2,
    #[cfg(test)]
    MockTable3,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum VersionedKVName {
    FlatKV,
    AmtNode,
    SlotAllocation,
}

pub const fn change_history(versioned_kv: VersionedKVName) -> TableName {
    HistoryChange(versioned_kv)
}

pub const fn history_index(versioned_kv: VersionedKVName) -> TableName {
    HistoryIndex(versioned_kv)
}

use TableName::*;
use VersionedKVName::*;

impl TableName {
    pub const fn max_index() -> u32 {
        10
    }
}

impl From<TableName> for u32 {
    fn from(t: TableName) -> Self {
        match t {
            CommitID => 1,
            HistoryNumber => 2,
            HistoryChange(FlatKV) => 3,
            HistoryIndex(FlatKV) => 4,
            HistoryChange(AmtNode) => 5,
            HistoryIndex(AmtNode) => 6,
            HistoryChange(SlotAllocation) => 7,
            HistoryIndex(SlotAllocation) => 8,
            AuthNodeChange => 9,
            StateRoot => 10,
            #[cfg(test)]
            MockTable1 => 1,
            #[cfg(test)]
            MockTable2 => 2,
            #[cfg(test)]
            MockTable3 => 3,
        }
    }
}

impl From<TableName> for &'static str {
    fn from(t: TableName) -> Self {
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
            #[cfg(test)]
            MockTable1 => "mock_table1",
            #[cfg(test)]
            MockTable2 => "mock_table2",
            #[cfg(test)]
            MockTable3 => "mock_table3",
        }
    }
}
