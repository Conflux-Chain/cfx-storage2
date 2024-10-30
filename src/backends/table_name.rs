#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TableName {
    CommitID,
    HistoryNumber,
    HistoryChange(VersionedKVName),
    HistoryIndex(VersionedKVName),
    #[cfg(test)]
    MockTable,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum VersionedKVName {
    FlatKV,
}

pub const fn change_history(versioned_kv: VersionedKVName) -> TableName {
    HistoryChange(versioned_kv)
}

pub const fn history_index(versioned_kv: VersionedKVName) -> TableName {
    HistoryIndex(versioned_kv)
}

use TableName::*;
use VersionedKVName::*;

impl From<TableName> for u32 {
    fn from(t: TableName) -> Self {
        match t {
            CommitID => 1,
            HistoryNumber => 2,
            HistoryChange(FlatKV) => 3,
            HistoryIndex(FlatKV) => 4,
            #[cfg(test)]
            MockTable => u32::MAX,
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
            #[cfg(test)]
            MockTable => "mock_table",
        }
    }
}
