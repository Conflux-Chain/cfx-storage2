#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TableName {
    CommitID,
    ChangeHistory(VersionedKVName),
    HistoryIndex(VersionedKVName),
    #[cfg(test)]
    MockTable,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum VersionedKVName {
    FlatKV,
}

pub const fn change_history(versioned_kv: VersionedKVName) -> TableName {
    ChangeHistory(versioned_kv)
}

pub const fn history_index(versioned_kv: VersionedKVName) -> TableName {
    HistoryIndex(versioned_kv)
}

use TableName::*;
use VersionedKVName::*;

impl From<TableName> for u32 {
    fn from(t: TableName) -> Self {
        let mut id = 0u32;
        let mut make_id = || {
            let ans = id;
            id += 1;
            ans
        };
        match t {
            CommitID => make_id(),
            ChangeHistory(FlatKV) => make_id(),
            HistoryIndex(FlatKV) => make_id(),
            #[cfg(test)]
            MockTable => u32::MAX,
        }
    }
}

impl From<TableName> for &'static str {
    fn from(t: TableName) -> Self {
        match t {
            CommitID => "commit_id",
            ChangeHistory(FlatKV) => "flat_kv_change_history",
            HistoryIndex(FlatKV) => "flat_kv_history_index",
            #[cfg(test)]
            MockTable => "mock_table",
        }
    }
}
