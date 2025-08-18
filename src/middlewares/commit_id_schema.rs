use ethereum_types::H256;

use crate::backends::{TableName, TableSchema};

pub type CommitID = H256;
pub type HistoryNumber = u64;

#[derive(Clone, Copy)]
pub struct CommitIDSchema;

impl TableSchema for CommitIDSchema {
    const NAME: TableName = TableName::CommitID;
    type Key = CommitID;
    type Value = HistoryNumber;
}

#[derive(Clone, Copy)]
pub struct HistoryNumberSchema;

impl TableSchema for HistoryNumberSchema {
    const NAME: TableName = TableName::HistoryNumber;
    type Key = HistoryNumber;
    type Value = CommitID;
}

/// Converts a `height` to a `history_number`.
#[inline]
pub fn height_to_history_number(height: u64) -> HistoryNumber {
    height
}

/// Converts a `history_number` back to a `height`.
pub fn history_number_to_height(history_number: HistoryNumber) -> u64 {
    history_number
}
