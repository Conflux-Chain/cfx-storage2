use ethereum_types::H256;

use crate::backends::{TableName, TableSchema};

pub type CommitID = H256;
pub type HistoryNumber = u64;

pub fn encode_history_number_rev(input: u64) -> [u8; 8] {
    (!input).to_be_bytes()
}
pub fn decode_history_number_rev(input: &[u8]) -> u64 {
    !u64::from_be_bytes(input.try_into().unwrap())
}

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
pub fn height_to_history_number(height: usize) -> HistoryNumber {
    height as u64 + 1
}

/// Converts a `history_number` back to a `height`.
#[cfg(test)]
pub fn history_number_to_height(history_number: HistoryNumber) -> usize {
    history_number as usize - 1
}
