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
