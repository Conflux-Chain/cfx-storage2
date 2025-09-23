use ethereum_types::H256;

use crate::{
    backends::{PendingTableName, TableSchema},
    middlewares::CommitID,
};

pub type StateRoot = H256;
#[derive(Clone, Copy)]
pub struct StateRootTable;
impl TableSchema for StateRootTable {
    type TableName = PendingTableName;
    const NAME: PendingTableName = PendingTableName::StateRoot;

    type Key = CommitID;
    type Value = StateRoot;
}
