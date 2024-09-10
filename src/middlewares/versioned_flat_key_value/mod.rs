mod table_schema;
mod serde;

use self::table_schema::{HistoryChangeTable, HistoryIndicesTable, VersionedKeyValueSchema};
use crate::backends::TableReader;
use crate::errors::Result;
use crate::middlewares::{CommitID, HistoryNumber, KeyValueStoreBulks};
use crate::traits::KeyValueStoreBulksTrait;
use crate::StorageError;
use super::ChangeKey;
use super::CommitIDSchema;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct HistoryIndexKey<K: Clone>(K, HistoryNumber);

pub type HistoryChangeKey<K> = ChangeKey<HistoryNumber, K>;

#[derive(Clone, Debug)]
pub struct HistoryIndices;
impl HistoryIndices {
    fn last(&self, offset: HistoryNumber) -> HistoryNumber {
        offset
    }
}

struct PendingPart;

pub struct VersionedStore<'db, T: VersionedKeyValueSchema> {
    pending_part: PendingPart,
    history_index_table: TableReader<'db, HistoryIndicesTable<T>>,
    commit_id_table: TableReader<'db, CommitIDSchema>,
    change_history_table: KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
}

impl<'db, T: VersionedKeyValueSchema> VersionedStore<'db, T> {
    pub fn get_pending_part(&self, commit: CommitID, key: &T::Key) -> Result<Option<T::Value>> {
        todo!()
    }

    pub fn get_historical_part(&self, commit: CommitID, key: &T::Key) -> Result<Option<T::Value>> {
        let target_history_number = if let Some(value) = self.commit_id_table.get(&commit)? {
            value.into_owned()
        } else {
            return Err(StorageError::CommitIDNotFound);
        };

        let range_query_key = HistoryIndexKey(key.clone(), target_history_number);

        let found_version_number = match self.history_index_table.iter(&range_query_key)?.next() {
            None => {
                return Ok(None);
            }
            Some(Err(e)) => {
                return Err(e.into());
            }
            Some(Ok((k, _))) if &k.as_ref().0 != key => {
                return Ok(None);
            }
            Some(Ok((k, indices))) => {
                let HistoryIndexKey(_, history_number) = k.as_ref();
                let offset = target_history_number - history_number;
                indices.as_ref().last(offset)
            }
        };

        self.change_history_table
            .get_versioned_key(&found_version_number, key)
    }
}
