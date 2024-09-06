mod table_schema;

use in_memory_tree::VersionedHashMap;
use std::hash::Hash;

use self::table_schema::{ChangeHistorySchema, HistoryIndicesSchema, VersionedKeyValueSchema};
use crate::backends::TableReader;
use crate::errors::Result;
use crate::middlewares::{CommitID, HistoryNumber, KeyValueBulks};
use crate::traits::KeyValueStoreBulks;
use crate::StorageError;

use super::CommitIDSchema;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct KeyHistory<K: Clone>(K, HistoryNumber);

#[derive(Clone, Debug)]
pub struct HistoryIndices;
impl HistoryIndices {
    fn last(&self, max: HistoryNumber) -> HistoryNumber {
        todo!()
    }
}

// struct PendingPart;

pub struct VersionedStore<'db, T: VersionedKeyValueSchema> where T::Key: Hash {
    pending_part: VersionedHashMap<T::Key, CommitID, T::Value>,
    history_index_table: TableReader<'db, HistoryIndicesSchema<T>>,
    commit_id_table: TableReader<'db, CommitIDSchema>,
    change_history_table: KeyValueBulks<'db, ChangeHistorySchema<T>>,
}

impl<'db, T: VersionedKeyValueSchema> VersionedStore<'db, T> where T::Key: Hash  {
    pub fn get_pending_part(&self, commit: CommitID, key: &T::Key) -> Result<Option<T::Value>> {
        todo!()
    }

    pub fn get_historical_part(&self, commit: CommitID, key: &T::Key) -> Result<Option<T::Value>> {
        let target_history_number = if let Some(value) = self.commit_id_table.get(&commit)? {
            value.into_owned()
        } else {
            return Err(StorageError::CommitIDNotFound);
        };

        let range_query_key = KeyHistory(key.clone(), target_history_number);

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
                let KeyHistory(_, history_number) = k.as_ref();
                let offset = target_history_number - history_number;
                indices.as_ref().last(*history_number)
            }
        };

        self.change_history_table
            .get_versioned_key(&found_version_number, key)
    }
}
