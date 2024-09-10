mod table_schema;
mod serde;


use in_memory_tree::VersionedHashMap;
use std::hash::Hash;

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

// struct PendingPart;


pub struct VersionedStore<'db, T: VersionedKeyValueSchema> where T::Key: Hash {
    pending_part: VersionedHashMap<T::Key, CommitID, T::Value>,
    history_index_table: TableReader<'db, HistoryIndicesTable<T>>,
    commit_id_table: TableReader<'db, CommitIDSchema>,
    change_history_table: KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
}

impl<'db, T: VersionedKeyValueSchema> VersionedStore<'db, T> where T::Key: Hash  {
    pub fn get_pending_part(&mut self, commit: CommitID, key: &T::Key) -> Result<Option<T::Value>> {
        let res_value = self.pending_part.query(commit, key);
        let history_commit = match res_value {
            Ok(None) => { self.pending_part.get_parent_of_root() },
            Err(in_memory_tree::PendingError::CommitIDNotFound(target_commit)) if target_commit == commit
                => { Some(commit) },
            Ok(Some(value)) => { return Ok(value) },
            Err(e) => { return Err(StorageError::PendingError(e)) }
        };
        if let Some(history_commit) = history_commit {
            self.get_historical_part(history_commit, key)
        } else {
            Ok(None)
        }
    }

    pub fn add_to_pending_part(&mut self, parent_commit: Option<CommitID>, commit: CommitID, 
        updates: Vec::<(T::Key, Option<T::Value>)>,
    ) -> Result<()> {
        Ok(self.pending_part.add_node(updates, commit, parent_commit)?)
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
