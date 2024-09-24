mod pending_part;
mod serde;
mod table_schema;

use std::collections::BTreeMap;

pub use pending_part::PendingError;

use self::pending_part::pending_schema::PendingKeyValueConfig;
use self::table_schema::{HistoryChangeTable, HistoryIndicesTable, VersionedKeyValueSchema};
use pending_part::VersionedHashMap;

use super::ChangeKey;
use super::CommitIDSchema;
use crate::backends::{TableReader, WriteSchemaTrait};
use crate::errors::Result;
use crate::middlewares::{CommitID, HistoryNumber, KeyValueStoreBulks};
use crate::traits::KeyValueStoreBulksTrait;
use crate::StorageError;

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

pub struct VersionedStore<'db, T: VersionedKeyValueSchema> {
    pending_part: VersionedHashMap<PendingKeyValueConfig<T, CommitID>>,
    history_index_table: TableReader<'db, HistoryIndicesTable<T>>,
    commit_id_table: TableReader<'db, CommitIDSchema>,
    change_history_table: KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
}

impl<'db, T: VersionedKeyValueSchema> VersionedStore<'db, T> {
    pub fn get_pending_part(&mut self, commit: CommitID, key: &T::Key) -> Result<Option<T::Value>> {
        let res_value = self.pending_part.query(commit, key);
        let history_commit = match res_value {
            Ok(Some(value)) => return Ok(value),
            Ok(None) => self.pending_part.get_parent_of_root(),
            Err(PendingError::CommitIDNotFound(target_commit)) => {
                assert_eq!(target_commit, commit);
                Some(commit)
            }
            Err(other_err) => return Err(StorageError::PendingError(other_err)),
        };
        if let Some(history_commit) = history_commit {
            self.get_historical_part(history_commit, key)
        } else {
            Ok(None)
        }
    }

    pub fn add_to_pending_part(
        &mut self,
        parent_commit: Option<CommitID>,
        commit: CommitID,
        updates: BTreeMap<T::Key, Option<T::Value>>,
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

        // history_number should be decreasing, use !height
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
                // let offset = target_history_number - history_number;
                indices.as_ref().last(*history_number)
            }
        };

        self.change_history_table
            .get_versioned_key(&found_version_number, key)
    }
}

impl<'db, T: VersionedKeyValueSchema> VersionedStore<'db, T> {
    fn confirmed_pending_to_history(
        &mut self,
        new_root_commit_id: CommitID,
        write_schema: &impl WriteSchemaTrait,
    ) -> Result<()> {
        // old root..=new root's parent
        let (start_height, confirmed_ids_maps) =
            self.pending_part.change_root(new_root_commit_id)?;
        for (delta_height, (confirmed_commit_id, updates)) in
            confirmed_ids_maps.into_iter().enumerate()
        {
            let height = (start_height + delta_height) as u64;
            let history_number = !height;
            // self.commit_id_table
            //     .set(confirmed_commit_id, history_number);
            self.change_history_table.commit(
                history_number as u64,
                updates.into_iter(),
                write_schema,
            )?;
            // self.history_index_table;
        }
        Ok(())
    }
}
