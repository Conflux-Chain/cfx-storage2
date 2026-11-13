use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap},
};

use crate::{backends::TableRead, errors::Result, middlewares::HistoryNumber};

use super::{
    history_indices::{HistoryIndices, PreviousRecord, LATEST},
    table_schema::{HistoryIndicesTable, VersionedKeyValueSchema},
    HistoryIndexKey,
};

pub struct HistoryIndexCache<T: VersionedKeyValueSchema> {
    // Maps each key to its pending state
    cache: HashMap<T::Key, KeyCacheEntry<T::Value>>,
}

struct KeyCacheEntry<V: Clone> {
    latest: HistoryIndices<V>,
    previous_entries: Vec<PreviousRecord>,
}

impl<T: VersionedKeyValueSchema> HistoryIndexCache<T> {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    pub fn insert(
        &mut self,
        k: T::Key,
        v: Option<T::Value>,
        version_number: HistoryNumber,
        db: &impl TableRead<HistoryIndicesTable<T>>,
    ) -> Result<()> {
        let entry = match self.cache.entry(k) {
            Entry::Occupied(occupied_entry) => occupied_entry.into_mut(),
            Entry::Vacant(vacant_entry) => {
                let k_latest = HistoryIndexKey(vacant_entry.key().clone(), LATEST);
                match db.get(&k_latest)? {
                    Some(db_latest) => {
                        let latest = db_latest.into_owned();
                        vacant_entry.insert(KeyCacheEntry {
                            latest,
                            previous_entries: Vec::new(),
                        })
                    }
                    None => {
                        vacant_entry.insert(KeyCacheEntry {
                            latest: HistoryIndices::new(version_number, v),
                            previous_entries: Vec::new(),
                        });
                        return Ok(());
                    }
                }
            }
        };

        let latest = &mut entry.latest;

        let maybe_new_previous = latest.push(version_number, v)?;
        if let Some(new_previous) = maybe_new_previous {
            entry.previous_entries.push(new_previous);
        }

        Ok(())
    }

    // Convert cache entries into write batch
    #[allow(clippy::type_complexity)]
    pub fn into_write_batch(
        self,
    ) -> Vec<(
        Cow<'static, HistoryIndexKey<T::Key>>,
        Option<Cow<'static, HistoryIndices<T::Value>>>,
    )> {
        let mut batch = Vec::new();
        for (k, entry) in self.cache {
            // Add previous entries
            for PreviousRecord {
                end_version_number,
                range_encoding,
            } in entry.previous_entries
            {
                batch.push((
                    Cow::Owned(HistoryIndexKey(k.clone(), end_version_number)),
                    Some(Cow::Owned(HistoryIndices::Previous(range_encoding))),
                ));
            }

            // Add latest entry
            let latest = entry.latest;
            batch.push((
                Cow::Owned(HistoryIndexKey(k, LATEST)),
                Some(Cow::Owned(latest)),
            ));
        }
        batch
    }
}
