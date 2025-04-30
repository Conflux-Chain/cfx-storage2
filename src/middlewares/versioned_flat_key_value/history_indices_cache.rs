use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap},
};

use crate::{backends::TableRead, errors::Result, middlewares::HistoryNumber};

use super::{
    history_indices::{
        HistoryIndices, OneRange, BITMAP_MAX_INDEX, LATEST, MAX_FOUR_ENTRIES, MAX_TWO_ENTRIES,
    },
    table_schema::{HistoryIndicesTable, VersionedKeyValueSchema},
    HistoryIndexKey,
};

pub struct HistoryIndexCache<T: VersionedKeyValueSchema> {
    // Maps each key to its pending state
    cache: HashMap<T::Key, KeyCacheEntry<T::Value>>,
}

/// Represents all pending changes for a key that will be persisted to the database.
///
/// During batch updates, modifications are first accumulated in the latest record. When capacity is
/// exceeded, the latest record splits into a non-latest record (added to previous_entries) and a new
/// latest record containing remaining versions.
struct KeyCacheEntry<V> {
    /// Represents the current active version range to be written as a `HistoryIndices::Latest` record.
    /// Contains the start version, `OneRange`, and the latest value (or deletion marker).
    /// This entry may be modified directly or split into non-latest records during compaction.
    latest: LatestEntry<V>,
    /// Non-latest version ranges split from the original latest record. Each will be written as a
    /// `HistoryIndices::Previous(OneRange)` record, where the tuple's `HistoryNumber`
    /// becomes the `end_version_number` in the database key `HistoryIndexKey(key, end_version_number)`.
    ///
    /// Maintained in version-ascending order - newer splits are pushed directly to the end. The actual `OneRange` data
    /// must always be non-empty as per `HistoryIndices::Previous` requirements.
    previous_entries: Vec<(HistoryNumber, OneRange)>,
}

struct LatestEntry<V> {
    start_version: HistoryNumber,
    version_numbers: OneRange,
    latest_v: Option<V>,
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
                        let latest = match db_latest.into_owned() {
                            HistoryIndices::Latest((start_version, version_numbers, latest_v)) => {
                                LatestEntry {
                                    start_version,
                                    version_numbers,
                                    latest_v,
                                }
                            }
                            HistoryIndices::Previous(_) => unreachable!(),
                        };
                        vacant_entry.insert(KeyCacheEntry {
                            latest,
                            previous_entries: Vec::new(),
                        })
                    }
                    None => {
                        vacant_entry.insert(KeyCacheEntry {
                            latest: LatestEntry {
                                start_version: version_number,
                                version_numbers: OneRange::new(),
                                latest_v: v,
                            },
                            previous_entries: Vec::new(),
                        });
                        return Ok(());
                    }
                }
            }
        };

        let latest = &mut entry.latest;
        let end_version = latest.start_version + latest.version_numbers.max_offset();
        // Important assertion: the version numbers added sequentially should be strictly increasing.
        assert!(version_number > end_version);

        // Safety of subtraction: from the previous assertion, we can infer version_number > latest.start_version.
        let offset_minus_1 = version_number - latest.start_version - 1;

        // Try to add to existing OneRange
        let added = Self::try_add_to_range(&mut latest.version_numbers, offset_minus_1);
        if added {
            latest.latest_v = v;
        } else {
            // Split current latest into previous entry
            entry
                .previous_entries
                .push((end_version, latest.version_numbers.clone()));

            // Create new latest entry
            let new_start = end_version;
            // Safety of subtraction: from the previous assertion: assert!(version_number > end_version);
            let new_offset_minus_1 = version_number - new_start - 1;
            let new_range = OneRange::new_with_offset_minus_1(new_offset_minus_1);

            entry.latest = LatestEntry {
                start_version: new_start,
                version_numbers: new_range,
                latest_v: v,
            };
        }

        Ok(())
    }

    fn try_add_to_range(range: &mut OneRange, offset_minus_1: HistoryNumber) -> bool {
        match range {
            OneRange::OnlyEnd(existing_offset) => {
                // Can't add to OnlyEnd; must split
                false
            }
            OneRange::Four(vec) => {
                if offset_minus_1 > u32::MAX as u64 {
                    return false;
                }
                let new_count = vec.len() + 1;
                if new_count > MAX_FOUR_ENTRIES {
                    false
                } else {
                    vec.push(offset_minus_1 as u32);
                    true
                }
            }
            OneRange::Two(vec) => {
                if offset_minus_1 > u16::MAX as u64 {
                    return false;
                }
                let new_count = vec.len() + 1;
                if new_count > MAX_TWO_ENTRIES {
                    false
                } else {
                    vec.push(offset_minus_1 as u16);
                    true
                }
            }
            OneRange::Bitmap(bits) => {
                if offset_minus_1 > BITMAP_MAX_INDEX {
                    return false;
                }
                let idx = offset_minus_1 as usize;
                let byte = idx / 8;
                let bit = idx % 8;
                bits[byte] |= 1 << bit;
                true
            }
        }
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
            for (end_version, range) in entry.previous_entries {
                batch.push((
                    Cow::Owned(HistoryIndexKey(k.clone(), end_version)),
                    Some(Cow::Owned(HistoryIndices::Previous(range))),
                ));
            }

            // Add latest entry
            let latest = entry.latest;
            batch.push((
                Cow::Owned(HistoryIndexKey(k, LATEST)),
                Some(Cow::Owned(HistoryIndices::Latest((
                    latest.start_version,
                    latest.version_numbers,
                    latest.latest_v,
                )))),
            ));
        }
        batch
    }
}
