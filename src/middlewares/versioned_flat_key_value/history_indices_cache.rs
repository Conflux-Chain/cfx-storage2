use std::{borrow::Cow, collections::{hash_map::Entry, HashMap}};

use crate::{backends::TableRead, errors::Result, middlewares::HistoryNumber};

use super::{history_indices_table::{HistoryIndices, OneRange, LATEST, ONE_RANGE_BYTES}, table_schema::{HistoryIndicesTable, VersionedKeyValueSchema}, HistoryIndexKey};

pub struct HistoryIndexCache<T: VersionedKeyValueSchema> {
    // Maps each key to its pending state
    cache: HashMap<T::Key, KeyCacheEntry<T::Value>>,
}

struct KeyCacheEntry<V> {
    latest: LatestEntry<V>,
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

    pub fn insert(&mut self, k: T::Key, v: Option<T::Value>, version_number: HistoryNumber, db: &impl TableRead<HistoryIndicesTable<T>>) -> Result<()> {
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
                                version_numbers: OneRange::Two(Vec::new()),
                                latest_v: v,
                            },
                            previous_entries: Vec::new(),
                        });
                        return Ok(())
                    }
                }
            }
        };

        let latest = &mut entry.latest;
        assert!(version_number > latest.start_version);

        // Calculate offset_minus_1
        let offset_minus_1 = version_number - latest.start_version - 1;

        // Try to add to existing OneRange
        let added = Self::try_add_to_range(&mut latest.version_numbers, offset_minus_1);
        if added {
            latest.latest_v = v;
        } else {
            // Split current latest into previous entry
            let (end_version, prev_range) = Self::convert_to_previous(&latest.version_numbers, latest.start_version);
            entry.previous_entries.push((end_version, prev_range));

            // Create new latest entry
            let new_start = end_version;
            let new_offset = version_number - new_start - 1;
            let new_range = match new_offset {
                o if o < 1 << 16 => OneRange::Two(vec![new_offset as u16]),
                o if o < 1 << 32 => OneRange::Four(vec![new_offset as u32]),
                o => OneRange::OnlyEnd(o),
            };

            entry.latest = LatestEntry {
                start_version: new_start,
                version_numbers: new_range,
                latest_v: v,
            };
        }

        return Ok(())
    }

    fn try_add_to_range(range: &mut OneRange, offset_minus_1: HistoryNumber) -> bool {
        match range {
            OneRange::OnlyEnd(existing_offset) => {
                // Can't add to OnlyEnd; must split
                false
            }
            OneRange::Four(vec) => {
                let max_offset = 1 << 32;
                if offset_minus_1 >= max_offset {
                    return false;
                }
                let new_count = vec.len() + 1;
                if new_count > ONE_RANGE_BYTES / 4 {
                    false
                } else {
                    vec.push(offset_minus_1 as u32);
                    true
                }
            }
            OneRange::Two(vec) => {
                let max_offset = 1 << 16;
                if offset_minus_1 >= max_offset {
                    return false;
                }
                let new_count = vec.len() + 1;
                if new_count > ONE_RANGE_BYTES / 2 {
                    false
                } else {
                    vec.push(offset_minus_1 as u16);
                    true
                }
            }
            OneRange::Bitmap(bits) => {
                if offset_minus_1 >= ONE_RANGE_BYTES as u64 * 8 {
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

    fn convert_to_previous(range: &OneRange, start_version: HistoryNumber) -> (HistoryNumber, OneRange) {
        match range {
            OneRange::OnlyEnd(offset) => (start_version + offset + 1, range.clone()),
            OneRange::Four(vec) => {
                let max_offset = vec.last().copied().unwrap_or(0) as u64;
                (start_version + max_offset + 1, range.clone())
            }
            OneRange::Two(vec) => {
                let max_offset = vec.last().copied().unwrap_or(0) as u64;
                (start_version + max_offset + 1, range.clone())
            }
            OneRange::Bitmap(bits) => {
                let mut max_offset = 0;
                for (byte_idx, byte) in bits.iter().enumerate() {
                    for bit in 0..8 {
                        if (byte & (1 << bit)) != 0 {
                            let idx = byte_idx * 8 + bit;
                            max_offset = max_offset.max(idx as u64);
                        }
                    }
                }
                (start_version + max_offset + 1, range.clone())
            }
        }
    }

    // Convert cache entries into write batch
    pub fn into_write_batch(self) -> Vec<(Cow<'static, HistoryIndexKey<T::Key>>, Option<Cow<'static, HistoryIndices<T::Value>>>)> {
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
                Some(Cow::Owned(HistoryIndices::Latest((latest.start_version, latest.version_numbers, latest.latest_v)))),
            ));
        }
        batch
    }
}