use core::panic;
use std::{
    any::Any,
    borrow::{Borrow, Cow},
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use super::super::{
    serde::{Decode, Encode},
    table::TableSchema,
    DatabaseTrait, TableIter, TableRead,
};
use crate::{
    backends::{write_schema::HybridWriteSchema, TableName},
    errors::{DatabaseError, Result},
    lvmt::{AmtNodes, FlatKeyValue},
    middlewares::{table_schema::HistoryIndicesTable, CommitIDSchema},
};

use kvdb::KeyValueDB;
use kvdb_rocksdb::DatabaseConfig;
use lru::LruCache;
use parking_lot::Mutex;

// A simple struct to hold cache metrics for a single table.
// Using AtomicU64 allows us to modify them from multiple threads without locks.
#[derive(Default, Debug)]
pub struct CacheMetrics {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub puts: AtomicU64,
    pub evictions: AtomicU64,
    pub pops: AtomicU64,
    pub not_pops: AtomicU64,
}

pub struct CachedDB {
    db: Arc<kvdb_rocksdb::Database>,
    caches: Mutex<HashMap<u32, Arc<dyn Any + Send + Sync>>>,
    // metrics for each cache
    metrics: Mutex<HashMap<u32, Arc<CacheMetrics>>>,
    cached_tables: HashSet<u32>,
}

impl CachedDB {
    pub fn open(num_cols: u32, path: &str) -> Result<Self> {
        let db = open_database(num_cols, path)?;

        let cached_tables = HashSet::from_iter(
            vec![
                TableName::CommitID.into(),
                TableName::HistoryIndex(crate::backends::VersionedKVName::FlatKV).into(),
                TableName::HistoryIndex(crate::backends::VersionedKVName::AmtNode).into(),
            ]
            .into_iter(),
        );

        Ok(Self {
            db: Arc::new(db),
            caches: Mutex::new(HashMap::new()),
            metrics: Mutex::new(HashMap::new()),
            cached_tables,
        })
    }

    pub fn clear_all_caches(&self) {
        self.caches.lock().clear();
        // dbg!("All caches have been cleared.");
    }

    pub fn print_cache_stats(&self) {
        const TABLE_WIDTH: usize = 125;

        println!("{:-<width$}", "", width = TABLE_WIDTH);
        println!(
            "{:^width$}",
            "Cache Performance Statistics",
            width = TABLE_WIDTH
        );
        println!("{:-<width$}", "", width = TABLE_WIDTH);
        println!(
            "{:<20} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10}",
            "Table Name",
            "Size",
            "Hit Rate",
            "Hits",
            "Misses",
            "Puts",
            "Evictions",
            "Pops",
            "Not Pops"
        );
        println!("{:-<width$}", "", width = TABLE_WIDTH);

        let caches_map = self.caches.lock();
        let metrics_map = self.metrics.lock();

        for (col_id, metrics) in metrics_map.iter() {
            let hits = metrics.hits.load(Ordering::Relaxed);
            let misses = metrics.misses.load(Ordering::Relaxed);
            let total = hits + misses;
            let hit_rate = if total == 0 {
                0.0
            } else {
                (hits as f64 / total as f64) * 100.0
            };

            let cache_size_str = if let Some(cache_any) = caches_map.get(col_id) {
                match col_id {
                    id if *id == TableName::CommitID.into() => cache_any.downcast_ref::<Mutex<LruCache<Box<<CommitIDSchema as TableSchema>::Key>, Option<Box<<CommitIDSchema as TableSchema>::Value>>>>>().map(|c| c.lock().len()).unwrap_or_default().to_string(),
                    id if *id == TableName::HistoryIndex(crate::backends::VersionedKVName::FlatKV).into() => cache_any.downcast_ref::<Mutex<LruCache<Box<<HistoryIndicesTable<FlatKeyValue> as TableSchema>::Key>, Option<Box<<HistoryIndicesTable<FlatKeyValue> as TableSchema>::Value>>>>>().map(|c| c.lock().len()).unwrap_or_default().to_string(),
                    id if *id == TableName::HistoryIndex(crate::backends::VersionedKVName::AmtNode).into() => cache_any.downcast_ref::<Mutex<LruCache<Box<<HistoryIndicesTable<AmtNodes> as TableSchema>::Key>, Option<Box<<HistoryIndicesTable<AmtNodes> as TableSchema>::Value>>>>>().map(|c| c.lock().len()).unwrap_or_default().to_string(),
                    _ => panic!("Uncached TableName"),
                }
            } else {
                "N/A".to_string()
            };

            let table_name = format!("Column({})", col_id);

            println!(
                "{:<20} | {:>10} | {:>9.2}% | {:>10} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10}",
                table_name,
                cache_size_str,
                hit_rate,
                hits,
                misses,
                metrics.puts.load(Ordering::Relaxed),
                metrics.evictions.load(Ordering::Relaxed),
                metrics.pops.load(Ordering::Relaxed),
                metrics.not_pops.load(Ordering::Relaxed),
            );
        }
        println!("{:-<width$}", "", width = TABLE_WIDTH);
    }
}

pub fn open_database(num_cols: u32, path: &str) -> Result<kvdb_rocksdb::Database> {
    let mut config = DatabaseConfig::with_columns(num_cols);
    let total_memory_budget = 8 * 1024;
    let column_memory_budget = total_memory_budget / num_cols as usize;
    let mut map = HashMap::new();
    for i in 0..num_cols {
        map.insert(i, column_memory_budget);
    }
    config.memory_budget = map;
    let db_path = PathBuf::from(path);
    Ok(kvdb_rocksdb::Database::open(&config, db_path)?)
}

pub struct UncachedRocksDBColumn<'a> {
    col: u32,
    inner: &'a kvdb_rocksdb::Database,
}

pub struct CachedRocksDBColumn<'a, T: TableSchema> {
    col: u32,
    inner: &'a kvdb_rocksdb::Database,
    cache: Arc<Mutex<LruCache<Box<T::Key>, Option<Box<T::Value>>>>>,
    metrics: Arc<CacheMetrics>,
}

impl<'b, T: TableSchema> TableRead<T> for UncachedRocksDBColumn<'b> {
    fn get(&self, key: &T::Key) -> Result<Option<Cow<T::Value>>> {
        if let Some(v) = KeyValueDB::get(self.inner, self.col, key.encode().borrow())? {
            let owned = <T::Value>::decode_owned(v)?;
            Ok(Some(Cow::Owned(owned)))
        } else {
            Ok(None)
        }
    }

    fn iter(&self, key: &T::Key) -> Result<TableIter<T>> {
        let iter = self
            .inner
            .iter_from(self.col, &key.encode())
            .map(|kv| match kv {
                Ok((k, v)) => Ok((
                    Cow::Owned(<T::Key>::decode_owned(k.to_vec())?),
                    Cow::Owned(<T::Value>::decode_owned(v)?),
                )),
                Err(e) => Err(DatabaseError::IoError(e)),
            });

        Ok(Box::new(iter))
    }

    fn iter_from_start(&self) -> Result<TableIter<T>> {
        let iter = self.inner.iter(self.col).map(|kv| match kv {
            Ok((k, v)) => Ok((
                Cow::Owned(<T::Key>::decode_owned(k.into_vec())?),
                Cow::Owned(<T::Value>::decode_owned(v)?),
            )),
            Err(e) => Err(DatabaseError::IoError(e)),
        });

        Ok(Box::new(iter))
    }
}

impl<'b, T: TableSchema> TableRead<T> for CachedRocksDBColumn<'b, T> {
    fn get(&self, key: &T::Key) -> Result<Option<Cow<T::Value>>> {
        // 1. get from cache
        {
            let mut cache = self.cache.lock();
            if let Some(cached_result) = cache.get(key) {
                self.metrics.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(cached_result
                    .as_ref()
                    .map(|v| Cow::Owned((*v.clone()).to_owned())));
            }
        } // unlock

        // 2. cache miss, get from db
        self.metrics.misses.fetch_add(1, Ordering::Relaxed);
        let db_result = match KeyValueDB::get(self.inner, self.col, key.encode().borrow())? {
            Some(v_bytes) => {
                let value = <T::Value>::decode_owned(v_bytes)?;
                Some(value)
            }
            None => None,
        };

        // 3. write db result to cache
        {
            let mut cache = self.cache.lock();
            let evicted_item = cache.put(
                Box::new(key.clone()),
                db_result.as_ref().map(|v| Box::new(v.clone())),
            );
            if evicted_item.is_some() {
                self.metrics.evictions.fetch_add(1, Ordering::Relaxed);
            }
            self.metrics.puts.fetch_add(1, Ordering::Relaxed);
        } // unlock

        // 4. return db result
        Ok(db_result.map(Cow::Owned))
    }

    fn iter(&self, key: &T::Key) -> Result<TableIter<T>> {
        let iter = self
            .inner
            .iter_from(self.col, &key.encode())
            .map(|kv| match kv {
                Ok((k, v)) => Ok((
                    Cow::Owned(<T::Key>::decode_owned(k.to_vec())?),
                    Cow::Owned(<T::Value>::decode_owned(v)?),
                )),
                Err(e) => Err(DatabaseError::IoError(e)),
            });

        Ok(Box::new(iter))
    }

    fn iter_from_start(&self) -> Result<TableIter<T>> {
        let iter = self.inner.iter(self.col).map(|kv| match kv {
            Ok((k, v)) => Ok((
                Cow::Owned(<T::Key>::decode_owned(k.into_vec())?),
                Cow::Owned(<T::Value>::decode_owned(v)?),
            )),
            Err(e) => Err(DatabaseError::IoError(e)),
        });

        Ok(Box::new(iter))
    }
}

impl DatabaseTrait for CachedDB {
    type TableID = u32;
    type WriteSchema = HybridWriteSchema;

    fn view<T: TableSchema>(&self) -> Result<Box<dyn '_ + TableRead<T>>> {
        const CACHE_CAPACITY: usize = 200_000;
        let col_id: u32 = T::NAME.into();

        if self.cached_tables.contains(&col_id) {
            let mut caches_map = self.caches.lock();
            let mut metrics_map = self.metrics.lock();

            let cache_any = caches_map.entry(col_id).or_insert_with(|| {
                // println!(
                //     "Creating new cache for table '{:?}' (col {})",
                //     T::NAME,
                //     col_id
                // );
                let new_cache: LruCache<Box<T::Key>, Option<Box<T::Value>>> =
                    LruCache::new(NonZeroUsize::new(CACHE_CAPACITY).unwrap());
                Arc::new(Mutex::new(new_cache))
            });

            let cache_typed = cache_any
                .clone()
                .downcast::<Mutex<LruCache<Box<T::Key>, Option<Box<T::Value>>>>>()
                .expect("Cache type mismatch. This should not happen.");

            let metrics = metrics_map
                .entry(col_id)
                .or_insert_with(|| Arc::new(CacheMetrics::default()));

            Ok(Box::new(CachedRocksDBColumn {
                col: col_id,
                inner: &self.db,
                cache: cache_typed,
                metrics: metrics.clone(),
            }))
        } else {
            Ok(Box::new(UncachedRocksDBColumn {
                col: col_id,
                inner: &self.db,
            }))
        }
    }

    fn write_schema() -> Self::WriteSchema {
        Self::WriteSchema::new()
    }

    fn commit(&mut self, changes: Self::WriteSchema) -> Result<()> {
        // self.clear_all_caches();
        let caches_map = self.caches.lock();
        let metrics_map = self.metrics.lock();
        let mut tx = kvdb::DBTransaction::new();

        for op in changes.drain() {
            if let Some(cache_any) = caches_map.get(&op.col_id()) {
                // op.apply_to_cache(cache_any);
                op.invalidate_in_cache(cache_any, metrics_map.get(&op.col_id()).unwrap());
            }

            if let Some(v) = op.raw_value() {
                tx.put_vec(op.col_id(), op.raw_key(), v.to_vec());
            } else {
                tx.delete(op.col_id(), op.raw_key())
            }
        }

        drop(caches_map);
        drop(metrics_map);

        self.print_cache_stats();

        let db_mut = Arc::get_mut(&mut self.db).ok_or_else(|| {
            DatabaseError::SharedAccessError(
                "Failed to get mutable access to DB for commit".to_string(),
            )
        })?;

        Ok(KeyValueDB::write(db_mut, tx)?)
    }
}
