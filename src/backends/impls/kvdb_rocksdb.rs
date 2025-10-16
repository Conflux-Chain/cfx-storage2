use std::{
    any::{Any, TypeId},
    borrow::{Borrow, Cow},
    collections::HashMap,
    marker::PhantomData,
    num::NonZeroUsize,
    path::Path,
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
    backends::{
        table_name::TableNameTrait, write_schema::HybridWriteSchemaNoSubkey, HistoricalTableName,
        TableReader,
    },
    errors::{DatabaseError, Result},
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

pub fn open_database<P: AsRef<Path>>(num_cols: u32, path: P) -> Result<kvdb_rocksdb::Database> {
    let mut config = DatabaseConfig::with_columns(num_cols);
    let total_memory_budget = 8 * 1024;
    let column_memory_budget = total_memory_budget / num_cols as usize;
    let mut map = HashMap::new();
    for i in 0..num_cols {
        map.insert(i, column_memory_budget);
    }
    config.memory_budget = map;

    Ok(kvdb_rocksdb::Database::open(&config, path)?)
}

pub struct UncachedRocksDBColumn {
    col: u32,
    inner: Arc<kvdb_rocksdb::Database>,
}

type TableCache<T> = LruCache<Box<<T as TableSchema>::Key>, Option<Box<<T as TableSchema>::Value>>>;

pub struct CachedRocksDBColumn<T: TableSchema> {
    col: u32,
    inner: Arc<kvdb_rocksdb::Database>,
    cache: Arc<Mutex<TableCache<T>>>,
    metrics: Arc<CacheMetrics>,
}

impl<T: TableSchema> TableRead<T> for UncachedRocksDBColumn {
    fn get(&self, key: &T::Key) -> Result<Option<Cow<T::Value>>> {
        if let Some(v) = self.inner.get(self.col, key.encode().borrow())? {
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

    fn iter_rev_from_end(&self) -> Result<TableIter<T>> {
        let iter = self.inner.iter_rev(self.col).map(|kv| match kv {
            Ok((k, v)) => Ok((
                Cow::Owned(<T::Key>::decode_owned(k.into_vec())?),
                Cow::Owned(<T::Value>::decode_owned(v)?),
            )),
            Err(e) => Err(DatabaseError::IoError(e)),
        });

        Ok(Box::new(iter))
    }
}

impl<T: TableSchema> TableRead<T> for CachedRocksDBColumn<T> {
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
        let db_result = match self.inner.get(self.col, key.encode().borrow())? {
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

    fn iter_rev_from_end(&self) -> Result<TableIter<T>> {
        let iter = self.inner.iter_rev(self.col).map(|kv| match kv {
            Ok((k, v)) => Ok((
                Cow::Owned(<T::Key>::decode_owned(k.into_vec())?),
                Cow::Owned(<T::Value>::decode_owned(v)?),
            )),
            Err(e) => Err(DatabaseError::IoError(e)),
        });

        Ok(Box::new(iter))
    }
}

// The Newtype wrapper. It's generic over the TableName enum `TN`.
pub struct WrappedRocksDb<TN: TableNameTrait> {
    // The actual database instance from the external crate.
    inner: Arc<kvdb_rocksdb::Database>,
    // A zero-sized marker to make the compiler aware of the generic type TN.
    // This is crucial for the type system to associate WrappedRocksDb<HistoricalTableName>
    // with HistoricalTableName.
    _phantom: PhantomData<TN>,

    caches: Mutex<HashMap<u32, Arc<dyn Any + Send + Sync>>>,
    // metrics for each cache
    metrics: Mutex<HashMap<u32, Arc<CacheMetrics>>>,
}

impl<TN: TableNameTrait> WrappedRocksDb<TN> {
    pub fn open<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let db = open_database(TN::num_tables(), db_path)?;

        Ok(Self {
            inner: Arc::new(db),
            _phantom: PhantomData,
            caches: Mutex::new(HashMap::new()),
            metrics: Mutex::new(HashMap::new()),
        })
    }

    pub fn print_cache_stats(&self) {
        const TABLE_WIDTH: usize = 140;

        println!("{:-<width$}", "", width = TABLE_WIDTH);
        println!(
            "{:^width$}",
            "Cache Performance Statistics",
            width = TABLE_WIDTH
        );
        println!("{:-<width$}", "", width = TABLE_WIDTH);
        println!(
            "{:<12} | {:<28} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10}",
            "Historical?", "Table Name", "Size", "Hit Rate", "Hits", "Misses", "Puts", "Evictions", "Pops", "Not Pops"
        );
        println!("{:-<width$}", "", width = TABLE_WIDTH);

        let is_historical = TypeId::of::<TN>() == TypeId::of::<HistoricalTableName>();

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
                TN::get_cache_size_for_col(cache_any.as_ref(), *col_id)
            } else {
                "N/A".to_string()
            };

            let table_name: String = TN::try_from(*col_id).map_or_else(
                |_| format!("Invalid Col ID ({})", col_id),
                |table_instance| {
                    let static_str: &'static str = table_instance.into();
                    static_str.to_string()
                },
            );

            println!(
                "{:<12} | {:<28} | {:>10} | {:>9.2}% | {:>10} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10}",
                if is_historical { "Yes" } else { "No" },
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

impl<TN: TableNameTrait> DatabaseTrait<TN> for WrappedRocksDb<TN> {
    // type TableID = u32;
    type WriteSchema = HybridWriteSchemaNoSubkey<TN>;

    fn view<T: TableSchema<TableName = TN>>(self: &Arc<Self>) -> Result<TableReader<'static, T>> {
        let col_id: u32 = T::NAME.into();

        if let Some(cache_capacity) = TN::get_cache_capacity(col_id) {
            let mut caches_map = self.caches.lock();
            let mut metrics_map = self.metrics.lock();

            let cache_any = caches_map.entry(col_id).or_insert_with(|| {
                let new_cache: LruCache<Box<T::Key>, Option<Box<T::Value>>> =
                    LruCache::new(NonZeroUsize::new(cache_capacity).unwrap());
                Arc::new(Mutex::new(new_cache))
            });

            let cache_typed = cache_any
                .clone()
                .downcast::<Mutex<LruCache<Box<T::Key>, Option<Box<T::Value>>>>>()
                .expect("Cache type mismatch. This should not happen.");

            let metrics = metrics_map
                .entry(col_id)
                .or_insert_with(|| Arc::new(CacheMetrics::default()));

            Ok(Arc::new(CachedRocksDBColumn {
                col: col_id,
                inner: self.inner.clone(),
                cache: cache_typed,
                metrics: metrics.clone(),
            }))
        } else {
            Ok(Arc::new(UncachedRocksDBColumn {
                col: col_id,
                inner: self.inner.clone(),
            }))
        }
    }

    fn write_schema() -> Self::WriteSchema {
        Self::WriteSchema::new()
    }

    fn commit(&self, changes: Self::WriteSchema) -> Result<()> {
        self.print_cache_stats();

        let caches_map = self.caches.lock();
        let metrics_map = self.metrics.lock();
        let mut tx = kvdb::DBTransaction::new();

        for op in changes.drain() {
            let col_id = op.col_id().into();
            if let Some(cache_any) = caches_map.get(&col_id) {
                let metrics = metrics_map
                    .get(&col_id)
                    .expect("Metrics should exist if cache exists");
                TN::apply_cache_update_policy(col_id, op.as_ref(), cache_any, metrics);
            }

            if let Some(v) = op.raw_value() {
                tx.put_vec(col_id, op.raw_key(), v.to_vec());
            } else {
                tx.delete(col_id, op.raw_key())
            }
        }

        drop(caches_map);
        drop(metrics_map);

        self.print_cache_stats();

        Ok(KeyValueDB::write(&*self.inner, tx)?)
    }
}
