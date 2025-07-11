use std::{
    borrow::{Borrow, Cow}, collections::HashMap, marker::PhantomData, path::Path, sync::Arc, num::NonZeroUsize
};

use super::super::{
    serde::{Decode, Encode},
    table::TableSchema,
    write_schema::WriteSchemaNoSubkey,
    DatabaseTrait, TableIter, TableRead,
};
use crate::{
    backends::table_name::TableNameTrait,
    errors::{DatabaseError, Result},
};

use kvdb::KeyValueDB;
use kvdb_rocksdb::DatabaseConfig;
use lru::LruCache;
use parking_lot::Mutex;

pub struct CachedRocksDBColumn<T: TableSchema> {
    col: u32,
    inner: Arc<kvdb_rocksdb::Database>,
    cache: Mutex<LruCache<Box<T::Key>, Option<Box<T::Value>>>>,
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

impl<T: TableSchema> TableRead<T> for CachedRocksDBColumn<T> {
    fn get(&self, key: &T::Key) -> Result<Option<Cow<T::Value>>> {
        // 1. get from cache
        {
            let mut cache = self.cache.lock();
            if let Some(cached_result) = cache.get(key) {
                return Ok(cached_result.as_ref().map(|v| Cow::Owned((*v.clone()).to_owned())));
            }
        } // unlock

        // 2. cache miss, get from db
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
            cache.put(Box::new(key.clone()), db_result.as_ref().map(|v| Box::new(v.clone())));
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
}

impl<TN: TableNameTrait> WrappedRocksDb<TN> {
    pub fn open<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let db = open_database(TN::num_tables(), db_path)?;
        Ok(Self {
            inner: Arc::new(db),
            _phantom: PhantomData,
        })
    }
}

impl<TN: TableNameTrait> DatabaseTrait<TN> for WrappedRocksDb<TN> {
    // type TableID = u32;
    type WriteSchema = WriteSchemaNoSubkey<TN>;

    fn view<T: TableSchema<TableName = TN>>(
        self: &Arc<Self>,
    ) -> Result<impl 'static + TableRead<T> + Send + Sync> {
        const CACHE_CAPACITY: usize = 100_000;

        Ok(CachedRocksDBColumn {
            col: T::NAME.into(),
            inner: self.inner.clone(),
            cache: Mutex::new(LruCache::new(NonZeroUsize::new(CACHE_CAPACITY).unwrap())),
        })
    }

    fn write_schema() -> Self::WriteSchema {
        Self::WriteSchema::new()
    }

    fn commit(&self, changes: Self::WriteSchema) -> Result<()> {
        let mut tx = kvdb::DBTransaction::new();
        for (col, key, value) in changes.drain() {
            if let Some(v) = value {
                tx.put_vec(col.into(), &key, v);
            } else {
                tx.delete(col.into(), key.borrow())
            }
        }

        Ok(KeyValueDB::write(&*self.inner, tx)?)
    }
}
