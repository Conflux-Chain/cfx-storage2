use std::{
    borrow::{Borrow, Cow}, collections::HashMap, num::NonZeroUsize, path::PathBuf
};

use super::super::{
    serde::{Decode, Encode},
    table::TableSchema,
    write_schema::WriteSchemaNoSubkey,
    DatabaseTrait, TableIter, TableRead,
};
use crate::errors::{DatabaseError, Result};

use kvdb::KeyValueDB;
use kvdb_rocksdb::DatabaseConfig;
use lru::LruCache;
use parking_lot::Mutex;

pub struct CachedRocksDBColumn<'a, T: TableSchema> {
    col: u32,
    inner: &'a kvdb_rocksdb::Database,
    cache: Mutex<LruCache<Box<T::Key>, Option<Box<T::Value>>>>,
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

impl<'b, T: TableSchema> TableRead<T> for CachedRocksDBColumn<'b, T> {
    fn get(&self, key: &T::Key) -> Result<Option<Cow<T::Value>>> {
        // 1. get from cache
        {
            let mut cache = self.cache.lock();
            if let Some(cached_result) = cache.get(key) {
                // if let Some(cached_existing) = cached_result.as_ref() {
                //     let cached_existing_v = *cached_existing.clone();
                //     return Ok(Some(Cow::Owned(cached_existing_v.to_owned())))
                // }
                return Ok(cached_result.as_ref().map(|v| Cow::Owned((*v.clone()).to_owned())));
            }
        } // unlock

        // 2. cache miss, get from db
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
}

impl DatabaseTrait for kvdb_rocksdb::Database {
    type TableID = u32;
    type WriteSchema = WriteSchemaNoSubkey<Self::TableID>;

    fn view<T: TableSchema>(&self) -> Result<impl '_ + TableRead<T>> {
        const CACHE_CAPACITY: usize = 100_000;
        Ok(CachedRocksDBColumn {
            col: T::NAME.into(),
            inner: self,
            cache: Mutex::new(LruCache::new(NonZeroUsize::new(CACHE_CAPACITY).unwrap())),
        })
    }

    fn write_schema() -> Self::WriteSchema {
        Self::WriteSchema::new()
    }

    fn commit(&mut self, changes: Self::WriteSchema) -> Result<()> {
        let mut tx = kvdb::DBTransaction::new();
        for (col, key, value) in changes.drain() {
            if let Some(v) = value {
                tx.put_vec(col, &key, v);
            } else {
                tx.delete(col, key.borrow())
            }
        }

        Ok(KeyValueDB::write(self, tx)?)
    }
}
