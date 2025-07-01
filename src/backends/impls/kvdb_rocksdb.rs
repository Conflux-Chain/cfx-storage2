use std::{
    borrow::{Borrow, Cow},
    path::PathBuf,
    sync::Arc,
};

use super::super::{
    serde::{Decode, Encode},
    table::TableSchema,
    write_schema::WriteSchemaNoSubkey,
    DatabaseTrait, TableIter, TableRead,
};
use crate::errors::{DatabaseError, Result};

use kvdb_rocksdb::DatabaseConfig;
use parking_lot::Mutex;

pub struct RocksDBColumn {
    col: u32,
    inner: Arc<Mutex<kvdb_rocksdb::Database>>,
}

pub fn open_database(num_cols: u32, path: &str) -> Result<kvdb_rocksdb::Database> {
    let config = DatabaseConfig::with_columns(num_cols);
    let db_path = PathBuf::from(path);
    Ok(kvdb_rocksdb::Database::open(&config, db_path)?)
}

impl<T: TableSchema> TableRead<T> for RocksDBColumn {
    fn get(&self, key: &T::Key) -> Result<Option<Cow<T::Value>>> {
        if let Some(v) = self.inner.lock().get(self.col, key.encode().borrow())? {
            let owned = <T::Value>::decode_owned(v)?;
            Ok(Some(Cow::Owned(owned)))
        } else {
            Ok(None)
        }
    }

    fn iter(&self, key: &T::Key) -> Result<TableIter<T>> {
        let iter = self
            .inner
            .lock()
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
        let iter = self.inner.lock().iter(self.col).map(|kv| match kv {
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

    fn view<T: TableSchema>(shared: &Arc<Mutex<Self>>) -> Result<impl 'static + TableRead<T>> {
        Ok(RocksDBColumn {
            col: T::NAME.into(),
            inner: shared.clone(),
        })
    }

    fn write_schema() -> Self::WriteSchema {
        Self::WriteSchema::new()
    }

    fn commit(shared: Arc<Mutex<Self>>, changes: Self::WriteSchema) -> Result<()> {
        let mut tx = kvdb::DBTransaction::new();
        for (col, key, value) in changes.drain() {
            if let Some(v) = value {
                tx.put_vec(col, &key, v);
            } else {
                tx.delete(col, key.borrow())
            }
        }
        
        let guard = shared.lock();
        Ok(guard.write(tx)?)
    }
}
