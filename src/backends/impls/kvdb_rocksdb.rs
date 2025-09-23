use std::{
    borrow::{Borrow, Cow},
    marker::PhantomData,
    path::Path,
    sync::Arc,
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

pub struct RocksDBColumn {
    col: u32,
    inner: Arc<kvdb_rocksdb::Database>,
}

pub fn open_database<P: AsRef<Path>>(num_cols: u32, path: P) -> Result<kvdb_rocksdb::Database> {
    let config = DatabaseConfig::with_columns(num_cols);
    Ok(kvdb_rocksdb::Database::open(&config, path)?)
}

impl<T: TableSchema> TableRead<T> for RocksDBColumn {
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
        Ok(RocksDBColumn {
            col: T::NAME.into(),
            inner: self.inner.clone(),
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
