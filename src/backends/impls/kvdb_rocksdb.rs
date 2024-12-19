use std::{
    borrow::{Borrow, Cow},
    path::PathBuf,
};

use super::super::{
    serde::{Decode, Encode},
    table::TableSchema,
    write_schema::WriteSchemaNoSubkey,
    DatabaseTrait, TableIter, TableRead,
};
use crate::errors::{DecodeError, Result};

use kvdb::KeyValueDB;
use kvdb_rocksdb::DatabaseConfig;

pub struct RocksDBColumn<'a> {
    col: u32,
    inner: &'a kvdb_rocksdb::Database,
}

pub fn empty_rocksdb(num_cols: u32, path: &str) -> Result<kvdb_rocksdb::Database> {
    let config = DatabaseConfig::with_columns(num_cols);
    let db_path = PathBuf::from(path);
    Ok(kvdb_rocksdb::Database::open(&config, db_path)?)
}

impl<'b, T: TableSchema> TableRead<T> for RocksDBColumn<'b> {
    fn get(&self, key: &T::Key) -> Result<Option<Cow<T::Value>>> {
        if let Some(v) = KeyValueDB::get(self.inner, self.col, key.encode().borrow())? {
            let owned = <T::Value>::decode(&v)?.into_owned();
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
                    Cow::Owned(<T::Key>::decode(&k)?.into_owned()),
                    Cow::Owned(<T::Value>::decode(&v)?.into_owned()),
                )),
                Err(_) => Err(DecodeError::RocksDbError),
            });

        Ok(Box::new(iter))
    }

    #[cfg(test)]
    fn iter_from_start(&self) -> Result<TableIter<T>> {
        let iter = self.inner.iter(self.col).map(|kv| match kv {
            Ok((k, v)) => Ok((
                Cow::Owned(<T::Key>::decode(&k)?.into_owned()),
                Cow::Owned(<T::Value>::decode(&v)?.into_owned()),
            )),
            Err(_) => Err(DecodeError::RocksDbError),
        });

        Ok(Box::new(iter))
    }
}

impl DatabaseTrait for kvdb_rocksdb::Database {
    type TableID = u32;
    type WriteSchema = WriteSchemaNoSubkey<Self::TableID>;

    #[cfg(test)]
    fn empty_for_test() -> Result<Self> {
        use crate::backends::TableName;

        let db_path = "test_database";
        if std::path::Path::new(db_path).exists() {
            std::fs::remove_dir_all(db_path)?;
        }
        std::fs::create_dir_all(db_path).unwrap();

        empty_rocksdb(TableName::max_index() + 1, db_path)
    }

    fn view<T: TableSchema>(&self) -> Result<impl '_ + TableRead<T>> {
        Ok(RocksDBColumn {
            col: T::NAME.into(),
            inner: self,
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
