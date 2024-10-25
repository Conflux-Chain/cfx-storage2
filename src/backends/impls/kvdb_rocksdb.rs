use std::borrow::{Borrow, Cow};

use super::super::{
    serde::{Decode, Encode},
    table::TableSchema,
    write_schema::WriteSchemaNoSubkey,
    DatabaseTrait, TableIter, TableRead,
};
use crate::errors::{DecResult, Result};

use kvdb::KeyValueDB;

pub struct RocksDBColumn<'a> {
    col: u32,
    inner: &'a kvdb_rocksdb::Database,
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

    fn iter(&self, _key: &T::Key) -> Result<TableIter<T>> {
        Ok(Box::new(crate::todo_iter::<
            DecResult<(Cow<T::Key>, Cow<T::Value>)>,
        >()))
    }

    fn min_key(&self) -> Result<Option<Cow<T::Key>>> {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }
}

impl DatabaseTrait for kvdb_rocksdb::Database {
    type TableID = u32;
    type WriteSchema = WriteSchemaNoSubkey<Self::TableID>;

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
