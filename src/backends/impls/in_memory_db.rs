use super::super::{
    serde::{Decode, Encode},
    table::TableSchema,
    write_schema::WriteSchemaNoSubkey,
    DatabaseTrait, TableIter, TableRead,
};
use crate::errors::Result;
use std::{borrow::Cow, collections::BTreeMap};

pub struct InMemoryDatabase(BTreeMap<(u32, Vec<u8>), Vec<u8>>);

pub struct InMemoryTable<'a> {
    inner: &'a InMemoryDatabase,
    col: u32,
}

impl InMemoryDatabase {
    pub fn empty() -> Self {
        Self(Default::default())
    }
}

impl<'b, T: TableSchema> TableRead<T> for InMemoryTable<'b> {
    fn get(&self, key: &T::Key) -> Result<Option<Cow<T::Value>>> {
        let key = (self.col, key.encode().into_owned());
        if let Some(v) = self.inner.0.get(&key) {
            Ok(Some(<T::Value>::decode(v)?))
        } else {
            Ok(None)
        }
    }

    fn iter(&self, key: &T::Key) -> Result<TableIter<T>> {
        let range = self.inner.0.range((self.col, key.encode().into_owned())..);
        let iter = range
            //.filter(|((col, _), _)| *col == self.col)
            .take_while(move |((col, _), _)| *col == self.col)
            .map(|((_, k), v)| Ok((<T::Key>::decode(k)?, <T::Value>::decode(v)?)));
        Ok(Box::new(iter))
    }
}

impl DatabaseTrait for InMemoryDatabase {
    type TableID = u32;
    type WriteSchema = WriteSchemaNoSubkey<Self::TableID>;

    fn view<T: TableSchema>(&self) -> Result<impl '_ + TableRead<T>> {
        Ok(InMemoryTable {
            inner: self,
            col: T::NAME.into(),
        })
    }

    fn write_schema() -> Self::WriteSchema {
        Self::WriteSchema::new()
    }

    fn commit<'a>(&mut self, changes: Self::WriteSchema) -> Result<()> {
        for (col, key, value) in changes.drain() {
            let k = (col, key);
            if let Some(v) = value {
                self.0.insert(k, v)
            } else {
                self.0.remove(&k)
            };
        }
        Ok(())
    }
}
