use super::super::{
    serde::{Decode, Encode},
    table::TableSchema,
    write_schema::WriteSchemaNoSubkey,
    DatabaseTrait, TableIter, TableRead,
};
use crate::errors::Result;
use std::{borrow::Cow, collections::BTreeMap, sync::Arc};

pub struct InMemoryDatabase(BTreeMap<(u32, Vec<u8>), Vec<u8>>);

pub struct InMemoryTable {
    inner: Arc<InMemoryDatabase>,
    col: u32,
}

impl InMemoryDatabase {
    pub fn empty() -> Self {
        Self(Default::default())
    }
}

impl<T: TableSchema> TableRead<T> for InMemoryTable {
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

    fn iter_from_start(&self) -> Result<TableIter<T>> {
        let range = self.inner.0.range((self.col, Vec::new())..);
        let iter = range
            //.filter(|((col, _), _)| *col == self.col)
            .take_while(move |((col, _), _)| *col == self.col)
            .map(|((_, k), v)| Ok((<T::Key>::decode(k)?, <T::Value>::decode(v)?)));
        Ok(Box::new(iter))
    }

    fn iter_rev_from_end(&self) -> Result<TableIter<T>> {
        type TmpItem<'a> = (&'a (u32, Vec<u8>), &'a Vec<u8>);
        let range: Box<dyn DoubleEndedIterator<Item = TmpItem>> = if self.col == u32::MAX {
            Box::new(self.inner.0.iter().rev())
        } else {
            let end_bound = (self.col + 1, Vec::new());
            Box::new(self.inner.0.range(..end_bound).rev())
        };
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

    fn view<T: TableSchema>(self: &Arc<Self>) -> Result<impl 'static + TableRead<T> + Send + Sync> {
        Ok(InMemoryTable {
            inner: self.clone(),
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
