use parking_lot::Mutex;

use super::super::{
    serde::{Decode, Encode},
    table::TableSchema,
    write_schema::WriteSchemaNoSubkey,
    DatabaseTrait, TableIter, TableRead,
};
use crate::{backends::table::GuardedIterator, errors::Result};
use std::{borrow::Cow, collections::BTreeMap, sync::Arc};

pub struct InMemoryDatabase(BTreeMap<(u32, Vec<u8>), Vec<u8>>);

pub struct InMemoryTable {
    inner: Arc<Mutex<InMemoryDatabase>>,
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
        let guard = self.inner.lock();
        if let Some(v) = guard.0.get(&key) {
            let owned = <T::Value>::decode_owned(v.to_vec())?;
            Ok(Some(Cow::Owned(owned)))
        } else {
            Ok(None)
        }
    }

    fn iter(&self, key: &T::Key) -> Result<TableIter<T>> {
        let guard = self.inner.lock();
        let range = guard.0.range((self.col, key.encode().into_owned())..);
        let iter = range
            //.filter(|((col, _), _)| *col == self.col)
            .take_while(move |((col, _), _)| *col == self.col)
            .map(|((_, k), v)| Ok((<T::Key>::decode(k)?, <T::Value>::decode(v)?)));
        Ok(Box::new(GuardedIterator::<T, InMemoryDatabase>{ iter: Box::new(iter), guard }))
    }

    fn iter_from_start(&self) -> Result<TableIter<T>> {
        let range = self.inner.lock().0.range((self.col, Vec::new())..);
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

    fn view<T: TableSchema>(shared: &Arc<Mutex<Self>>) -> Result<impl 'static + TableRead<T>> {
        Ok(InMemoryTable {
            inner: shared.clone(),
            col: T::NAME.into(),
        })
    }

    fn write_schema() -> Self::WriteSchema {
        Self::WriteSchema::new()
    }

    fn commit<'a>(shared: Arc<Mutex<Self>>, changes: Self::WriteSchema) -> Result<()> {
        let mut guard = shared.lock();

        for (col, key, value) in changes.drain() {
            let k = (col, key);
            if let Some(v) = value {
                guard.0.insert(k, v)
            } else {
                guard.0.remove(&k)
            };
        }
        Ok(())
    }
}
