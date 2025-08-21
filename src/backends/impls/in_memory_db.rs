use super::super::{
    serde::{Decode, Encode},
    table::TableSchema,
    write_schema::WriteSchemaNoSubkey,
    DatabaseTrait, TableIter, TableRead,
};
use crate::{backends::table_name::TableNameTrait, errors::Result};
use std::{borrow::Cow, collections::BTreeMap, marker::PhantomData, sync::Arc};

struct InnerInMemoryDatabase(BTreeMap<(u32, Vec<u8>), Vec<u8>>);

impl InnerInMemoryDatabase {
    pub fn empty() -> Self {
        Self(Default::default())
    }

    pub fn commit<I>(&mut self, changes: I) -> Result<()>
    where
        I: Iterator<Item = (u32, Vec<u8>, Option<Vec<u8>>)>,
    {
        for (col, key, value) in changes {
            let k = (col, key);
            if let Some(v) = value {
                self.0.insert(k, v);
            } else {
                self.0.remove(&k);
            }
        }
        Ok(())
    }
}

pub struct InMemoryTable {
    inner: Arc<InnerInMemoryDatabase>,
    col: u32,
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

// The public-facing database type, generic over the table name enum `TN`.
pub struct WrappedInMemoryDb<TN: TableNameTrait> {
    inner: Arc<InnerInMemoryDatabase>,
    _phantom: PhantomData<TN>,
}

impl<TN: TableNameTrait> WrappedInMemoryDb<TN> {
    pub fn empty() -> Self {
        Self {
            inner: Arc::new(InnerInMemoryDatabase::empty()),
            _phantom: PhantomData,
        }
    }
}

impl<TN: TableNameTrait> DatabaseTrait<TN> for WrappedInMemoryDb<TN> {
    // type TableID = u32;
    type WriteSchema = WriteSchemaNoSubkey<TN>;

    fn view<T: TableSchema<TableName = TN>>(
        self: &Arc<Self>,
    ) -> Result<impl 'static + TableRead<T> + Send + Sync> {
        Ok(InMemoryTable {
            inner: self.inner.clone(),
            col: T::NAME.into(),
        })
    }

    fn write_schema() -> Self::WriteSchema {
        Self::WriteSchema::new()
    }

    fn commit(&mut self, changes: Self::WriteSchema) -> Result<()> {
        let inner_mut = Arc::get_mut(&mut self.inner).expect(
            "Cannot get mutable access to InMemoryDatabase for commit. It is shared elsewhere.",
        );

        let raw_changes = changes
            .drain()
            .into_iter()
            .map(|(col, key, val)| (col.into(), key, val));

        inner_mut.commit(raw_changes)
    }
}
