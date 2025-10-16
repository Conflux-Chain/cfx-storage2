use parking_lot::Mutex;

use super::super::{
    serde::{Decode, Encode},
    table::TableSchema,
    write_schema::WriteSchemaNoSubkey,
    DatabaseTrait, TableIter, TableRead,
};
use crate::{
    backends::{table_name::TableNameTrait, TableReader},
    errors::Result,
};
use std::{borrow::Cow, collections::BTreeMap, marker::PhantomData, sync::Arc};

type InMemoryKVMap = BTreeMap<(u32, Vec<u8>), Vec<u8>>;
struct InnerInMemoryDatabase(Mutex<InMemoryKVMap>);

impl InnerInMemoryDatabase {
    pub fn empty() -> Self {
        Self(Default::default())
    }

    pub fn commit<I>(&self, changes: I) -> Result<()>
    where
        I: Iterator<Item = (u32, Vec<u8>, Option<Vec<u8>>)>,
    {
        let mut inner_guard = self.0.lock();

        for (col, key, value) in changes {
            let k = (col, key);
            if let Some(v) = value {
                inner_guard.insert(k, v);
            } else {
                inner_guard.remove(&k);
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

        let maybe_v = {
            let guard = self.inner.0.lock();
            guard.get(&key).cloned()
        };

        if let Some(v) = maybe_v {
            Ok(Some(Cow::Owned(<T::Value>::decode_owned(v)?)))
        } else {
            Ok(None)
        }
    }

    fn iter(&self, key: &T::Key) -> Result<TableIter<T>> {
        let start = (self.col, key.encode().into_owned());

        let items: Vec<(Vec<u8>, Vec<u8>)> = {
            let guard = self.inner.0.lock();
            guard
                .range(start..)
                .take_while(|((col, _), _)| *col == self.col)
                .map(|((_, k), v)| (k.clone(), v.clone()))
                .collect()
        };

        let iter = items.into_iter().map(|(kb, vb)| {
            let k = <T::Key as Decode>::decode_owned(kb)?;
            let v = <T::Value as Decode>::decode_owned(vb)?;
            Ok((Cow::Owned(k), Cow::Owned(v)))
        });

        Ok(Box::new(iter))
    }

    fn iter_from_start(&self) -> Result<TableIter<T>> {
        let start = (self.col, Vec::new());

        let items: Vec<(Vec<u8>, Vec<u8>)> = {
            let guard = self.inner.0.lock();
            guard
                .range(start..)
                .take_while(|((col, _), _)| *col == self.col)
                .map(|((_, k), v)| (k.clone(), v.clone()))
                .collect()
        };

        let iter = items.into_iter().map(|(kb, vb)| {
            let k = <T::Key as Decode>::decode_owned(kb)?;
            let v = <T::Value as Decode>::decode_owned(vb)?;
            Ok((Cow::Owned(k), Cow::Owned(v)))
        });

        Ok(Box::new(iter))
    }

    fn iter_rev_from_end(&self) -> Result<TableIter<T>> {
        let items: Vec<(Vec<u8>, Vec<u8>)> = {
            let guard = self.inner.0.lock();
            if self.col == u32::MAX {
                guard
                    .iter()
                    .rev()
                    .take_while(|((col, _), _)| *col == self.col)
                    .map(|((_, k), v)| (k.clone(), v.clone()))
                    .collect()
            } else {
                let end_bound = (self.col + 1, Vec::new());
                guard
                    .range(..end_bound)
                    .rev()
                    .take_while(|((col, _), _)| *col == self.col)
                    .map(|((_, k), v)| (k.clone(), v.clone()))
                    .collect()
            }
        };

        let iter = items.into_iter().map(|(kb, vb)| {
            let k = <T::Key as Decode>::decode_owned(kb)?;
            let v = <T::Value as Decode>::decode_owned(vb)?;
            Ok((Cow::Owned(k), Cow::Owned(v)))
        });

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

    fn view<T: TableSchema<TableName = TN>>(self: &Arc<Self>) -> Result<TableReader<'static, T>> {
        Ok(Arc::new(InMemoryTable {
            inner: self.inner.clone(),
            col: T::NAME.into(),
        }))
    }

    fn write_schema() -> Self::WriteSchema {
        Self::WriteSchema::new()
    }

    fn commit(&self, changes: Self::WriteSchema) -> Result<()> {
        let raw_changes = changes
            .drain()
            .into_iter()
            .map(|(col, key, val)| (col.into(), key, val));

        self.inner.commit(raw_changes)
    }
}
