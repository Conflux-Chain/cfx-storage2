use std::{any::Any, sync::Arc};

use lru::LruCache;
use parking_lot::Mutex;

use crate::backends::{serde::Encode, TableNameTrait, TableSchema};

use super::{TableWriteOp, WriteSchemaTrait};

pub struct HybridWriteSchema<Name: TableNameTrait> {
    inner: Mutex<Vec<Box<dyn GenericWriteOperation<Name>>>>,
}

impl<Name: TableNameTrait> HybridWriteSchema<Name> {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Vec::new()),
        }
    }

    pub fn drain(self) -> Vec<Box<dyn GenericWriteOperation<Name>>> {
        self.inner.into_inner()
    }
}

impl<Name: TableNameTrait> Default for HybridWriteSchema<Name> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Name: TableNameTrait> WriteSchemaTrait<Name> for HybridWriteSchema<Name> {
    fn write<T: TableSchema<TableName = Name>>(&self, op: TableWriteOp<'_, T>) {
        let (key, value) = op;
        
        let raw_key = <T::Key as Encode>::encode_cow(key.clone()).into_owned();
        let raw_value = value.clone().map(|v| <T::Value as Encode>::encode_cow(v).into_owned());

        let operation = TypedWriteOperation::<T> {
            col: T::NAME.into(),
            raw_key,
            raw_value,
            structured_key: Box::new(key.into_owned()),
            structured_value: value.map(|v| Box::new(v.into_owned())),
        };

        self.inner.lock().push(Box::new(operation));
    }

    fn write_batch<'a, T: TableSchema<TableName = Name>>(&self, changes: impl IntoIterator<Item = TableWriteOp<'a, T>>) {
        let mut inner = self.inner.lock();
        for op in changes {
            let (key, value) = op;
            let raw_key = T::Key::encode_cow(key.clone()).into_owned();
            let raw_value = value.as_ref().map(|v| T::Value::encode_cow(v.clone()).into_owned());
            let operation = TypedWriteOperation::<T> {
                col: T::NAME.into(),
                raw_key,
                raw_value,
                structured_key: Box::new(key.into_owned()),
                structured_value: value.map(|v| Box::new(v.into_owned())),
            };
            inner.push(Box::new(operation));
        }
    }
}

pub trait GenericWriteOperation<Name: TableNameTrait>: Send + Sync {
    fn col_id(&self) -> u32;

    fn raw_key(&self) -> &[u8];

    fn raw_value(&self) -> &Option<Vec<u8>>;

    fn apply_to_cache(&self, cache_any: &Arc<dyn Any + Send + Sync>);
}

struct TypedWriteOperation<T: TableSchema> {
    col: u32,
    raw_key: Vec<u8>,
    raw_value: Option<Vec<u8>>,
    structured_key: Box<T::Key>,
    structured_value: Option<Box<T::Value>>,
}

impl<Name: TableNameTrait, T: TableSchema<TableName = Name>> GenericWriteOperation<Name> for TypedWriteOperation<T> {
    fn col_id(&self) -> u32 {
        self.col
    }

    fn raw_key(&self) -> &[u8] {
        &self.raw_key
    }

    fn raw_value(&self) -> &Option<Vec<u8>> {
        &self.raw_value
    }

    fn apply_to_cache(&self, cache_any: &Arc<dyn Any + Send + Sync>) {
        if let Ok(typed_cache_arc) = cache_any
            .clone()
            .downcast::<Mutex<LruCache<Box<T::Key>, Option<Box<T::Value>>>>>()
        {
            let mut cache = typed_cache_arc.lock();
            cache.put(self.structured_key.clone(), self.structured_value.clone());
        } else {
            unreachable!();
        }
    }
}