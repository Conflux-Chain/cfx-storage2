use std::{
    any::Any,
    sync::{atomic::Ordering, Arc},
};

use lru::LruCache;
use parking_lot::Mutex;

use crate::backends::{impls::kvdb_rocksdb::CacheMetrics, serde::Encode, TableName, TableSchema};

use super::{TableWriteOp, WriteSchemaTrait};

// Supports both serialized data and structured data to
// support writing to the database and updating the cache respectively
pub struct HybridWriteSchemaNoSubkey<Name> {
    inner: Mutex<Vec<Box<dyn GenericWriteOperation<Name>>>>,
}

impl<Name> Default for HybridWriteSchemaNoSubkey<Name> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Name> HybridWriteSchemaNoSubkey<Name> {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(vec![]),
        }
    }

    pub fn drain(self) -> Vec<Box<dyn GenericWriteOperation<Name>>> {
        let mut inner = self.inner.lock();

        std::mem::take(&mut *inner)
    }
}

impl<Name: From<TableName> + Send + Sync + Copy + 'static> HybridWriteSchemaNoSubkey<Name> {
    #[inline]
    fn write_inner<T: TableSchema>(
        inner: &mut Vec<Box<dyn GenericWriteOperation<Name>>>,
        op: TableWriteOp<T>,
    ) {
        let (key, value) = op;

        let raw_key = <T::Key as Encode>::encode_cow(key.clone()).into_owned();
        let raw_value = value
            .clone()
            .map(|v| <T::Value as Encode>::encode_cow(v).into_owned());

        let operation = TypedWriteOperation::<T, Name> {
            col: T::NAME.into(),
            raw_key,
            raw_value,
            structured_key: Box::new(key.into_owned()),
            structured_value: value.map(|v| Box::new(v.into_owned())),
        };

        inner.push(Box::new(operation));
    }
}

impl<Name: From<TableName> + Send + Sync + Copy + 'static> WriteSchemaTrait
    for HybridWriteSchemaNoSubkey<Name>
{
    fn write<T: TableSchema>(&self, op: TableWriteOp<'_, T>) {
        let mut inner = self.inner.lock();
        Self::write_inner::<T>(&mut *inner, op)
    }

    fn write_batch<'a, T: TableSchema>(
        &self,
        changes: impl IntoIterator<Item = TableWriteOp<'a, T>>,
    ) {
        let mut inner = self.inner.lock();
        for op in changes {
            Self::write_inner::<T>(&mut *inner, op)
        }
    }
}

pub trait GenericWriteOperation<Name>: Send + Sync {
    fn col_id(&self) -> Name;

    fn raw_key(&self) -> &[u8];

    fn raw_value(&self) -> &Option<Vec<u8>>;

    fn structured_key_any(&self) -> &dyn Any;

    fn apply_to_cache(&self, cache_any: &Arc<dyn Any + Send + Sync>, metrics: &Arc<CacheMetrics>);

    fn invalidate_in_cache(
        &self,
        cache_any: &Arc<dyn Any + Send + Sync>,
        metrics: &Arc<CacheMetrics>,
    );
}

struct TypedWriteOperation<T: TableSchema, Name> {
    col: Name,
    raw_key: Vec<u8>,
    raw_value: Option<Vec<u8>>,
    structured_key: Box<T::Key>,
    structured_value: Option<Box<T::Value>>,
}

impl<T: TableSchema, Name: From<TableName> + Send + Sync + Copy> GenericWriteOperation<Name>
    for TypedWriteOperation<T, Name>
{
    fn col_id(&self) -> Name {
        self.col
    }

    fn raw_key(&self) -> &[u8] {
        &self.raw_key
    }

    fn raw_value(&self) -> &Option<Vec<u8>> {
        &self.raw_value
    }

    fn structured_key_any(&self) -> &dyn Any {
        &self.structured_key
    }

    fn apply_to_cache(&self, cache_any: &Arc<dyn Any + Send + Sync>, metrics: &Arc<CacheMetrics>) {
        if let Ok(typed_cache_arc) = cache_any
            .clone()
            .downcast::<Mutex<LruCache<Box<T::Key>, Option<Box<T::Value>>>>>()
        {
            let mut cache = typed_cache_arc.lock();
            let evicted_item =
                cache.put(self.structured_key.clone(), self.structured_value.clone());
            metrics.puts.fetch_add(1, Ordering::Relaxed);
            if evicted_item.is_some() {
                metrics.evictions.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            unreachable!();
        }
    }

    fn invalidate_in_cache(
        &self,
        cache_any: &Arc<dyn Any + Send + Sync>,
        metrics: &Arc<CacheMetrics>,
    ) {
        if let Ok(typed_cache_arc) = cache_any
            .clone()
            .downcast::<Mutex<LruCache<Box<T::Key>, Option<Box<T::Value>>>>>()
        {
            let mut cache = typed_cache_arc.lock();
            // Just pop! No clones of the value needed.
            let popped_item = cache.pop(&self.structured_key);
            if popped_item.is_some() {
                metrics.pops.fetch_add(1, Ordering::Relaxed);
            } else {
                metrics.not_pops.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            unreachable!();
        }
    }
}
