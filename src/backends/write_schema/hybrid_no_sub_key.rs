use std::{
    any::Any,
    sync::{atomic::Ordering, Arc},
};

use lru::LruCache;
use parking_lot::Mutex;

use crate::backends::{
    impls::kvdb_rocksdb::CacheMetrics, serde::Encode, TableNameTrait, TableSchema,
};

use super::{TableWriteOp, WriteSchemaTrait};

pub struct HybridWriteSchemaNoSubkey<TN: TableNameTrait> {
    inner: Mutex<Vec<Box<dyn GenericWriteOperation<TN>>>>,
}

impl<TN: TableNameTrait> HybridWriteSchemaNoSubkey<TN> {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Vec::new()),
        }
    }

    pub fn drain(self) -> Vec<Box<dyn GenericWriteOperation<TN>>> {
        self.inner.into_inner()
    }
}

impl<TN: TableNameTrait> Default for HybridWriteSchemaNoSubkey<TN> {
    fn default() -> Self {
        Self::new()
    }
}

impl<TN: TableNameTrait> WriteSchemaTrait<TN> for HybridWriteSchemaNoSubkey<TN> {
    fn write<T: TableSchema<TableName = TN>>(&self, op: TableWriteOp<'_, T>) {
        let (key, value) = op;

        let raw_key = <T::Key as Encode>::encode_cow(key.clone()).into_owned();
        let raw_value = value
            .clone()
            .map(|v| <T::Value as Encode>::encode_cow(v).into_owned());

        let operation = TypedWriteOperation::<TN, T> {
            col: T::NAME,
            raw_key,
            raw_value,
            structured_key: Box::new(key.into_owned()),
            structured_value: value.map(|v| Box::new(v.into_owned())),
        };

        self.inner.lock().push(Box::new(operation));
    }

    fn write_batch<'a, T: TableSchema<TableName = TN>>(
        &self,
        changes: impl IntoIterator<Item = TableWriteOp<'a, T>>,
    ) {
        let mut inner = self.inner.lock();
        for op in changes {
            let (key, value) = op;
            let raw_key = T::Key::encode_cow(key.clone()).into_owned();
            let raw_value = value
                .as_ref()
                .map(|v| T::Value::encode_cow(v.clone()).into_owned());
            let operation = TypedWriteOperation::<TN, T> {
                col: T::NAME,
                raw_key,
                raw_value,
                structured_key: Box::new(key.into_owned()),
                structured_value: value.map(|v| Box::new(v.into_owned())),
            };
            inner.push(Box::new(operation));
        }
    }
}

pub trait GenericWriteOperation<TN: TableNameTrait>: Send + Sync {
    fn col_id(&self) -> TN;

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

struct TypedWriteOperation<TN, T: TableSchema> {
    col: TN,
    raw_key: Vec<u8>,
    raw_value: Option<Vec<u8>>,
    structured_key: Box<T::Key>,
    structured_value: Option<Box<T::Value>>,
}

impl<TN: TableNameTrait, T: TableSchema<TableName = TN>> GenericWriteOperation<TN>
    for TypedWriteOperation<TN, T>
{
    fn col_id(&self) -> TN {
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
