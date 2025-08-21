use std::{path::Path, sync::Arc};

use crate::{
    backends::{HistoricalTableName, VersionedKVName, WrappedInMemoryDb},
    errors::Result,
    middlewares::{table_schema::VersionedKeyValueSchema, VersionedStore, VersionedStoreCache},
    traits::KeyValueStoreManager,
};
use ethereum_types::H256;
use parking_lot::Mutex;
use static_assertions::assert_impl_all;

pub struct Storage {
    backend: Arc<WrappedInMemoryDb<HistoricalTableName>>,
    cache: Arc<Mutex<VersionedStoreCache<FlatKeyValue>>>,
}

impl Storage {
    pub fn new_empty(log_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            backend: WrappedInMemoryDb::empty().into(),
            cache: Mutex::new(VersionedStoreCache::new(log_path, None, 0)?).into(),
        })
    }

    pub fn as_manager(&mut self) -> Result<VersionedStore<'_, FlatKeyValue>> {
        VersionedStore::new(self.backend.clone(), self.cache.clone())
    }
}

assert_impl_all!(VersionedStore<'_, FlatKeyValue>: KeyValueStoreManager<Box<[u8]>, Box<[u8]>, H256>);

#[derive(Clone, Copy, Debug)]
pub struct FlatKeyValue;

impl VersionedKeyValueSchema for FlatKeyValue {
    const NAME: VersionedKVName = VersionedKVName::FlatKV;

    type Key = Box<[u8]>;
    type Value = Box<[u8]>;
}
