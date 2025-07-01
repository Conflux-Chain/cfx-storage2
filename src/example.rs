use std::sync::Arc;

use crate::{
    backends::{InMemoryDatabase, VersionedKVName},
    errors::Result,
    middlewares::{table_schema::VersionedKeyValueSchema, VersionedStore, VersionedStoreCache},
    traits::KeyValueStoreManager,
};
use ethereum_types::H256;
use parking_lot::Mutex;
use static_assertions::assert_impl_all;

pub struct Storage {
    backend: Arc<Mutex<InMemoryDatabase>>,
    cache: Arc<Mutex<VersionedStoreCache<FlatKeyValue>>>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            backend: Mutex::new(InMemoryDatabase::empty()).into(),
            cache: Mutex::new(VersionedStoreCache::new_empty()).into(),
        }
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
