use crate::{
    backends::{InMemoryDatabase, VersionedKVName},
    errors::Result,
    middlewares::{table_schema::VersionedKeyValueSchema, VersionedStore, VersionedStoreCache},
    traits::KeyValueStoreManager,
};
use ethereum_types::H256;
use static_assertions::assert_impl_all;

pub struct Storage {
    backend: InMemoryDatabase,
    cache: VersionedStoreCache<FlatKeyValue>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            backend: InMemoryDatabase::empty(),
            cache: VersionedStoreCache::new_empty(),
        }
    }

    pub fn as_manager(&mut self) -> Result<VersionedStore<'_, '_, FlatKeyValue>> {
        VersionedStore::new(&self.backend, &mut self.cache)
    }
}

assert_impl_all!(VersionedStore<'_, '_, FlatKeyValue>: KeyValueStoreManager<Box<[u8]>, Box<[u8]>, H256>);

#[derive(Clone, Copy, Debug)]
pub struct FlatKeyValue;

impl VersionedKeyValueSchema for FlatKeyValue {
    const NAME: VersionedKVName = VersionedKVName::FlatKV;

    type Key = Box<[u8]>;
    type Value = Box<[u8]>;
}
