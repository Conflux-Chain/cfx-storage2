use std::borrow::Cow;
use std::fmt::Debug;
use std::sync::Arc;

use super::serde::{Decode, Encode, EncodeSubKey};
use super::table_name::TableName;
use super::DatabaseTrait;
use crate::combine_traits;

use crate::errors::{DbResult, Result};
use auto_impl::auto_impl;

pub type TableItem<'a, T> = (
    Cow<'a, <T as TableSchema>::Key>,
    Cow<'a, <T as TableSchema>::Value>,
);
pub type TableIter<'a, T> = Box<dyn 'a + Iterator<Item = DbResult<TableItem<'a, T>>>>;
pub type TableReader<'a, T> = Arc<dyn 'a + TableRead<T> + Send + Sync>;

pub struct GuardedIterator<'a, T: TableSchema, D: DatabaseTrait> {
    pub(crate) guard: parking_lot::MutexGuard<'a, D>,
    pub(crate) iter: dyn 'a + Iterator<Item = DbResult<TableItem<'a, T>>>,
}

// 为结构体实现 Iterator trait
impl<'a, T: TableSchema, D: DatabaseTrait> Iterator for GuardedIterator<'a, T, D> {
    type Item = DbResult<TableItem<'a, T>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

#[auto_impl(&, Arc)]
pub trait TableRead<T: TableSchema> {
    fn get(&self, key: &T::Key) -> Result<Option<Cow<T::Value>>>;

    fn iter<'a>(&'a self, key: &T::Key) -> Result<TableIter<'a, T>>;

    fn iter_from_start(&self) -> Result<TableIter<T>>;
}

combine_traits!(TableKey: 'static + EncodeSubKey + Decode + ToOwned + Ord + Eq + Send + Sync + Debug);
combine_traits!(TableValue: 'static + Encode + Decode + ToOwned  + Send + Sync + Debug);

pub trait TableSchema: 'static + Copy + Send + Sync {
    const NAME: TableName;
    type Key: TableKey + ?Sized;
    type Value: TableValue + ?Sized;
}

#[cfg(test)]
mod tests {
    use super::{TableName, TableSchema};

    #[derive(Clone, Copy)]
    struct MockTable;
    impl TableSchema for MockTable {
        const NAME: TableName = TableName::MockTable;
        type Key = [u8];
        type Value = [u8];
    }
}
