use std::borrow::Cow;
use std::fmt::Debug;
use std::sync::Arc;

use super::serde::{Decode, Encode, EncodeSubKey};
use super::table_name::TableName;
use crate::combine_traits;

use crate::errors::{DbResult, Result};
use auto_impl::auto_impl;

pub type TableItem<'a, T> = (
    Cow<'a, <T as TableSchema>::Key>,
    Cow<'a, <T as TableSchema>::Value>,
);
pub type TableIter<'a, 'b, T> = Box<dyn 'a + Iterator<Item = DbResult<TableItem<'b, T>>>>;
pub type TableReader<'a, T> = Arc<dyn 'a + TableRead<T>>;

#[auto_impl(&, Arc)]
pub trait TableRead<T: TableSchema> {
    fn get(&self, key: &T::Key) -> Result<Option<Cow<T::Value>>>;

    fn iter<'a>(&'a self, key: &T::Key) -> Result<TableIter<'a, '_, T>>;

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
