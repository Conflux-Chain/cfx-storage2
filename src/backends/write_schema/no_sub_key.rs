use crate::backends::table_name::TableNameTrait;

use super::super::{serde::Encode, TableSchema};
use super::{TableWriteOp, WriteSchemaTrait};
use parking_lot::Mutex;

pub type WriteSchemaOp<TN> = (TN, Vec<u8>, Option<Vec<u8>>);
pub struct WriteSchemaNoSubkey<TN: TableNameTrait> {
    inner: Mutex<Vec<WriteSchemaOp<TN>>>,
}

impl<TN: TableNameTrait> Default for WriteSchemaNoSubkey<TN> {
    fn default() -> Self {
        Self::new()
    }
}

impl<TN: TableNameTrait> WriteSchemaNoSubkey<TN> {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(vec![]),
        }
    }

    pub fn drain(self) -> Vec<WriteSchemaOp<TN>> {
        let mut inner = self.inner.lock();

        std::mem::take(&mut *inner)
    }
}

impl<TN: TableNameTrait> WriteSchemaNoSubkey<TN> {
    fn write_inner<T: TableSchema>(inner: &mut Vec<WriteSchemaOp<TN>>, op: TableWriteOp<T>)
    where
        T: TableSchema<TableName = TN>,
    {
        let (key, value) = op;
        let raw_key = <T::Key as Encode>::encode_cow(key).into_owned();
        let raw_value = value.map(|v| <T::Value as Encode>::encode_cow(v).into_owned());
        inner.push((T::NAME, raw_key, raw_value))
    }
}

impl<TN: TableNameTrait> WriteSchemaTrait<TN> for WriteSchemaNoSubkey<TN> {
    fn write<T: TableSchema<TableName = TN>>(&self, op: TableWriteOp<'_, T>) {
        let mut inner = self.inner.lock();
        Self::write_inner::<T>(&mut *inner, op)
    }

    fn write_batch<'a, T: TableSchema<TableName = TN>>(
        &self,
        changes: impl IntoIterator<Item = TableWriteOp<'a, T>>,
    ) {
        let mut inner = self.inner.lock();
        for op in changes {
            Self::write_inner::<T>(&mut *inner, op)
        }
    }
}
