use crate::backends::table_name::TableNameTrait;

use super::super::{serde::Encode, TableSchema};
use super::{TableWriteOp, WriteSchemaTrait};
use parking_lot::Mutex;

pub type WriteSchemaOp<Name> = (Name, Vec<u8>, Option<Vec<u8>>);
pub struct WriteSchemaNoSubkey<Name: TableNameTrait> {
    inner: Mutex<Vec<WriteSchemaOp<Name>>>,
}

impl<Name: TableNameTrait> Default for WriteSchemaNoSubkey<Name> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Name: TableNameTrait> WriteSchemaNoSubkey<Name> {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(vec![]),
        }
    }

    pub fn drain(self) -> Vec<WriteSchemaOp<Name>> {
        let mut inner = self.inner.lock();

        std::mem::take(&mut *inner)
    }
}

impl<Name: TableNameTrait> WriteSchemaNoSubkey<Name> {
    fn write_inner<T: TableSchema>(inner: &mut Vec<WriteSchemaOp<Name>>, op: TableWriteOp<T>)
    where
        T: TableSchema<TableName = Name>,
    {
        let (key, value) = op;
        let raw_key = <T::Key as Encode>::encode_cow(key).into_owned();
        let raw_value = value.map(|v| <T::Value as Encode>::encode_cow(v).into_owned());
        inner.push((T::NAME, raw_key, raw_value))
    }
}

impl<Name: TableNameTrait> WriteSchemaTrait<Name> for WriteSchemaNoSubkey<Name> {
    fn write<T: TableSchema<TableName = Name>>(&self, op: TableWriteOp<'_, T>) {
        let mut inner = self.inner.lock();
        Self::write_inner::<T>(&mut *inner, op)
    }

    fn write_batch<'a, T: TableSchema<TableName = Name>>(
        &self,
        changes: impl IntoIterator<Item = TableWriteOp<'a, T>>,
    ) {
        let mut inner = self.inner.lock();
        for op in changes {
            Self::write_inner::<T>(&mut *inner, op)
        }
    }
}
