mod no_sub_key;

pub use no_sub_key::WriteSchemaNoSubkey;

use super::TableSchema;
use auto_impl::auto_impl;
use std::borrow::Cow;

pub type TableWriteOp<'a, T> = (
    Cow<'a, <T as TableSchema>::Key>,
    Option<Cow<'a, <T as TableSchema>::Value>>,
);

#[auto_impl(&)]
pub trait WriteSchemaTrait: Send + Sync {
    fn write<T: TableSchema>(&self, op: TableWriteOp<'_, T>);
    fn write_batch<'a, T: TableSchema>(&self, changes: impl Iterator<Item = TableWriteOp<'a, T>>);
}

type A = Box<dyn WriteSchemaTrait>;

// pub trait WriteSchemaTableTrait<T: TableSchema>: Send + Sync {
//     fn write_table(&self, op: TableWriteOp<'_, T>);
//     fn write_table_batch<'a>(&self, changes: Box<dyn 'a + Iterator<Item = TableWriteOp<'_, T>>>);
// }

// impl<T: TableSchema, U: WriteSchemaTrait> WriteSchemaTableTrait<T> for U {
//     fn write_table(&self, op: TableWriteOp<'_, T>) {
//         WriteSchemaTrait::write::<T>(self,  op)
//     }

//     fn write_table_batch<'a>(&self, changes: Box<dyn 'a + Iterator<Item = TableWriteOp<'_, T>>>) {
//         WriteSchemaTrait::write_batch::<T>(&self, changes)
//     }
// }
