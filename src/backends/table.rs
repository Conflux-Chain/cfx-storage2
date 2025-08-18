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
pub type TableReader<'a, T> = Arc<dyn 'a + TableRead<T> + Send + Sync>;

#[auto_impl(&, Arc)]
pub trait TableRead<T: TableSchema> {
    fn get(&self, key: &T::Key) -> Result<Option<Cow<T::Value>>>;

    fn iter<'a>(&'a self, key: &T::Key) -> Result<TableIter<'a, '_, T>>;

    fn iter_from_start(&self) -> Result<TableIter<T>>;
    fn iter_rev_from_end(&self) -> Result<TableIter<T>>;
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
    use std::borrow::Cow;
    use std::sync::Arc;

    use crate::backends::{DatabaseTrait, InMemoryDatabase, TableRead, WriteSchemaTrait};
    use crate::errors::{DatabaseError, Result};
    use crate::middlewares::empty_rocksdb;

    use super::{TableName, TableSchema};

    #[derive(Clone, Copy)]
    struct MockTable1;
    impl TableSchema for MockTable1 {
        const NAME: TableName = TableName::MockTable1;
        type Key = [u8];
        type Value = [u8];
    }

    #[derive(Clone, Copy)]
    struct MockTable2;
    impl TableSchema for MockTable2 {
        const NAME: TableName = TableName::MockTable2;
        type Key = [u8];
        type Value = [u8];
    }

    #[derive(Clone, Copy)]
    struct MockTable3;
    impl TableSchema for MockTable3 {
        const NAME: TableName = TableName::MockTable3;
        type Key = [u8];
        type Value = [u8];
    }

    /// Generic test to verify `DatabaseTrait` implementations.
    ///
    /// This version writes to three separate tables but only reads from one
    /// to ensure that iterators and getters are properly isolated to the
    /// specified table (Column Family).
    fn test_table_read_behavior<DB: DatabaseTrait>(mut db: DB) -> Result<()> {
        let test_data1: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (b"key1:10".to_vec(), b"value1:ten".to_vec()),
            (b"key1:20".to_vec(), b"value1:twenty".to_vec()),
        ];

        let test_data2: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (b"key2:10".to_vec(), b"value2:ten".to_vec()),
            (b"key2:20".to_vec(), b"value2:twenty".to_vec()),
            (b"key2:30".to_vec(), b"value2:thirty".to_vec()),
            (b"key2:40".to_vec(), b"value2:forty".to_vec()),
        ];

        let test_data3: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (b"key3:10".to_vec(), b"value3:ten".to_vec()),
            (b"key3:20".to_vec(), b"value3:twenty".to_vec()),
        ];

        let schema = DB::write_schema();
        for (k, v) in &test_data1 {
            schema.write::<MockTable1>((Cow::Borrowed(k), Some(Cow::Borrowed(v))));
        }
        for (k, v) in &test_data2 {
            schema.write::<MockTable2>((Cow::Borrowed(k), Some(Cow::Borrowed(v))));
        }
        for (k, v) in &test_data3 {
            schema.write::<MockTable3>((Cow::Borrowed(k), Some(Cow::Borrowed(v))));
        }
        db.commit(schema)?;

        // Get a read-only view of MockTable2 only.
        let reader = Arc::new(db).view::<MockTable2>()?;

        // --- Test: get() ---
        let val = reader
            .get(b"key2:20")?
            .expect("Value for key2:20 should exist");
        assert_eq!(*val, b"value2:twenty".to_vec());

        // Verify that keys from other tables are not accessible.
        let val_none_other_table = reader.get(b"key1:10")?;
        assert!(
            val_none_other_table.is_none(),
            "Should not find a key from another table"
        );

        // Verify that a non-existent key returns None.
        let val_none = reader.get(b"key2:99")?;
        assert!(val_none.is_none(), "Value for key2:99 should not exist");

        // --- Test: iter_from_start() ---
        // The iterator should only yield items from MockTable2.
        let collected_forward: Vec<_> = reader
            .iter_from_start()?
            .map(|kv| kv.map(|(k, v)| (k.into_owned(), v.into_owned())))
            .collect::<std::result::Result<Vec<_>, DatabaseError>>()?;
        assert_eq!(collected_forward, test_data2);

        // --- Test: iter_rev_from_end() ---
        let mut expected_reversed = test_data2.clone();
        expected_reversed.reverse();
        let collected_reversed: Vec<_> = reader
            .iter_rev_from_end()?
            .map(|kv| kv.map(|(k, v)| (k.into_owned(), v.into_owned())))
            .collect::<std::result::Result<Vec<_>, DatabaseError>>()?;
        assert_eq!(collected_reversed, expected_reversed);

        // --- Test: iter() ---
        let iter_from_20: Vec<_> = reader
            .iter(b"key2:20")?
            .map(|kv| kv.map(|(k, v)| (k.into_owned(), v.into_owned())))
            .collect::<std::result::Result<Vec<_>, DatabaseError>>()?;
        assert_eq!(iter_from_20, &test_data2[1..]);

        let iter_from_25: Vec<_> = reader
            .iter(b"key2:25")?
            .map(|kv| kv.map(|(k, v)| (k.into_owned(), v.into_owned())))
            .collect::<std::result::Result<Vec<_>, DatabaseError>>()?;
        assert_eq!(iter_from_25, &test_data2[2..]);

        let iter_from_50: Vec<_> = reader
            .iter(b"key2:50")?
            .map(|kv| kv.map(|(k, v)| (k.into_owned(), v.into_owned())))
            .collect::<std::result::Result<Vec<_>, DatabaseError>>()?;
        assert!(iter_from_50.is_empty());

        Ok(())
    }

    #[test]
    fn test_in_memory_database_behavior() -> Result<()> {
        let db = InMemoryDatabase::empty();
        test_table_read_behavior(db)
    }

    #[test]
    fn test_rocksdb_database_behavior() -> Result<()> {
        let temp_dir = "__test_rocksdb_table_read";
        let db = empty_rocksdb(temp_dir)?;
        test_table_read_behavior(db)
    }
}
