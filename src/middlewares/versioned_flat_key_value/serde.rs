use std::borrow::Cow;

use super::HistoryIndexKey;
use crate::backends::serde::{Decode, Encode, EncodeSubKey, FixedLengthEncoded};
use crate::errors::{DecResult, DecodeError};
use crate::middlewares::HistoryNumber;

impl<K: Clone + Encode> Encode for HistoryIndexKey<K> {
    fn encode(&self) -> Cow<[u8]> {
        let encoded_key = self.0.encode();
        let encoded_version = self.1.encode();
        
        // Assumption: The number of bytes of Key is not larger than u32::MAX.
        let key_len = (encoded_key.len() as u32).to_be_bytes();

        // [Key_LEN] + [Key] + [Version]
        let mut out = Vec::with_capacity(4 + encoded_key.len() + encoded_version.len());
        out.extend_from_slice(&key_len);
        out.extend_from_slice(encoded_key.as_ref());
        out.extend_from_slice(encoded_version.as_ref());

        Cow::Owned(out)
    }
}

impl<K: Clone + FixedLengthEncoded> FixedLengthEncoded for HistoryIndexKey<K> {
    const LENGTH: usize = 4 + K::LENGTH + std::mem::size_of::<HistoryNumber>();
}

impl<K: Clone + Encode + ToOwned<Owned = K>> EncodeSubKey for HistoryIndexKey<K> {
    const HAVE_SUBKEY: bool = true;

    fn encode_subkey(&self) -> (Cow<[u8]>, Cow<[u8]>) {
        // Prefix: [KeyLen + Key]; Suffix: [Version]
        let encoded_key = self.0.encode();
        let key_len = (encoded_key.len() as u32).to_be_bytes();
        
        let mut prefix = Vec::with_capacity(4 + encoded_key.len());
        prefix.extend_from_slice(&key_len);
        prefix.extend_from_slice(encoded_key.as_ref());

        (Cow::Owned(prefix), self.1.encode())
    }

    fn encode_subkey_owned(input: <Self as ToOwned>::Owned) -> (Vec<u8>, Vec<u8>) {
        let encoded_key = K::encode_owned(input.0);
        let key_len = (encoded_key.len() as u32).to_be_bytes();
        
        let mut prefix = Vec::with_capacity(4 + encoded_key.len());
        prefix.extend_from_slice(&key_len);
        prefix.extend_from_slice(&encoded_key);

        (
            prefix,
            HistoryNumber::encode_owned(input.1),
        )
    }
}

impl<K: Clone + Decode + ToOwned<Owned = K>> Decode for HistoryIndexKey<K> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        const LEN_SIZE: usize = 4;
        const VER_SIZE: usize = std::mem::size_of::<HistoryNumber>();
        
        if input.len() < LEN_SIZE + VER_SIZE {
            return Err(DecodeError::IncorrectLength);
        }

        // Safe after the above length check.
        let len_bytes: [u8; 4] = input[0..4].try_into().unwrap();

        let key_len = u32::from_be_bytes(len_bytes) as usize;

        if input.len() != LEN_SIZE + key_len + VER_SIZE {
             return Err(DecodeError::IncorrectLength);
        }

        // Safe after the above length check.
        let key_raw = &input[LEN_SIZE .. LEN_SIZE + key_len];
        let version_raw = &input[LEN_SIZE + key_len ..];

        let (key, version) = (K::decode(key_raw)?, HistoryNumber::decode(version_raw)?);
        Ok(Cow::Owned(HistoryIndexKey(
            key.into_owned(),
            version.into_owned(),
        )))
    }

    fn decode_owned(mut input: Vec<u8>) -> DecResult<Self> {
        const LEN_SIZE: usize = 4;
        const VER_SIZE: usize = std::mem::size_of::<HistoryNumber>();

        if input.len() < LEN_SIZE + VER_SIZE {
            return Err(DecodeError::IncorrectLength);
        }

        // Safe after the above length check.
        let len_bytes: [u8; 4] = input[0..4].try_into().unwrap();

        let key_len = u32::from_be_bytes(len_bytes) as usize;

        if input.len() != LEN_SIZE + key_len + VER_SIZE {
             return Err(DecodeError::IncorrectLength);
        }

        // Safe after the above length check.
        let version_raw = input.split_off(input.len() - VER_SIZE);
        let key_raw = input.split_off(LEN_SIZE);

        let key = K::decode_owned(key_raw)?;
        let version = HistoryNumber::decode_owned(version_raw)?;
        Ok(HistoryIndexKey(key, version))
    }
}

// Test cases used to prevent errors related to the encoding method of `HistoryIndexKey` from recurring.
// See the `Encode` code for error details.
#[cfg(test)]
mod tests_history_index_order {
    use std::borrow::Cow;
    use std::sync::Arc;

    use crate::backends::{DatabaseTrait, TableName, TableRead, TableSchema, InMemoryDatabase, WriteSchemaTrait};
    use crate::errors::{DatabaseError, Result, DecResult};
    use crate::backends::serde::{Encode, Decode, FixedLengthEncoded};

    use super::{HistoryIndexKey, HistoryNumber};

    #[derive(Debug, Clone, PartialEq, Hash, PartialOrd, Eq, Ord)]
    pub struct BoundedVec(pub Vec<u8>);

    impl Encode for BoundedVec {
        fn encode(&self) -> Cow<[u8]> {
            self.0.encode()
        }
    }

    impl Decode for BoundedVec {
        fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
            Ok(Cow::Owned(BoundedVec(Vec::<u8>::decode(input)?.into_owned())))
        }
    }

    crate::subkey_not_support!(BoundedVec);

    #[derive(Clone, Copy)]
    struct MockHistoryTable;
    impl TableSchema for MockHistoryTable {
        const NAME: TableName = TableName::CommitID;
        type Key = HistoryIndexKey<BoundedVec>;
        type Value = Vec<u8>;
    }

    fn hk_bytes(bytes: Vec<u8>, n: HistoryNumber) -> HistoryIndexKey<BoundedVec> {
        HistoryIndexKey(BoundedVec(bytes), n)
    }

    fn test_data() -> Vec<(HistoryIndexKey<BoundedVec>, Vec<u8>)> {
        vec![
            // Key 1: [82, 81, 4, 81, 249, 4], u64::MAX
            (
                hk_bytes(vec![82, 81, 4, 81, 249, 4], 18446744073709551615), 
                b"value_for_82".to_vec()
            ),
            // Key 2: [171, 171, 171, 171, 171, 171], u64::MAX
            (
                hk_bytes(vec![171, 171, 171, 171, 171, 171], 18446744073709551615), 
                b"value_for_171".to_vec()
            ),
            // Key 3: [], u64::MAX
            (
                hk_bytes(vec![], 18446744073709551615), 
                b"value_for_empty".to_vec()
            ),
        ]
    }

    fn seed<DB: DatabaseTrait>(db: &mut DB) -> Result<()> {
        let schema = DB::write_schema();
        for (k, v) in test_data() {
            schema.write::<MockHistoryTable>((Cow::Owned(k), Some(Cow::Owned(v))));
        }
        db.commit(schema)?;
        Ok(())
    }

    fn run_read_assertions<DB: DatabaseTrait>(mut db: DB) -> Result<()> {
        seed(&mut db)?;
        let reader = db.view::<MockHistoryTable>()?;

        let range_query_key = hk_bytes(vec![], 0);
        println!("Querying with key: {:?}", range_query_key);

        let iter_result: Vec<_> = reader
            .iter(&range_query_key)?
            .map(|kv| kv.map(|(k, v)| (k.into_owned(), v.into_owned())))
            .collect::<std::result::Result<Vec<_>, DatabaseError>>()?;

        println!("Iter result count: {}", iter_result.len());
        for (i, (k, _)) in iter_result.iter().enumerate() {
            println!("Result [{}]: {:?}", i, k);
        }

        let expected_order = vec![
            hk_bytes(vec![], 18446744073709551615),
            hk_bytes(vec![82, 81, 4, 81, 249, 4], 18446744073709551615), 
            hk_bytes(vec![171, 171, 171, 171, 171, 171], 18446744073709551615),
        ];

        assert_eq!(iter_result.len(), 3);
        assert_eq!(iter_result[0].0, expected_order[0]);
        assert_eq!(iter_result[1].0, expected_order[1]);
        assert_eq!(iter_result[2].0, expected_order[2]);

        Ok(())
    }

    #[test]
    fn test_history_index_key_order_in_memory() {
        let db = InMemoryDatabase::empty();
        run_read_assertions(db).unwrap();
    }

    pub fn clear_dir(dir_path: &str) {
        if std::path::Path::new(dir_path).exists() {
            std::fs::remove_dir_all(dir_path).unwrap();
        }
    }

    pub fn clear_dir_then_create(dir_path: &str) {
        clear_dir(dir_path);

        std::fs::create_dir_all(dir_path).unwrap();
    }

    #[test]
    fn test_history_index_key_order_rocksdb() {
        use crate::backends::impls::kvdb_rocksdb::CachedDB;
        let temp_dir = "__test_rocksdb_history_index_key_order_custom";
        clear_dir_then_create(temp_dir);
        let db = CachedDB::open(TableName::max_index() + 1, temp_dir).unwrap();
        run_read_assertions(db).unwrap();
        clear_dir(temp_dir);
    }
}