use std::borrow::Cow;

use super::HistoryIndexKey;
use crate::backends::serde::{Decode, Encode, EncodeSubKey};
use crate::errors::{DecResult, DecodeError};
use crate::middlewares::HistoryNumber;

/// Byte-stuffing: encode raw key bytes into an order-preserving, self-terminating form.
///
/// Escaping rule: every `0x00` in the input becomes `[0x00, 0xFF]`.
/// A `[0x00, 0x00]` terminator is appended at the end.
///
/// Properties:
/// - **Order-preserving**: non-zero bytes compare directly; when one key is a prefix of
///   another, the shorter key's terminator `0x00 0x00` is less than the longer key's next
///   byte (either non-zero, or the escaped `0x00 0xFF`).
/// - **Self-terminating**: `0x00 0x00` never appears inside the escaped body because all
///   original `0x00` bytes are escaped to `0x00 0xFF`.
fn byte_stuff(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(raw.len() + 2);
    for &b in raw {
        out.push(b);
        if b == 0x00 {
            out.push(0xFF);
        }
    }
    // Terminator
    out.push(0x00);
    out.push(0x00);
    out
}

/// Reverse byte-stuffing.
///
/// Returns `(decoded_key_bytes, bytes_consumed)` where `bytes_consumed` includes the
/// terminator. Returns `Err` on malformed input.
fn byte_unstuff(input: &[u8]) -> DecResult<(Vec<u8>, usize)> {
    let mut out = Vec::new();
    let mut i = 0;
    loop {
        if i >= input.len() {
            return Err(DecodeError::IncorrectLength);
        }
        if input[i] == 0x00 {
            if i + 1 >= input.len() {
                return Err(DecodeError::IncorrectLength);
            }
            match input[i + 1] {
                0x00 => {
                    // Terminator
                    i += 2;
                    break;
                }
                0xFF => {
                    // Escaped 0x00
                    out.push(0x00);
                    i += 2;
                }
                _ => return Err(DecodeError::IncorrectLength),
            }
        } else {
            out.push(input[i]);
            i += 1;
        }
    }
    Ok((out, i))
}

impl<K: Clone + Encode> Encode for HistoryIndexKey<K> {
    fn encode(&self) -> Cow<[u8]> {
        let encoded_key = self.0.encode();
        let encoded_version = self.1.encode();

        // [escaped_key + terminator] + [version]
        let mut out = byte_stuff(encoded_key.as_ref());
        out.extend_from_slice(encoded_version.as_ref());

        Cow::Owned(out)
    }
}

impl<K: Clone + Encode + ToOwned<Owned = K>> EncodeSubKey for HistoryIndexKey<K> {
    const HAVE_SUBKEY: bool = true;

    fn encode_subkey(&self) -> (Cow<[u8]>, Cow<[u8]>) {
        // Prefix: [escaped_key + terminator]; Suffix: [version]
        let encoded_key = self.0.encode();
        (Cow::Owned(byte_stuff(encoded_key.as_ref())), self.1.encode())
    }

    fn encode_subkey_owned(input: <Self as ToOwned>::Owned) -> (Vec<u8>, Vec<u8>) {
        let encoded_key = K::encode_owned(input.0);
        (
            byte_stuff(&encoded_key),
            HistoryNumber::encode_owned(input.1),
        )
    }
}

impl<K: Clone + Decode + ToOwned<Owned = K>> Decode for HistoryIndexKey<K> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        const VER_SIZE: usize = std::mem::size_of::<HistoryNumber>();

        let (key_bytes, consumed) = byte_unstuff(input)?;

        if input.len() - consumed != VER_SIZE {
            return Err(DecodeError::IncorrectLength);
        }

        let version_raw = &input[consumed..];
        let key = K::decode(&key_bytes)?;
        let version = HistoryNumber::decode(version_raw)?;
        Ok(Cow::Owned(HistoryIndexKey(
            key.into_owned(),
            version.into_owned(),
        )))
    }

    fn decode_owned(input: Vec<u8>) -> DecResult<Self> {
        const VER_SIZE: usize = std::mem::size_of::<HistoryNumber>();

        let (key_bytes, consumed) = byte_unstuff(&input)?;

        if input.len() - consumed != VER_SIZE {
            return Err(DecodeError::IncorrectLength);
        }

        let version_raw = input[consumed..].to_vec();
        let key = K::decode_owned(key_bytes)?;
        let version = HistoryNumber::decode_owned(version_raw)?;
        Ok(HistoryIndexKey(key, version))
    }
}

#[cfg(test)]
mod tests_byte_stuff {
    use super::{byte_stuff, byte_unstuff};

    #[test]
    fn test_roundtrip() {
        let cases: Vec<Vec<u8>> = vec![
            vec![],
            vec![1, 2, 3],
            vec![0],
            vec![0, 0],
            vec![0, 0xFF],
            vec![0xFF, 0, 0, 0xFF],
            vec![0; 64],
        ];
        for raw in cases {
            let stuffed = byte_stuff(&raw);
            let (decoded, consumed) = byte_unstuff(&stuffed).unwrap();
            assert_eq!(decoded, raw);
            assert_eq!(consumed, stuffed.len());
        }
    }

    #[test]
    fn test_order_different_lengths() {
        // [2] > [1, 3] lexicographically, encoding must agree
        let a = byte_stuff(&[2]);
        let b = byte_stuff(&[1, 3]);
        assert!(a > b, "expected {:?} > {:?}", a, b);
    }

    #[test]
    fn test_order_prefix_relation() {
        // [1] < [1, 0] lexicographically
        let a = byte_stuff(&[1]);
        let b = byte_stuff(&[1, 0]);
        assert!(a < b, "expected {:?} < {:?}", a, b);
    }

    #[test]
    fn test_order_with_zeros() {
        // [0] < [0, 0]
        let a = byte_stuff(&[0]);
        let b = byte_stuff(&[0, 0]);
        assert!(a < b, "expected {:?} < {:?}", a, b);

        // [0, 0] < [0, 1]
        let c = byte_stuff(&[0, 1]);
        assert!(b < c, "expected {:?} < {:?}", b, c);
    }
}

#[cfg(test)]
mod tests_history_index_order {
    use std::borrow::Cow;
    use std::sync::Arc;

    use crate::backends::MockTableName;
    use crate::backends::{DatabaseTrait, TableRead, WrappedInMemoryDb, WriteSchemaTrait, TableSchema};
    use crate::errors::{DatabaseError, Result, DecResult};
    use crate::backends::serde::{Encode, Decode};

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
        type TableName = MockTableName;
        const NAME: MockTableName = MockTableName::MockTable1;
        type Key = HistoryIndexKey<BoundedVec>;
        type Value = Vec<u8>;
    }

    fn hk(bytes: Vec<u8>, n: HistoryNumber) -> HistoryIndexKey<BoundedVec> {
        HistoryIndexKey(BoundedVec(bytes), n)
    }

    const LATEST: u64 = u64::MAX;

    /// Encode-decode roundtrip for HistoryIndexKey
    #[test]
    fn test_encode_decode_roundtrip() {
        let cases = vec![
            hk(vec![], 0),
            hk(vec![], LATEST),
            hk(vec![0], 42),
            hk(vec![0, 0, 0], LATEST),
            hk(vec![1, 2, 3], 100),
            hk(vec![0xFF; 8], 0),
        ];
        for original in cases {
            let encoded = original.encode();
            let decoded = HistoryIndexKey::<BoundedVec>::decode(&encoded)
                .unwrap()
                .into_owned();
            assert_eq!(decoded, original);
        }
    }

    fn test_data() -> Vec<(HistoryIndexKey<BoundedVec>, Vec<u8>)> {
        vec![
            (hk(vec![82, 81, 4, 81, 249, 4], LATEST), b"v_82".to_vec()),
            (hk(vec![171; 6], LATEST), b"v_171".to_vec()),
            (hk(vec![], LATEST), b"v_empty".to_vec()),
        ]
    }

    fn seed<DB: DatabaseTrait<MockTableName>>(db: &DB) -> Result<()> {
        let schema = DB::write_schema();
        for (k, v) in test_data() {
            schema.write::<MockHistoryTable>((Cow::Owned(k), Some(Cow::Owned(v))));
        }
        db.commit(schema)?;
        Ok(())
    }

    /// Same-length keys and empty key: verify order matches logical Ord
    fn run_basic_order_assertions<DB: DatabaseTrait<MockTableName>>(db: DB) -> Result<()> {
        seed(&db)?;
        let reader = Arc::new(db).view::<MockHistoryTable>()?;

        let iter_result: Vec<_> = reader
            .iter(&hk(vec![], 0))?
            .map(|kv| kv.map(|(k, _)| k.into_owned()))
            .collect::<std::result::Result<Vec<_>, DatabaseError>>()?;

        assert_eq!(iter_result.len(), 3);
        assert_eq!(iter_result[0], hk(vec![], LATEST));
        assert_eq!(iter_result[1], hk(vec![82, 81, 4, 81, 249, 4], LATEST));
        assert_eq!(iter_result[2], hk(vec![171; 6], LATEST));
        Ok(())
    }

    /// Variable-length keys: the previous encoding (length-prefix) got this wrong.
    fn run_variable_length_order_assertions<DB: DatabaseTrait<MockTableName>>(db: DB) -> Result<()> {
        let schema = DB::write_schema();
        // Insert keys of different lengths whose ordering is swapped by length-prefix encoding
        let entries = vec![
            (hk(vec![2], LATEST), b"v2".to_vec()),       // logically > [1,3]
            (hk(vec![1, 3], LATEST), b"v13".to_vec()),    // logically < [2]
            (hk(vec![1], LATEST), b"v1".to_vec()),        // logically < [1,3]
            (hk(vec![1, 0], LATEST), b"v10".to_vec()),    // logically < [1,3], > [1]
        ];
        for (k, v) in &entries {
            schema.write::<MockHistoryTable>((Cow::Owned(k.clone()), Some(Cow::Owned(v.clone()))));
        }
        db.commit(schema)?;

        let reader = Arc::new(db).view::<MockHistoryTable>()?;

        let iter_result: Vec<_> = reader
            .iter(&hk(vec![], 0))?
            .map(|kv| kv.map(|(k, _)| k.into_owned()))
            .collect::<std::result::Result<Vec<_>, DatabaseError>>()?;

        let expected = vec![
            hk(vec![1], LATEST),
            hk(vec![1, 0], LATEST),
            hk(vec![1, 3], LATEST),
            hk(vec![2], LATEST),
        ];
        assert_eq!(iter_result, expected);
        Ok(())
    }

    #[test]
    fn test_basic_order_in_memory() {
        run_basic_order_assertions(WrappedInMemoryDb::empty()).unwrap();
    }

    #[test]
    fn test_variable_length_order_in_memory() {
        run_variable_length_order_assertions(WrappedInMemoryDb::empty()).unwrap();
    }

    #[test]
    fn test_basic_order_rocksdb() {
        use crate::middlewares::{clear_dir, clear_dir_then_create};
        use crate::backends::impls::kvdb_rocksdb::WrappedRocksDb;
        let temp_dir = "__test_rocksdb_hik_basic_order";
        clear_dir_then_create(temp_dir);
        let db = WrappedRocksDb::open(temp_dir).unwrap();
        run_basic_order_assertions(db).unwrap();
        clear_dir(temp_dir);
    }

    #[test]
    fn test_variable_length_order_rocksdb() {
        use crate::middlewares::{clear_dir, clear_dir_then_create};
        use crate::backends::impls::kvdb_rocksdb::WrappedRocksDb;
        let temp_dir = "__test_rocksdb_hik_varlen_order";
        clear_dir_then_create(temp_dir);
        let db = WrappedRocksDb::open(temp_dir).unwrap();
        run_variable_length_order_assertions(db).unwrap();
        clear_dir(temp_dir);
    }
}