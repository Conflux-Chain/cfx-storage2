use std::borrow::Cow;

use super::{
    super::{
        DecResult, Decode, DecodeError, Encode, FixedLengthEncoded, PendingKeyValueSchema,
        RecoverRecord, ValueEntry,
    },
    codec::{decode_option, decode_option_owned, encode_option},
};

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum WalValue<S: PendingKeyValueSchema> {
    MetaValue {
        commit_id: S::CommitId,
        maybe_parent_cid: Option<S::CommitId>,
        /// The total number of MapValue records associated with this commit. This is only for checking consistency.
        map_value_count: u64,
    },
    MapValue(RecoverRecord<S>),
}

impl<S: PendingKeyValueSchema> Encode for RecoverRecord<S> {
    fn encode(&self) -> Cow<[u8]> {
        let encoded_value_entry = self.value.encode();
        let encoded_last_cid = encode_option(&self.last_commit_id);

        let len_prefix = (encoded_value_entry.len() as u32).to_be_bytes();

        let mut vec = Vec::with_capacity(4 + encoded_value_entry.len() + encoded_last_cid.len());
        vec.extend_from_slice(&len_prefix);
        vec.extend_from_slice(encoded_value_entry.as_ref());
        vec.extend_from_slice(encoded_last_cid.as_ref());

        Cow::Owned(vec)
    }
}

impl<S: PendingKeyValueSchema> Decode for RecoverRecord<S> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.len() < 4 {
            return Err(DecodeError::IncorrectLength);
        }
        let value_entry_len = u32::from_be_bytes(input[0..4].try_into().unwrap()) as usize;

        let cursor = 4 + value_entry_len;
        if input.len() < cursor {
            return Err(DecodeError::IncorrectLength);
        }

        let value_entry_data = &input[4..cursor];
        let value = ValueEntry::<S::Value>::decode(value_entry_data)?;

        let remaining_data = &input[cursor..];
        let last_commit_id = decode_option::<S::CommitId>(remaining_data)?;

        Ok(Cow::Owned(RecoverRecord {
            value: value.into_owned(),
            last_commit_id: last_commit_id.map(|cow| cow.into_owned()),
        }))
    }

    fn decode_owned(mut input: Vec<u8>) -> DecResult<Self> {
        if input.len() < 4 {
            return Err(DecodeError::IncorrectLength);
        }
        let value_entry_len = u32::from_be_bytes(input[0..4].try_into().unwrap()) as usize;

        let total_prefix_len = 4 + value_entry_len;
        if input.len() < total_prefix_len {
            return Err(DecodeError::IncorrectLength);
        }

        let last_cid_data = input.split_off(total_prefix_len);

        let value_entry_data = input.split_off(4);

        let value = ValueEntry::<S::Value>::decode_owned(value_entry_data)?;
        let last_commit_id = decode_option_owned::<S::CommitId>(last_cid_data)?;

        Ok(RecoverRecord {
            value,
            last_commit_id,
        })
    }
}

impl<S: PendingKeyValueSchema> Encode for WalValue<S> {
    fn encode(&self) -> Cow<[u8]> {
        match self {
            WalValue::MetaValue {
                commit_id,
                maybe_parent_cid,
                map_value_count,
            } => {
                let encoded_cid = commit_id.encode();
                let encoded_parent_opt = encode_option(maybe_parent_cid);
                let encoded_count = map_value_count.encode();

                let mut vec = Vec::with_capacity(
                    1 // Tag
                    + S::CommitId::LENGTH
                    + encoded_parent_opt.len()
                    + u64::LENGTH,
                );

                vec.push(0x00); // MetaValue tag
                vec.extend_from_slice(encoded_cid.as_ref());
                vec.extend_from_slice(encoded_parent_opt.as_ref());
                vec.extend_from_slice(encoded_count.as_ref());
                Cow::Owned(vec)
            }
            WalValue::MapValue(record) => {
                let encoded_record = record.encode();
                let mut vec = Vec::with_capacity(1 + encoded_record.len());
                vec.push(0x01); // MapValue tag
                vec.extend_from_slice(encoded_record.as_ref());
                Cow::Owned(vec)
            }
        }
    }
}

impl<S: PendingKeyValueSchema> Decode for WalValue<S> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.is_empty() {
            return Err(DecodeError::IncorrectLength);
        }

        let tag = input[0];
        let data = &input[1..];

        match tag {
            0x00 => {
                // MetaValue
                let cid_len = S::CommitId::LENGTH;
                let count_len = u64::LENGTH;
                let fixed_parts_len = cid_len + count_len;

                if data.len() < fixed_parts_len {
                    return Err(DecodeError::IncorrectLength);
                }

                let (commit_id_data, rest) = data.split_at(cid_len);

                let (parent_cid_data, count_data) = rest.split_at(rest.len() - count_len);

                let commit_id = S::CommitId::decode(commit_id_data)?;
                let maybe_parent_cid = decode_option::<S::CommitId>(parent_cid_data)?;
                let map_value_count = u64::decode(count_data)?;

                Ok(Cow::Owned(WalValue::MetaValue {
                    commit_id: commit_id.into_owned(),
                    maybe_parent_cid: maybe_parent_cid.map(|cow| cow.into_owned()),
                    map_value_count: map_value_count.into_owned(),
                }))
            }
            0x01 => {
                // MapValue
                let record = RecoverRecord::<S>::decode(data)?;
                Ok(Cow::Owned(WalValue::MapValue(record.into_owned())))
            }
            _ => Err(DecodeError::Custom("Invalid WalValue variant prefix")),
        }
    }

    fn decode_owned(mut input: Vec<u8>) -> DecResult<Self> {
        if input.is_empty() {
            return Err(DecodeError::IncorrectLength);
        }

        let tag = input.remove(0);

        match tag {
            0x00 => {
                // MetaValue
                let cid_len: usize = S::CommitId::LENGTH;
                let count_len = u64::LENGTH;
                let fixed_parts_len = cid_len + count_len;

                if input.len() < fixed_parts_len {
                    return Err(DecodeError::IncorrectLength);
                }

                let mut rest = input.split_off(cid_len);
                let commit_id_data = input;

                let count_data = rest.split_off(rest.len() - count_len);
                let parent_cid_data = rest;

                let commit_id = S::CommitId::decode_owned(commit_id_data)?;
                let maybe_parent_cid = decode_option_owned::<S::CommitId>(parent_cid_data)?;
                let map_value_count = u64::decode_owned(count_data)?;

                Ok(WalValue::MetaValue {
                    commit_id,
                    maybe_parent_cid,
                    map_value_count,
                })
            }
            0x01 => {
                // MapValue
                let record = RecoverRecord::<S>::decode_owned(input)?;
                Ok(WalValue::MapValue(record))
            }
            _ => Err(DecodeError::Custom("Invalid WalValue variant prefix")),
        }
    }
}

#[cfg(test)]
mod tests {
    use ethereum_types::H256;

    use super::super::super::test_util::*;
    use super::*;

    #[test]
    fn meta_value_no_parent_roundtrip_borrowed_and_owned() {
        let original = WalValue::<TestSchema>::MetaValue {
            commit_id: H256::from([0x11; 32]),
            maybe_parent_cid: None,
            map_value_count: 42,
        };

        // Encode
        let original_clone = original.clone();
        let enc = original_clone.encode();
        assert_eq!(enc.as_ref()[0], 0x00, "tag for MetaValue should be 0x00");

        // Roundtrip
        test_encode_decode_round_trip(vec![original]);
    }

    #[test]
    fn meta_value_with_parent_roundtrip_borrowed_and_owned() {
        let original = WalValue::<TestSchema>::MetaValue {
            commit_id: H256::from([0x22; 32]),
            maybe_parent_cid: Some(H256::from([0x33; 32])),
            map_value_count: u64::MAX - 7,
        };

        let original_clone = original.clone();
        let enc = original_clone.encode();
        assert_eq!(enc.as_ref()[0], 0x00);

        // Roundtrip
        test_encode_decode_round_trip(vec![original]);
    }

    #[test]
    fn meta_value_encoding_structure() {
        // Validate meta encoding exact layout: [0x00][commit_id(32)][opt_parent][count(8)]
        let commit_id = H256::from([0x55; 32]);
        let parent = H256::from([0x66; 32]);
        let count = 123456789u64;

        let w = WalValue::<TestSchema>::MetaValue {
            commit_id,
            maybe_parent_cid: Some(parent),
            map_value_count: count,
        };

        let enc = w.encode().into_owned();
        assert_eq!(enc[0], 0x00);

        // Commit id bytes
        let cid_enc = <H256 as Encode>::encode(&commit_id);
        assert_eq!(&enc[1..33], cid_enc.as_ref());

        // Option discriminant for Some
        assert_eq!(enc[33], 0x01);

        // Parent bytes
        let parent_enc = <H256 as Encode>::encode(&parent);
        assert_eq!(&enc[34..66], parent_enc.as_ref());

        // Count bytes
        assert_eq!(&enc[66..74], &u64::to_be_bytes(count));

        assert_eq!(enc.len(), 1 + 32 + 1 + 32 + 8);
    }

    #[test]
    fn map_value_with_value_and_last_cid_roundtrip_borrowed_and_owned() {
        let value_entry = ValueEntry::<Box<[u8]>>::Value(k(&[0xDE, 0xAD, 0xBE, 0xEF]));
        let record = RecoverRecord::<TestSchema> {
            value: value_entry,
            last_commit_id: Some(H256::from([0x44; 32])),
        };
        let original = WalValue::<TestSchema>::MapValue(record.clone());

        let enc = original.encode();
        assert_eq!(enc.as_ref()[0], 0x01);

        // Roundtrip
        test_encode_decode_round_trip(vec![original]);

        // RecoverRecord direct roundtrip (borrowed)
        test_encode_decode_round_trip(vec![record]);
    }

    #[test]
    fn map_value_deleted_without_last_cid_roundtrip_borrowed_and_owned() {
        let record = RecoverRecord::<TestSchema> {
            value: ValueEntry::<Box<[u8]>>::Deleted,
            last_commit_id: None,
        };
        let original = WalValue::<TestSchema>::MapValue(record.clone());

        let enc = original.encode();
        assert_eq!(enc.as_ref()[0], 0x01);

        // Roundtrip
        test_encode_decode_round_trip(vec![original]);

        // RecoverRecord direct roundtrip
        test_encode_decode_round_trip(vec![record]);
    }

    #[test]
    fn map_value_encoding_structure_is_record_encoding() {
        // Ensure that MapValue payload equals RecoverRecord encoding
        let record = RecoverRecord::<TestSchema> {
            value: ValueEntry::<Box<[u8]>>::Value(k(&[1, 2, 3])),
            last_commit_id: None,
        };
        let w = WalValue::<TestSchema>::MapValue(record.clone());

        let enc = w.encode().into_owned();
        assert_eq!(enc[0], 0x01);

        let rr_enc = record.encode().into_owned();
        assert_eq!(&enc[1..], rr_enc.as_slice());
    }

    #[test]
    fn wal_value_decode_errors() {
        // Empty input
        assert!(matches!(
            WalValue::<TestSchema>::decode(&[]),
            Err(DecodeError::IncorrectLength)
        ));
        assert!(matches!(
            WalValue::<TestSchema>::decode_owned(Vec::new()),
            Err(DecodeError::IncorrectLength)
        ));

        // Unknown tag
        assert!(matches!(
            WalValue::<TestSchema>::decode(&[0xFF]),
            Err(DecodeError::Custom("Invalid WalValue variant prefix"))
        ));
        assert!(matches!(
            WalValue::<TestSchema>::decode_owned(vec![0xFF]),
            Err(DecodeError::Custom("Invalid WalValue variant prefix"))
        ));

        // MetaValue too short for fixed parts (needs at least 32 + 8 bytes after tag)
        assert!(matches!(
            WalValue::<TestSchema>::decode(&[0x00, 0xAA]),
            Err(DecodeError::IncorrectLength)
        ));
        assert!(matches!(
            WalValue::<TestSchema>::decode_owned(vec![0x00, 0xAA]),
            Err(DecodeError::IncorrectLength)
        ));

        // MapValue with incomplete RecoverRecord (insufficient for 4-byte length prefix)
        assert!(matches!(
            WalValue::<TestSchema>::decode(&[0x01, 0x00]),
            Err(DecodeError::IncorrectLength)
        ));
        assert!(matches!(
            WalValue::<TestSchema>::decode_owned(vec![0x01, 0x00]),
            Err(DecodeError::IncorrectLength)
        ));
    }

    #[test]
    fn recover_record_decode_errors() {
        // Too short for 4-byte length prefix
        assert!(matches!(
            RecoverRecord::<TestSchema>::decode(&[0x00, 0x01, 0x02]),
            Err(DecodeError::IncorrectLength)
        ));
        assert!(matches!(
            RecoverRecord::<TestSchema>::decode_owned(vec![0x00, 0x01, 0x02]),
            Err(DecodeError::IncorrectLength)
        ));

        // Length prefix longer than available
        let mut bad = Vec::new();
        bad.extend_from_slice(&10u32.to_be_bytes());
        bad.extend_from_slice(&[0xFF, 0xEE]);
        assert!(matches!(
            RecoverRecord::<TestSchema>::decode(&bad),
            Err(DecodeError::IncorrectLength)
        ));
        assert!(matches!(
            RecoverRecord::<TestSchema>::decode_owned(bad),
            Err(DecodeError::IncorrectLength)
        ));
    }

    #[test]
    fn mixed_round_trips_without_eq() {
        let cases = vec![
            WalValue::<TestSchema>::MetaValue {
                commit_id: H256::from([0x00; 32]),
                maybe_parent_cid: None,
                map_value_count: 0,
            },
            WalValue::<TestSchema>::MetaValue {
                commit_id: H256::from([0xAB; 32]),
                maybe_parent_cid: Some(H256::from([0xCD; 32])),
                map_value_count: 1,
            },
            WalValue::<TestSchema>::MapValue(RecoverRecord {
                value: ValueEntry::<Box<[u8]>>::Deleted,
                last_commit_id: None,
            }),
            WalValue::<TestSchema>::MapValue(RecoverRecord {
                value: ValueEntry::<Box<[u8]>>::Value(k(&[])),
                last_commit_id: Some(H256::from([0xEF; 32])),
            }),
            WalValue::<TestSchema>::MapValue(RecoverRecord {
                value: ValueEntry::<Box<[u8]>>::Value(k(&[9, 8, 7, 6, 5, 4, 3, 2, 1])),
                last_commit_id: None,
            }),
        ];

        test_encode_decode_round_trip(cases);
    }
}
