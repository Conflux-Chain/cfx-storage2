use std::borrow::Cow;

use crate::middlewares::versioned_flat_key_value::pending_part::pending_schema::RecoverRecord;

use super::{
    decode_option, decode_option_owned, encode_option, DecResult, Decode, DecodeError, Encode,
    FixedLengthEncoded, PendingKeyValueSchema,
};

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SnapshotMapValue<S: PendingKeyValueSchema> {
    NodeMeta {
        /// Only consider nodes in tree. I.e., for the root, `node_parent_commit_id` is None, rather than the `parent_of_root`.
        parent_commit_id: Option<S::CommitId>,
        modifications_count: u64,
    },
    NodeMap(RecoverRecord<S>),
}

impl<S: PendingKeyValueSchema> Encode for SnapshotMapValue<S> {
    fn encode(&self) -> Cow<[u8]> {
        match self {
            SnapshotMapValue::NodeMeta {
                parent_commit_id,
                modifications_count,
            } => {
                let encoded_parent = encode_option(parent_commit_id);
                let encoded_count = modifications_count.encode();

                let mut vec = Vec::with_capacity(
                    1 // tag
                    + encoded_parent.len()
                    + u64::LENGTH,
                );
                vec.push(0x00); // NodeMeta tag
                vec.extend_from_slice(encoded_parent.as_ref());
                vec.extend_from_slice(encoded_count.as_ref());
                Cow::Owned(vec)
            }
            SnapshotMapValue::NodeMap(record) => {
                let encoded_record = record.encode();
                let mut vec = Vec::with_capacity(1 + encoded_record.len());
                vec.push(0x01); // NodeMap tag
                vec.extend_from_slice(encoded_record.as_ref());
                Cow::Owned(vec)
            }
        }
    }
}

impl<S: PendingKeyValueSchema> Decode for SnapshotMapValue<S> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.is_empty() {
            return Err(DecodeError::IncorrectLength);
        }

        let tag = input[0];
        let data = &input[1..];

        match tag {
            0x00 => {
                // NodeMeta: encoded(parent_commit_id Option) | encoded(u64)
                if data.len() < u64::LENGTH {
                    return Err(DecodeError::IncorrectLength);
                }

                let (parent_part, count_part) = data.split_at(data.len() - u64::LENGTH);

                let parent_commit_id = decode_option::<S::CommitId>(parent_part)?;
                let modifications_count = u64::decode(count_part)?;

                Ok(Cow::Owned(SnapshotMapValue::NodeMeta {
                    parent_commit_id: parent_commit_id.map(|c| c.into_owned()),
                    modifications_count: modifications_count.into_owned(),
                }))
            }
            0x01 => {
                // NodeMap: RecoverRecord<S>
                let record = RecoverRecord::<S>::decode(data)?;
                Ok(Cow::Owned(SnapshotMapValue::NodeMap(record.into_owned())))
            }
            _ => Err(DecodeError::Custom(
                "Invalid SnapshotMapValue variant prefix",
            )),
        }
    }

    fn decode_owned(mut input: Vec<u8>) -> DecResult<Self> {
        if input.is_empty() {
            return Err(DecodeError::IncorrectLength);
        }

        let tag = input.remove(0);

        match tag {
            0x00 => {
                if input.len() < u64::LENGTH {
                    return Err(DecodeError::IncorrectLength);
                }

                let count_part = input.split_off(input.len() - u64::LENGTH);
                let parent_part = input;

                let parent_commit_id = decode_option_owned::<S::CommitId>(parent_part)?;
                let modifications_count = u64::decode_owned(count_part)?;

                Ok(SnapshotMapValue::NodeMeta {
                    parent_commit_id,
                    modifications_count,
                })
            }
            0x01 => {
                let record = RecoverRecord::<S>::decode_owned(input)?;
                Ok(SnapshotMapValue::NodeMap(record))
            }
            _ => Err(DecodeError::Custom(
                "Invalid SnapshotMapValue variant prefix",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use ethereum_types::H256;

    use super::super::super::super::{test_util::*, ValueEntry};
    use super::*;

    #[test]
    fn node_meta_no_parent_roundtrip_borrowed_and_owned() {
        let original = SnapshotMapValue::<TestSchema>::NodeMeta {
            parent_commit_id: None,
            modifications_count: 42,
        };

        // Encode
        let original_clone = original.clone();
        let enc = original_clone.encode();
        assert_eq!(enc.as_ref()[0], 0x00, "tag for NodeMeta should be 0x00");

        // Roundtrip
        test_encode_decode_round_trip(vec![original]);
    }

    #[test]
    fn node_meta_with_parent_roundtrip_borrowed_and_owned() {
        let original = SnapshotMapValue::<TestSchema>::NodeMeta {
            parent_commit_id: Some(H256::from([0xAA; 32])),
            modifications_count: u64::MAX - 1,
        };

        let original_clone = original.clone();
        let enc = original_clone.encode();
        assert_eq!(enc.as_ref()[0], 0x00);

        // Roundtrip
        test_encode_decode_round_trip(vec![original]);
    }

    #[test]
    fn node_meta_encoding_structure() {
        // Verify NodeMeta encoding layout: [0x00][opt_parent][count(8)]
        let parent = H256::from([0x11; 32]);
        let count = 123456789u64;

        let w = SnapshotMapValue::<TestSchema>::NodeMeta {
            parent_commit_id: Some(parent),
            modifications_count: count,
        };

        let enc = w.encode().into_owned();
        assert_eq!(enc[0], 0x00);

        // Option discriminant for Some
        assert_eq!(enc[1], 0x01);

        // parent bytes
        let parent_enc = <H256 as Encode>::encode(&parent);
        assert_eq!(&enc[2..34], parent_enc.as_ref());

        // Count bytes (big-endian u64)
        assert_eq!(&enc[34..42], &u64::to_be_bytes(count));

        // Total length: 1(tag) + 1(opt tag) + 32(parent) + 8(count)
        assert_eq!(enc.len(), 1 + 1 + 32 + 8);
    }

    #[test]
    fn node_meta_encoding_structure_none_parent() {
        // Verify layout for None: [0x00][0x00][count(8)]
        let count = 7u64;

        let w = SnapshotMapValue::<TestSchema>::NodeMeta {
            parent_commit_id: None,
            modifications_count: count,
        };

        let enc = w.encode().into_owned();
        assert_eq!(enc[0], 0x00);
        assert_eq!(enc[1], 0x00, "Option::None discriminant should be 0x00");
        assert_eq!(&enc[2..10], &u64::to_be_bytes(count));
        assert_eq!(enc.len(), 1 + 1 + 8);
    }

    #[test]
    fn node_map_with_value_and_last_cid_roundtrip_borrowed_and_owned() {
        // Don't test RecoverRecord roundtrip directly again, just wrap it in SnapshotMapValue
        let record = RecoverRecord::<TestSchema> {
            value: ValueEntry::<Box<[u8]>>::Value(k(&[1, 2, 3, 4])),
            last_commit_id: Some(H256::from([0x44; 32])),
        };
        let original = SnapshotMapValue::<TestSchema>::NodeMap(record);

        let enc = original.encode();
        assert_eq!(enc.as_ref()[0], 0x01);

        // Roundtrip
        test_encode_decode_round_trip(vec![original]);
    }

    #[test]
    fn node_map_deleted_without_last_cid_roundtrip_borrowed_and_owned() {
        let record = RecoverRecord::<TestSchema> {
            value: ValueEntry::<Box<[u8]>>::Deleted,
            last_commit_id: None,
        };
        let original = SnapshotMapValue::<TestSchema>::NodeMap(record);

        let enc = original.encode();
        assert_eq!(enc.as_ref()[0], 0x01);

        // Roundtrip
        test_encode_decode_round_trip(vec![original]);
    }

    #[test]
    fn node_map_encoding_structure_is_record_encoding() {
        // Confirm that NodeMap payload is equal to the encoding of RecoverRecord
        let record = RecoverRecord::<TestSchema> {
            value: ValueEntry::<Box<[u8]>>::Value(k(&[9, 8, 7])),
            last_commit_id: None,
        };
        let w = SnapshotMapValue::<TestSchema>::NodeMap(record.clone());

        let enc = w.encode().into_owned();
        assert_eq!(enc[0], 0x01);

        let rr_enc = record.encode().into_owned();
        assert_eq!(&enc[1..], rr_enc.as_slice());
    }

    #[test]
    fn snapshot_map_value_decode_errors() {
        // Empty input
        assert!(matches!(
            SnapshotMapValue::<TestSchema>::decode(&[]),
            Err(DecodeError::IncorrectLength)
        ));
        assert!(matches!(
            SnapshotMapValue::<TestSchema>::decode_owned(Vec::new()),
            Err(DecodeError::IncorrectLength)
        ));

        // Unknown tag
        assert!(matches!(
            SnapshotMapValue::<TestSchema>::decode(&[0xFF]),
            Err(DecodeError::Custom(
                "Invalid SnapshotMapValue variant prefix"
            ))
        ));
        assert!(matches!(
            SnapshotMapValue::<TestSchema>::decode_owned(vec![0xFF]),
            Err(DecodeError::Custom(
                "Invalid SnapshotMapValue variant prefix"
            ))
        ));

        // NodeMeta: length insufficient to contain the fixed u64 part
        assert!(matches!(
            SnapshotMapValue::<TestSchema>::decode(&[0x00]),
            Err(DecodeError::IncorrectLength)
        ));
        assert!(matches!(
            SnapshotMapValue::<TestSchema>::decode_owned(vec![0x00]),
            Err(DecodeError::IncorrectLength)
        ));

        // NodeMap: RecoverRecord lacks the 4-byte length prefix
        assert!(matches!(
            SnapshotMapValue::<TestSchema>::decode(&[0x01, 0x00]),
            Err(DecodeError::IncorrectLength)
        ));
        assert!(matches!(
            SnapshotMapValue::<TestSchema>::decode_owned(vec![0x01, 0x00]),
            Err(DecodeError::IncorrectLength)
        ));
    }

    #[test]
    fn mixed_round_trips_without_eq() {
        let cases = vec![
            SnapshotMapValue::<TestSchema>::NodeMeta {
                parent_commit_id: None,
                modifications_count: 0,
            },
            SnapshotMapValue::<TestSchema>::NodeMeta {
                parent_commit_id: Some(H256::from([0xCD; 32])),
                modifications_count: 1,
            },
            SnapshotMapValue::<TestSchema>::NodeMap(RecoverRecord {
                value: ValueEntry::<Box<[u8]>>::Deleted,
                last_commit_id: None,
            }),
            SnapshotMapValue::<TestSchema>::NodeMap(RecoverRecord {
                value: ValueEntry::<Box<[u8]>>::Value(k(&[])),
                last_commit_id: Some(H256::from([0xEF; 32])),
            }),
            SnapshotMapValue::<TestSchema>::NodeMap(RecoverRecord {
                value: ValueEntry::<Box<[u8]>>::Value(k(&[9, 8, 7, 6, 5])),
                last_commit_id: None,
            }),
        ];

        test_encode_decode_round_trip(cases);
    }
}
