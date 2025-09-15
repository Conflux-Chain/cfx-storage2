use std::borrow::Cow;

mod map;

pub use map::SnapshotMapValue;

use super::{
    super::{DecResult, Decode, DecodeError, Encode, FixedLengthEncoded, PendingKeyValueSchema},
    codec::{decode_option, decode_option_owned, encode_option},
    identifiers::SnapshotId,
};

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SnapshotValue<S: PendingKeyValueSchema> {
    MetaValue {
        parent_of_root: Option<S::CommitId>,
        snapshot_id: SnapshotId,
        nodes_count: u64,
    },
    MapValue(SnapshotMapValue<S>),
}

impl<S: PendingKeyValueSchema> Encode for SnapshotValue<S> {
    fn encode(&self) -> Cow<[u8]> {
        match self {
            // Layout (tagged):
            // 0x00 | encode_option(parent_of_root) | encode(snapshot_id) | encode(nodes_count)
            SnapshotValue::MetaValue {
                parent_of_root,
                snapshot_id,
                nodes_count,
            } => {
                let encoded_parent = encode_option(parent_of_root);
                let encoded_snapshot_id = snapshot_id.encode();
                let encoded_nodes = nodes_count.encode();

                let mut vec = Vec::with_capacity(
                    1 // tag
                    + encoded_parent.len()
                    + SnapshotId::LENGTH
                    + u64::LENGTH,
                );
                vec.push(0x00); // MetaValue tag
                vec.extend_from_slice(encoded_parent.as_ref());
                vec.extend_from_slice(encoded_snapshot_id.as_ref());
                vec.extend_from_slice(encoded_nodes.as_ref());
                Cow::Owned(vec)
            }
            // Layout:
            // 0x01 | SnapshotMapValue<S>.encode()
            SnapshotValue::MapValue(map_val) => {
                let encoded_map = map_val.encode();
                let mut vec = Vec::with_capacity(1 + encoded_map.len());
                vec.push(0x01); // MapValue tag
                vec.extend_from_slice(encoded_map.as_ref());
                Cow::Owned(vec)
            }
        }
    }
}

impl<S: PendingKeyValueSchema> Decode for SnapshotValue<S> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.is_empty() {
            return Err(DecodeError::IncorrectLength);
        }

        let tag = input[0];
        let data = &input[1..];

        match tag {
            0x00 => {
                // MetaValue: encode_option(parent_of_root) | snapshot_id | nodes_count
                let fixed_tail_len = SnapshotId::LENGTH + u64::LENGTH;
                if data.len() < fixed_tail_len {
                    return Err(DecodeError::IncorrectLength);
                }

                // Split: [parent_opt ...][snapshot_id][nodes_count]
                let (prefix, tail) = data.split_at(data.len() - fixed_tail_len);
                let (snapshot_id_part, nodes_part) = tail.split_at(SnapshotId::LENGTH);

                let parent_of_root = decode_option::<S::CommitId>(prefix)?;
                let snapshot_id = SnapshotId::decode(snapshot_id_part)?;
                let nodes_count = u64::decode(nodes_part)?;

                Ok(Cow::Owned(SnapshotValue::MetaValue {
                    parent_of_root: parent_of_root.map(|c| c.into_owned()),
                    snapshot_id: snapshot_id.into_owned(),
                    nodes_count: nodes_count.into_owned(),
                }))
            }
            0x01 => {
                // MapValue
                let map_val = SnapshotMapValue::<S>::decode(data)?;
                Ok(Cow::Owned(SnapshotValue::MapValue(map_val.into_owned())))
            }
            _ => Err(DecodeError::Custom("Invalid SnapshotValue variant prefix")),
        }
    }

    fn decode_owned(mut input: Vec<u8>) -> DecResult<Self> {
        if input.is_empty() {
            return Err(DecodeError::IncorrectLength);
        }

        let tag = input.remove(0);

        match tag {
            0x00 => {
                let fixed_tail_len = SnapshotId::LENGTH + u64::LENGTH;
                if input.len() < fixed_tail_len {
                    return Err(DecodeError::IncorrectLength);
                }

                // Split owned:
                // input = [ parent_opt ... | snapshot_id | nodes_count ]
                let mut tail = input.split_off(input.len() - fixed_tail_len);
                let parent_part = input;

                let nodes_part = tail.split_off(SnapshotId::LENGTH);
                let snapshot_part = tail;

                let parent_of_root = decode_option_owned::<S::CommitId>(parent_part)?;
                let snapshot_id = SnapshotId::decode_owned(snapshot_part)?;
                let nodes_count = u64::decode_owned(nodes_part)?;

                Ok(SnapshotValue::MetaValue {
                    parent_of_root,
                    snapshot_id,
                    nodes_count,
                })
            }
            0x01 => {
                let map_val = SnapshotMapValue::<S>::decode_owned(input)?;
                Ok(SnapshotValue::MapValue(map_val))
            }
            _ => Err(DecodeError::Custom("Invalid SnapshotValue variant prefix")),
        }
    }
}

#[cfg(test)]
mod tests {
    use ethereum_types::H256;

    use super::super::super::{test_util::*, RecoverRecord, ValueEntry};
    use super::map::SnapshotMapValue;
    use super::*;

    #[test]
    fn meta_value_no_parent_roundtrip_borrowed_and_owned() {
        let snapshot_id = SnapshotId(0x5A);
        let original = SnapshotValue::<TestSchema>::MetaValue {
            parent_of_root: None,
            snapshot_id,
            nodes_count: 42,
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
        let snapshot_id = SnapshotId(0xA5);
        let original = SnapshotValue::<TestSchema>::MetaValue {
            parent_of_root: Some(H256::from([0xBB; 32])),
            snapshot_id,
            nodes_count: u64::MAX - 7,
        };

        let original_clone = original.clone();
        let enc = original_clone.encode();
        assert_eq!(enc.as_ref()[0], 0x00);

        // Roundtrip
        test_encode_decode_round_trip(vec![original]);
    }

    #[test]
    fn meta_value_encoding_structure() {
        // Validate meta encoding layout: [0x00][opt_parent][snapshot_id][nodes_count(8)]
        let parent = H256::from([0x11; 32]);
        let snapshot_id = SnapshotId(0x22);
        let nodes = 123456789u64;

        let w = SnapshotValue::<TestSchema>::MetaValue {
            parent_of_root: Some(parent),
            snapshot_id,
            nodes_count: nodes,
        };

        let enc = w.encode().into_owned();
        assert_eq!(enc[0], 0x00, "MetaValue tag mismatch");

        // Option discriminant for Some
        assert_eq!(enc[1], 0x01, "Option::Some discriminant must be 0x01");

        // Parent bytes
        let parent_enc = <H256 as Encode>::encode(&parent);
        assert_eq!(&enc[2..34], parent_enc.as_ref());

        // SnapshotId bytes
        let sid_start = 34;
        let sid_end = sid_start + SnapshotId::LENGTH;
        let sid_enc = <SnapshotId as Encode>::encode(&snapshot_id);
        assert_eq!(&enc[sid_start..sid_end], sid_enc.as_ref());

        // Nodes count (u64 big-endian)
        let nodes_start = sid_end;
        let nodes_end = nodes_start + 8;
        assert_eq!(&enc[nodes_start..nodes_end], &u64::to_be_bytes(nodes));

        // Total length: 1(tag) + 1(opt tag) + 32(parent) + SnapshotId::LENGTH + 8
        assert_eq!(enc.len(), 1 + 1 + 32 + SnapshotId::LENGTH + 8);
    }

    #[test]
    fn meta_value_encoding_structure_none_parent() {
        // Layout for None: [0x00][0x00][snapshot_id][nodes_count(8)]
        let snapshot_id = SnapshotId(0x77);
        let nodes = 7u64;

        let w = SnapshotValue::<TestSchema>::MetaValue {
            parent_of_root: None,
            snapshot_id,
            nodes_count: nodes,
        };

        let enc = w.encode().into_owned();
        assert_eq!(enc[0], 0x00);
        assert_eq!(enc[1], 0x00, "Option::None discriminant should be 0x00");

        let sid_start = 2;
        let sid_end = sid_start + SnapshotId::LENGTH;
        let sid_enc = <SnapshotId as Encode>::encode(&snapshot_id);
        assert_eq!(&enc[sid_start..sid_end], sid_enc.as_ref());

        assert_eq!(&enc[sid_end..sid_end + 8], &u64::to_be_bytes(nodes));
        assert_eq!(enc.len(), 1 + 1 + SnapshotId::LENGTH + 8);
    }

    #[test]
    fn map_value_with_value_and_last_cid_roundtrip_borrowed_and_owned() {
        // Do not duplicate direct RecoverRecord roundtrip tests; wrap via SnapshotMapValue
        let record = RecoverRecord::<TestSchema> {
            value: ValueEntry::<Box<[u8]>>::Value(k(&[0xDE, 0xAD, 0xBE, 0xEF])),
            last_commit_id: Some(H256::from([0x44; 32])),
        };
        let map = SnapshotMapValue::<TestSchema>::NodeMap(record);
        let original = SnapshotValue::<TestSchema>::MapValue(map);

        let enc = original.encode();
        assert_eq!(enc.as_ref()[0], 0x01, "tag for MapValue should be 0x01");

        // Roundtrip
        test_encode_decode_round_trip(vec![original]);
    }

    #[test]
    fn map_value_deleted_without_last_cid_roundtrip_borrowed_and_owned() {
        let record = RecoverRecord::<TestSchema> {
            value: ValueEntry::<Box<[u8]>>::Deleted,
            last_commit_id: None,
        };
        let map = SnapshotMapValue::<TestSchema>::NodeMap(record);
        let original = SnapshotValue::<TestSchema>::MapValue(map);

        let enc = original.encode();
        assert_eq!(enc.as_ref()[0], 0x01);

        // Roundtrip
        test_encode_decode_round_trip(vec![original]);
    }

    #[test]
    fn map_value_encoding_structure_is_snapshot_map_encoding() {
        // Ensure MapValue payload equals SnapshotMapValue encoding
        let record = RecoverRecord::<TestSchema> {
            value: ValueEntry::<Box<[u8]>>::Value(k(&[1, 2, 3])),
            last_commit_id: None,
        };
        let map = SnapshotMapValue::<TestSchema>::NodeMap(record.clone());
        let w = SnapshotValue::<TestSchema>::MapValue(map.clone());

        let enc = w.encode().into_owned();
        assert_eq!(enc[0], 0x01);

        let inner_enc = map.encode().into_owned();
        assert_eq!(&enc[1..], inner_enc.as_slice());
    }

    #[test]
    fn snapshot_value_decode_errors() {
        // Empty input
        assert!(matches!(
            SnapshotValue::<TestSchema>::decode(&[]),
            Err(DecodeError::IncorrectLength)
        ));
        assert!(matches!(
            SnapshotValue::<TestSchema>::decode_owned(Vec::new()),
            Err(DecodeError::IncorrectLength)
        ));

        // Unknown tag
        assert!(matches!(
            SnapshotValue::<TestSchema>::decode(&[0xFF]),
            Err(DecodeError::Custom("Invalid SnapshotValue variant prefix"))
        ));
        assert!(matches!(
            SnapshotValue::<TestSchema>::decode_owned(vec![0xFF]),
            Err(DecodeError::Custom("Invalid SnapshotValue variant prefix"))
        ));

        // MetaValue too short for fixed tail (needs at least SnapshotId::LENGTH + 8 after option)
        assert!(matches!(
            SnapshotValue::<TestSchema>::decode(&[0x00]),
            Err(DecodeError::IncorrectLength)
        ));
        assert!(matches!(
            SnapshotValue::<TestSchema>::decode_owned(vec![0x00]),
            Err(DecodeError::IncorrectLength)
        ));

        // MapValue with incomplete inner payload (insufficient for RecoverRecord 4-byte len prefix)
        assert!(matches!(
            SnapshotValue::<TestSchema>::decode(&[0x01, 0x01, 0x00]), // 0x01 tag + inner NodeMap with too-short payload
            Err(DecodeError::IncorrectLength)
        ));
        assert!(matches!(
            SnapshotValue::<TestSchema>::decode_owned(vec![0x01, 0x01, 0x00]),
            Err(DecodeError::IncorrectLength)
        ));
    }

    #[test]
    fn mixed_round_trips_without_eq() {
        let cases = vec![
            SnapshotValue::<TestSchema>::MetaValue {
                parent_of_root: None,
                snapshot_id: SnapshotId(0x00),
                nodes_count: 0,
            },
            SnapshotValue::<TestSchema>::MetaValue {
                parent_of_root: Some(H256::from([0xCD; 32])),
                snapshot_id: SnapshotId(0xAB),
                nodes_count: 1,
            },
            SnapshotValue::<TestSchema>::MapValue(SnapshotMapValue::NodeMap(RecoverRecord {
                value: ValueEntry::<Box<[u8]>>::Deleted,
                last_commit_id: None,
            })),
            SnapshotValue::<TestSchema>::MapValue(SnapshotMapValue::NodeMap(RecoverRecord {
                value: ValueEntry::<Box<[u8]>>::Value(k(&[])),
                last_commit_id: Some(H256::from([0xEF; 32])),
            })),
            SnapshotValue::<TestSchema>::MapValue(SnapshotMapValue::NodeMap(RecoverRecord {
                value: ValueEntry::<Box<[u8]>>::Value(k(&[9, 8, 7, 6, 5])),
                last_commit_id: None,
            })),
        ];

        test_encode_decode_round_trip(cases);
    }
}
