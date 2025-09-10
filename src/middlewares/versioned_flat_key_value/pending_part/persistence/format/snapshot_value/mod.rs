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
