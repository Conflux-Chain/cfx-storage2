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
