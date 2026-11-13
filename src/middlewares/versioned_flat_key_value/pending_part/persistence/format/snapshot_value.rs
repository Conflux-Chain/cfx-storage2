use std::borrow::Cow;

use super::{
    super::{DecResult, Decode, DecodeError, Encode, PendingKeyValueSchema},
    codec::{decode_option, decode_option_owned, encode_option},
    identifiers::SnapshotId,
};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SnapshotValue<S: PendingKeyValueSchema> {
    pub parent_of_root: Option<S::CommitId>,
    pub snapshot_id: SnapshotId,
}

impl<S: PendingKeyValueSchema> Encode for SnapshotValue<S> {
    fn encode(&self) -> Cow<[u8]> {
        let encoded_parent_of_root = encode_option(&self.parent_of_root);
        let encoded_snapshot_id = self.snapshot_id.encode();

        Cow::Owned(
            [
                encoded_parent_of_root.as_ref(),
                encoded_snapshot_id.as_ref(),
            ]
            .concat(),
        )
    }
}

impl<S: PendingKeyValueSchema> Decode for SnapshotValue<S> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        const BYTES: usize = std::mem::size_of::<u64>();
        if input.len() < BYTES {
            return Err(DecodeError::IncorrectLength);
        }

        let (parent_of_root_raw, snapshot_id_raw) = input.split_at(input.len() - BYTES);
        let (parent_of_root, snapshot_id) = (
            decode_option::<S::CommitId>(parent_of_root_raw)?,
            SnapshotId::decode(snapshot_id_raw)?,
        );
        Ok(Cow::Owned(SnapshotValue {
            parent_of_root: parent_of_root.map(|cow| cow.into_owned()),
            snapshot_id: snapshot_id.into_owned(),
        }))
    }

    fn decode_owned(mut input: Vec<u8>) -> DecResult<Self> {
        const BYTES: usize = std::mem::size_of::<u64>();
        if input.len() < BYTES {
            return Err(DecodeError::IncorrectLength);
        }

        let snapshot_id_raw = input.split_off(input.len() - BYTES);
        let parent_of_root_raw = input;
        let parent_of_root = decode_option_owned::<S::CommitId>(parent_of_root_raw)?;
        let snapshot_id = SnapshotId::decode_owned(snapshot_id_raw)?;
        Ok(SnapshotValue {
            parent_of_root,
            snapshot_id,
        })
    }
}
