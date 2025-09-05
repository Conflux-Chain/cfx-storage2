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

#[cfg(test)]
mod tests {
    use ethereum_types::H256;

    use super::super::super::test_util::*;
    use super::*;

    #[test]
    fn encode_then_decode_roundtrip_with_parent() {
        let value = SnapshotValue::<TestSchema> {
            parent_of_root: Some(H256::zero()),
            snapshot_id: SnapshotId(42u64),
        };

        // Encode
        let enc = value.encode();

        // Decode (borrowed)
        let decoded = <SnapshotValue<TestSchema> as Decode>::decode(enc.as_ref())
            .expect("decode should succeed")
            .into_owned();

        assert_eq!(decoded, value);

        // Decode owned
        let decoded_owned = <SnapshotValue<TestSchema> as Decode>::decode_owned(enc.to_vec())
            .expect("owned decode should succeed");
        assert_eq!(decoded_owned, value);
    }

    #[test]
    fn encode_then_decode_roundtrip_without_parent() {
        let value = SnapshotValue::<TestSchema> {
            parent_of_root: None,
            snapshot_id: SnapshotId(u64::MAX),
        };

        let enc = value.encode();

        let decoded = <SnapshotValue<TestSchema> as Decode>::decode(enc.as_ref())
            .expect("decode should succeed")
            .into_owned();
        assert_eq!(decoded, value);

        let decoded_owned = <SnapshotValue<TestSchema> as Decode>::decode_owned(enc.to_vec())
            .expect("owned decode should succeed");
        assert_eq!(decoded_owned, value);
    }

    #[test]
    fn decode_fails_when_too_short() {
        // Less than 8 bytes (u64 size) must fail regardless of option encoding.
        for len in 0..8 {
            let bytes = vec![0u8; len];
            let err = <SnapshotValue<TestSchema> as Decode>::decode(&bytes).unwrap_err();
            assert!(matches!(err, DecodeError::IncorrectLength));

            let err_owned =
                <SnapshotValue<TestSchema> as Decode>::decode_owned(bytes.clone()).unwrap_err();
            assert!(matches!(err_owned, DecodeError::IncorrectLength));
        }
    }

    #[test]
    fn binary_layout_is_parent_then_snapshot_id() {
        // Construct a value with a known parent and snapshot id.
        let snapshot_id = SnapshotId(0x0102030405060708u64);
        let parent = Some(H256::from_low_u64_be(0xAABBCCDD));

        let value = SnapshotValue::<TestSchema> {
            parent_of_root: parent,
            snapshot_id,
        };

        // Encode
        let enc = value.encode().into_owned();

        // Split as the decoder does: last 8 bytes are snapshot id.
        assert!(enc.len() >= 8);
        let (parent_part, sid_part) = enc.split_at(enc.len() - 8);

        // SnapshotId roundtrip check from tail
        let sid_decoded = SnapshotId::decode(sid_part)
            .expect("snapshot id decode ok")
            .into_owned();
        assert_eq!(sid_decoded, snapshot_id);

        // Parent part must decode as the option<CommitId>
        let parent_decoded = decode_option::<H256>(parent_part)
            .expect("parent option decode ok")
            .map(|c| c.into_owned());
        assert_eq!(parent_decoded, parent);
    }

    #[test]
    fn owned_decoding_uses_in_place_split() {
        // Ensure decode_owned path correctly splits and decodes without copying mistakes.
        let snapshot_id = SnapshotId(123456789u64);
        let parent = None;

        let value = SnapshotValue::<TestSchema> {
            parent_of_root: parent,
            snapshot_id,
        };

        let enc = value.encode().into_owned();

        let decoded = <SnapshotValue<TestSchema> as Decode>::decode_owned(enc)
            .expect("owned decode should succeed");
        assert_eq!(decoded.parent_of_root, parent);
        assert_eq!(decoded.snapshot_id, snapshot_id);
    }

    #[test]
    fn different_inputs_produce_different_encodings() {
        let v1 = SnapshotValue::<TestSchema> {
            parent_of_root: None,
            snapshot_id: SnapshotId(1),
        };
        let v2 = SnapshotValue::<TestSchema> {
            parent_of_root: Some(H256::repeat_byte(0x11)),
            snapshot_id: SnapshotId(1),
        };
        let v3 = SnapshotValue::<TestSchema> {
            parent_of_root: Some(H256::repeat_byte(0x11)),
            snapshot_id: SnapshotId(2),
        };

        let e1 = v1.encode().into_owned();
        let e2 = v2.encode().into_owned();
        let e3 = v3.encode().into_owned();

        assert_ne!(e1, e2);
        assert_ne!(e2, e3);
        assert_ne!(e1, e3);
    }
}
