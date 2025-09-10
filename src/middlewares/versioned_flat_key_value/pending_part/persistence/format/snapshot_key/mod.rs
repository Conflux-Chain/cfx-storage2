mod tree;

use core::cmp::Ordering;
use std::borrow::Cow;

use super::super::{
    DecResult, Decode, DecodeError, Encode, EncodeSubKey, FixedLengthEncoded,
    PendingKeyValueSchema, SeekKey,
};
pub use tree::{SnapshotKeyTreePart, SnapshotNodeDataType};

#[derive(Clone, Debug)]
pub struct SnapshotKey<S: PendingKeyValueSchema> {
    pub snapshot_root_height: u64,
    pub record_type: SnapshotRecordType<S>,
}

/// Distinguishes between the single metadata record of a snapshot and the collection
/// of records that represent the `Tree` state.
#[derive(Clone, Debug)]
pub enum SnapshotRecordType<S: PendingKeyValueSchema> {
    /// Identifies the single metadata record for the snapshot.
    /// This corresponds to `SnapshotValue::MetaValue`.
    Meta,

    /// Identifies a record belonging to the serialized `Tree` state (the map).
    /// This corresponds to `SnapshotValue::MapValue`. The associated data
    /// contains the rest of the key needed for correct sorting during recovery.
    Map(SnapshotKeyTreePart<S>),
}

impl<S: PendingKeyValueSchema> SnapshotKey<S> {
    /// Generates the `SeekKey` to start an iteration from the entry at a
    /// specific `height`.
    ///
    /// An iterator starting from this key will first yield the entry for `height`
    /// (if it exists) and then proceed to subsequent heights (`height + 1`, etc.).
    ///
    /// **Note**: This only provides a starting point. If the caller is only
    /// interested in the single entry at the specified `height`, they are
    /// responsible for stopping the iteration after the first item.
    pub fn seek_key_for_height(height: u64) -> SeekKey<Self> {
        let start_key = SnapshotKey {
            snapshot_root_height: height,
            record_type: SnapshotRecordType::Meta,
        };

        SeekKey { key: start_key }
    }
}

impl<S: PendingKeyValueSchema> Encode for SnapshotRecordType<S> {
    fn encode(&self) -> Cow<[u8]> {
        match self {
            SnapshotRecordType::Meta => {
                let v = vec![0x00];
                Cow::Owned(v)
            }
            SnapshotRecordType::Map(key_part) => {
                let encoded = key_part.encode();
                let mut v = Vec::with_capacity(1 + encoded.len());
                v.push(0x01);
                v.extend_from_slice(encoded.as_ref());
                Cow::Owned(v)
            }
        }
    }
}

impl<S: PendingKeyValueSchema> Decode for SnapshotRecordType<S> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.is_empty() {
            return Err(DecodeError::IncorrectLength);
        }
        let tag = input[0];
        let data = &input[1..];
        match tag {
            0x00 => Ok(Cow::Owned(SnapshotRecordType::Meta)),
            0x01 => {
                let key_part = SnapshotKeyTreePart::<S>::decode(data)?;
                Ok(Cow::Owned(SnapshotRecordType::Map(key_part.into_owned())))
            }
            _ => Err(DecodeError::Custom(
                "Invalid SnapshotRecordType variant prefix",
            )),
        }
    }

    fn decode_owned(mut input: Vec<u8>) -> DecResult<Self> {
        if input.is_empty() {
            return Err(DecodeError::IncorrectLength);
        }
        let tag = input.remove(0);
        match tag {
            0x00 => Ok(SnapshotRecordType::Meta),
            0x01 => {
                let key_part = SnapshotKeyTreePart::<S>::decode_owned(input)?;
                Ok(SnapshotRecordType::Map(key_part))
            }
            _ => Err(DecodeError::Custom(
                "Invalid SnapshotRecordType variant prefix",
            )),
        }
    }
}

impl<S: PendingKeyValueSchema> Encode for SnapshotKey<S> {
    fn encode(&self) -> Cow<[u8]> {
        // Layout: encode(snapshot_root_height) | encode(record_type)
        let enc_height = self.snapshot_root_height.encode();
        let enc_rec_type = self.record_type.encode();

        let mut vec = Vec::with_capacity(u64::LENGTH + enc_rec_type.len());
        vec.extend_from_slice(enc_height.as_ref());
        vec.extend_from_slice(enc_rec_type.as_ref());
        Cow::Owned(vec)
    }
}

impl<S: PendingKeyValueSchema> EncodeSubKey for SnapshotKey<S> {
    const HAVE_SUBKEY: bool = true;

    fn encode_subkey(&self) -> (Cow<[u8]>, Cow<[u8]>) {
        let prefix = self.snapshot_root_height.encode();
        let suffix = self.record_type.encode();

        (prefix, suffix)
    }

    fn encode_subkey_owned(input: <Self as ToOwned>::Owned) -> (Vec<u8>, Vec<u8>) {
        let prefix_vec = <u64 as Encode>::encode_owned(input.snapshot_root_height);
        let suffix_vec = <SnapshotRecordType<S> as Encode>::encode_owned(input.record_type);

        (prefix_vec, suffix_vec)
    }
}

impl<S: PendingKeyValueSchema> Decode for SnapshotKey<S> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.len() < u64::LENGTH {
            return Err(DecodeError::IncorrectLength);
        }
        let (height_bytes, rec_type_bytes) = input.split_at(u64::LENGTH);

        let snapshot_root_height = u64::decode(height_bytes)?;
        let record_type = SnapshotRecordType::<S>::decode(rec_type_bytes)?;

        Ok(Cow::Owned(SnapshotKey {
            snapshot_root_height: snapshot_root_height.into_owned(),
            record_type: record_type.into_owned(),
        }))
    }

    fn decode_owned(mut input: Vec<u8>) -> DecResult<Self> {
        if input.len() < u64::LENGTH {
            return Err(DecodeError::IncorrectLength);
        }
        let rec_type_bytes = input.split_off(u64::LENGTH);
        let height_bytes = input; // exactly u64::LENGTH

        let snapshot_root_height = u64::decode_owned(height_bytes)?;
        let record_type = SnapshotRecordType::<S>::decode_owned(rec_type_bytes)?;

        Ok(SnapshotKey {
            snapshot_root_height,
            record_type,
        })
    }
}

// --------------------- SnapshotRecordType ---------------------

impl<S: PendingKeyValueSchema> PartialEq for SnapshotRecordType<S> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SnapshotRecordType::Meta, SnapshotRecordType::Meta) => true,
            (SnapshotRecordType::Map(a), SnapshotRecordType::Map(b)) => a == b,
            _ => false,
        }
    }
}

impl<S: PendingKeyValueSchema> Eq for SnapshotRecordType<S> {}

impl<S: PendingKeyValueSchema> PartialOrd for SnapshotRecordType<S> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<S: PendingKeyValueSchema> Ord for SnapshotRecordType<S> {
    fn cmp(&self, other: &Self) -> Ordering {
        use SnapshotRecordType::*;
        match (self, other) {
            (Meta, Meta) => Ordering::Equal,
            (Meta, Map(_)) => Ordering::Less, // Meta < Map
            (Map(_), Meta) => Ordering::Greater,
            (Map(a), Map(b)) => a.cmp(b),
        }
    }
}

// --------------------- SnapshotKey ---------------------

impl<S: PendingKeyValueSchema> PartialEq for SnapshotKey<S> {
    fn eq(&self, other: &Self) -> bool {
        self.snapshot_root_height == other.snapshot_root_height
            && self.record_type == other.record_type
    }
}

impl<S: PendingKeyValueSchema> Eq for SnapshotKey<S> {}

impl<S: PendingKeyValueSchema> PartialOrd for SnapshotKey<S> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<S: PendingKeyValueSchema> Ord for SnapshotKey<S> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.snapshot_root_height
            .cmp(&other.snapshot_root_height)
            .then_with(|| self.record_type.cmp(&other.record_type))
    }
}
