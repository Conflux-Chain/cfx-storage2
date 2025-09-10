use core::cmp::Ordering;
use std::borrow::Cow;

use super::{DecResult, Decode, DecodeError, Encode, FixedLengthEncoded, PendingKeyValueSchema};

#[derive(Clone, Debug)]
pub struct SnapshotKeyTreePart<S: PendingKeyValueSchema> {
    /// The height of the specific node this key pertains to.
    /// This is the primary sort key within the map records, ensuring that
    /// parent nodes are always processed before their children.
    pub node_height: u64,

    /// The unique commit ID of the node. This serves as a secondary sort key.
    pub node_commit_id: S::CommitId,

    pub node_data_type: SnapshotNodeDataType<S>,
}

/// Distinguishes between the core metadata of a `TreeNode` and its modification entries.
/// The `Encode` implementation for this enum will prepend a tag (e.g., `0x00` for `NodeMeta`,
/// `0x01` for `NodeMap`) to ensure `NodeMeta` records are always sorted before `NodeMap`
/// records for the same node.
#[derive(Clone, Debug)]
pub enum SnapshotNodeDataType<S: PendingKeyValueSchema> {
    /// Represents the key for a node's core metadata (parent_commit_id, modifications_count).
    /// This corresponds to `SnapshotMapValue::NodeMeta`.
    NodeMeta,

    /// Represents the key for a single entry in a node's `modifications` map.
    /// This corresponds to `SnapshotMapValue::NodeMap`.
    NodeMap {
        /// The key of the specific modification record from the node's `RecoverMap`.
        key: S::Key,
    },
}

impl<S: PendingKeyValueSchema> Encode for SnapshotNodeDataType<S> {
    fn encode(&self) -> Cow<[u8]> {
        match self {
            SnapshotNodeDataType::NodeMeta => {
                let vec = vec![0x00];
                Cow::Owned(vec)
            }
            SnapshotNodeDataType::NodeMap { key } => {
                let encoded_key = key.encode();
                let mut vec = Vec::with_capacity(1 + encoded_key.len());
                vec.push(0x01);
                vec.extend_from_slice(encoded_key.as_ref());
                Cow::Owned(vec)
            }
        }
    }
}

impl<S: PendingKeyValueSchema> Decode for SnapshotNodeDataType<S> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.is_empty() {
            return Err(DecodeError::IncorrectLength);
        }

        match input[0] {
            0x00 => Ok(Cow::Owned(SnapshotNodeDataType::NodeMeta)),
            0x01 => {
                let data = &input[1..];
                // key may have variable length; delegate to S::Key::decode
                let key = S::Key::decode(data)?;
                Ok(Cow::Owned(SnapshotNodeDataType::NodeMap {
                    key: key.into_owned(),
                }))
            }
            _ => Err(DecodeError::Custom(
                "Invalid SnapshotNodeDataType variant prefix",
            )),
        }
    }

    fn decode_owned(mut input: Vec<u8>) -> DecResult<Self> {
        if input.is_empty() {
            return Err(DecodeError::IncorrectLength);
        }

        let tag = input.remove(0);
        match tag {
            0x00 => Ok(SnapshotNodeDataType::NodeMeta),
            0x01 => {
                let key = S::Key::decode_owned(input)?;
                Ok(SnapshotNodeDataType::NodeMap { key })
            }
            _ => Err(DecodeError::Custom(
                "Invalid SnapshotNodeDataType variant prefix",
            )),
        }
    }
}

impl<S: PendingKeyValueSchema> Encode for SnapshotKeyTreePart<S> {
    fn encode(&self) -> Cow<[u8]> {
        // Layout:
        // tagless concatenation to preserve ordering:
        // encode(node_height) | encode(node_commit_id) | encode(node_data_type)
        let enc_height = self.node_height.encode();
        let enc_cid = self.node_commit_id.encode();
        let enc_dtype = self.node_data_type.encode();

        let mut vec = Vec::with_capacity(u64::LENGTH + S::CommitId::LENGTH + enc_dtype.len());
        vec.extend_from_slice(enc_height.as_ref());
        vec.extend_from_slice(enc_cid.as_ref());
        vec.extend_from_slice(enc_dtype.as_ref());

        Cow::Owned(vec)
    }
}

impl<S: PendingKeyValueSchema> Decode for SnapshotKeyTreePart<S> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        // Expect fixed prefix: u64 (node_height) + CommitId (fixed length),
        // followed by variable-length node_data_type.
        let fixed_prefix = u64::LENGTH + S::CommitId::LENGTH;
        if input.len() < fixed_prefix {
            return Err(DecodeError::IncorrectLength);
        }

        let (prefix, dtype_bytes) = input.split_at(fixed_prefix);
        let (height_bytes, cid_bytes) = prefix.split_at(u64::LENGTH);

        let node_height = u64::decode(height_bytes)?;
        let node_commit_id = S::CommitId::decode(cid_bytes)?;
        let node_data_type = SnapshotNodeDataType::<S>::decode(dtype_bytes)?;

        Ok(Cow::Owned(SnapshotKeyTreePart {
            node_height: node_height.into_owned(),
            node_commit_id: node_commit_id.into_owned(),
            node_data_type: node_data_type.into_owned(),
        }))
    }

    fn decode_owned(mut input: Vec<u8>) -> DecResult<Self> {
        let fixed_prefix = u64::LENGTH + S::CommitId::LENGTH;
        if input.len() < fixed_prefix {
            return Err(DecodeError::IncorrectLength);
        }

        // Split off dtype bytes (variable tail)
        let dtype_bytes = input.split_off(fixed_prefix);
        // Remaining input is fixed prefix: [u64 | CommitId]
        let cid_bytes = input.split_off(u64::LENGTH);
        let height_bytes = input; // exactly u64::LENGTH

        let node_height = u64::decode_owned(height_bytes)?;
        let node_commit_id = S::CommitId::decode_owned(cid_bytes)?;
        let node_data_type = SnapshotNodeDataType::<S>::decode_owned(dtype_bytes)?;

        Ok(SnapshotKeyTreePart {
            node_height,
            node_commit_id,
            node_data_type,
        })
    }
}

// --------------------- SnapshotNodeDataType ---------------------

impl<S: PendingKeyValueSchema> PartialEq for SnapshotNodeDataType<S> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SnapshotNodeDataType::NodeMeta, SnapshotNodeDataType::NodeMeta) => true,
            (
                SnapshotNodeDataType::NodeMap { key: k1 },
                SnapshotNodeDataType::NodeMap { key: k2 },
            ) => k1 == k2,
            _ => false,
        }
    }
}

impl<S: PendingKeyValueSchema> Eq for SnapshotNodeDataType<S> {}

impl<S: PendingKeyValueSchema> PartialOrd for SnapshotNodeDataType<S> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<S: PendingKeyValueSchema> Ord for SnapshotNodeDataType<S> {
    fn cmp(&self, other: &Self) -> Ordering {
        use SnapshotNodeDataType::*;
        match (self, other) {
            (NodeMeta, NodeMeta) => Ordering::Equal,
            (NodeMeta, NodeMap { .. }) => Ordering::Less, // NodeMeta < NodeMap
            (NodeMap { .. }, NodeMeta) => Ordering::Greater,
            (NodeMap { key: k1 }, NodeMap { key: k2 }) => k1.cmp(k2),
        }
    }
}

// --------------------- SnapshotKeyTreePart ---------------------

impl<S: PendingKeyValueSchema> PartialEq for SnapshotKeyTreePart<S> {
    fn eq(&self, other: &Self) -> bool {
        self.node_height == other.node_height
            && self.node_commit_id == other.node_commit_id
            && self.node_data_type == other.node_data_type
    }
}

impl<S: PendingKeyValueSchema> Eq for SnapshotKeyTreePart<S> {}

impl<S: PendingKeyValueSchema> PartialOrd for SnapshotKeyTreePart<S> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<S: PendingKeyValueSchema> Ord for SnapshotKeyTreePart<S> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.node_height
            .cmp(&other.node_height)
            .then_with(|| self.node_commit_id.cmp(&other.node_commit_id))
            .then_with(|| self.node_data_type.cmp(&other.node_data_type))
    }
}
