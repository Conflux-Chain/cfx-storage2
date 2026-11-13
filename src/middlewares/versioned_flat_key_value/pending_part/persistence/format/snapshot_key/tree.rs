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

#[cfg(test)]
mod tests {
    use ethereum_types::H256;

    use super::super::super::super::test_util::*;
    use super::*;
    use std::cmp::Ordering;

    type D = SnapshotNodeDataType<TestSchema>;
    type K = SnapshotKeyTreePart<TestSchema>;

    // --------------------- SnapshotNodeDataType ---------------------

    #[test]
    fn dtype_eq_and_ord_basic() {
        // Equality among identical variants
        assert_eq!(D::NodeMeta, D::NodeMeta);
        assert_eq!(D::NodeMap { key: k(&[]) }, D::NodeMap { key: k(&[]) });
        assert_ne!(D::NodeMeta, D::NodeMap { key: k(&[]) });

        // Ordering: NodeMeta < NodeMap
        assert!(D::NodeMeta < D::NodeMap { key: k(&[]) });

        // Ordering within NodeMap by key
        assert_eq!(
            D::NodeMap { key: k(&[1, 2, 3]) }.cmp(&D::NodeMap { key: k(&[1, 2, 3]) }),
            Ordering::Equal
        );
        assert_eq!(
            D::NodeMap { key: k(&[1, 2, 3]) }.cmp(&D::NodeMap { key: k(&[1, 2, 4]) }),
            Ordering::Less
        );
        assert_eq!(
            D::NodeMap { key: k(&[1, 3]) }.cmp(&D::NodeMap {
                key: k(&[1, 2, 255])
            }),
            Ordering::Greater
        );
    }

    #[test]
    fn dtype_encode_tags_and_payloads() {
        // NodeMeta tag only
        assert_eq!(D::NodeMeta.encode().as_ref(), &[0x00]);

        // NodeMap: 0x01 + key bytes
        let original = D::NodeMap {
            key: k(&[0xAA, 0xBB, 0xCC]),
        };
        let enc = original.encode();
        assert_eq!(enc.as_ref(), &[0x01, 0xAA, 0xBB, 0xCC]);

        // Empty key allowed
        let original_empty = D::NodeMap { key: k(&[]) };
        let enc_empty = original_empty.encode();
        assert_eq!(enc_empty.as_ref(), &[0x01]);
    }

    #[test]
    fn dtype_decode_cases() {
        // Empty input -> error
        assert_eq!(D::decode(&[]), Err(DecodeError::IncorrectLength));

        // NodeMeta single tag
        assert!(matches!(D::decode(&[0x00]), Ok(c) if matches!(c.as_ref(), D::NodeMeta)));

        // NodeMap empty key
        assert!(matches!(
            D::decode(&[0x01]),
            Ok(c) if matches!(c.as_ref(), D::NodeMap { key } if key.is_empty())
        ));

        // NodeMap non-empty key
        assert!(matches!(
            D::decode(&[0x01, 0x10, 0x20]),
            Ok(c) if matches!(c.as_ref(), D::NodeMap { key } if key.as_ref() == [0x10, 0x20])
        ));

        // Unknown tag
        assert_eq!(
            D::decode(&[0xFF]),
            Err(DecodeError::Custom(
                "Invalid SnapshotNodeDataType variant prefix"
            ))
        );
    }

    #[test]
    fn dtype_decode_owned_matches_borrowed() {
        // NodeMeta
        let owned = D::decode_owned(vec![0x00]).unwrap();
        assert!(matches!(owned, D::NodeMeta));

        // NodeMap empty key
        let owned = D::decode_owned(vec![0x01]).unwrap();
        assert!(matches!(owned, D::NodeMap { ref key } if key.is_empty()));

        // NodeMap non-empty key
        let owned = D::decode_owned(vec![0x01, 0xDE, 0xAD]).unwrap();
        assert!(matches!(owned, D::NodeMap { ref key } if key.as_ref() == [0xDE, 0xAD]));
    }

    #[test]
    fn dtype_round_trip_encode_decode() {
        let cases = vec![
            D::NodeMeta,
            D::NodeMap { key: k(&[]) },
            D::NodeMap { key: k(&[1, 2, 3]) },
        ];

        test_encode_decode_round_trip::<SnapshotNodeDataType<TestSchema>>(cases);
    }

    // --------------------- SnapshotKeyTreePart ---------------------

    #[test]
    fn keytree_eq_and_ord_basic() {
        // Equality
        let a = K {
            node_height: 0,
            node_commit_id: H256::from(&[0x00; 32]),
            node_data_type: D::NodeMeta,
        };
        let b = K {
            node_height: 0,
            node_commit_id: H256::from(&[0x00; 32]),
            node_data_type: D::NodeMeta,
        };
        assert_eq!(a, b);

        // Different by height
        let h0 = K {
            node_height: 0,
            node_commit_id: H256::from(&[0x00; 32]),
            node_data_type: D::NodeMeta,
        };
        let h1 = K {
            node_height: 1,
            node_commit_id: H256::from(&[0x00; 32]),
            node_data_type: D::NodeMeta,
        };
        assert!(h0 < h1);

        // Same height, different commit id
        let c0 = K {
            node_height: 5,
            node_commit_id: H256::from(&[0x00; 32]),
            node_data_type: D::NodeMeta,
        };
        let c1 = K {
            node_height: 5,
            node_commit_id: H256::from(&[0x01; 32]),
            node_data_type: D::NodeMeta,
        };
        assert!(c0 < c1);

        // Same height and commit id, NodeMeta < NodeMap
        let m0 = K {
            node_height: 7,
            node_commit_id: H256::from(&[0x10; 32]),
            node_data_type: D::NodeMeta,
        };
        let m1 = K {
            node_height: 7,
            node_commit_id: H256::from(&[0x10; 32]),
            node_data_type: D::NodeMap { key: k(&[0x00]) },
        };
        assert!(m0 < m1);

        // NodeMap ordering by key when height and commit id match
        let k1 = K {
            node_height: 9,
            node_commit_id: H256::from(&[0xFF; 32]),
            node_data_type: D::NodeMap { key: k(&[1]) },
        };
        let k2 = K {
            node_height: 9,
            node_commit_id: H256::from(&[0xFF; 32]),
            node_data_type: D::NodeMap { key: k(&[1, 0]) },
        };
        assert!(k1 < k2);
    }

    #[test]
    fn keytree_sort_sequence() {
        // The expected order is by:
        // 1) node_height (ascending)
        // 2) node_commit_id (ascending)
        // 3) node_data_type (NodeMeta before NodeMap, then key ordering)
        let mut items = vec![
            K {
                node_height: 2,
                node_commit_id: H256::from(&[0x02; 32]),
                node_data_type: D::NodeMap { key: k(&[2]) },
            },
            K {
                node_height: 1,
                node_commit_id: H256::from(&[0x02; 32]),
                node_data_type: D::NodeMeta,
            },
            K {
                node_height: 1,
                node_commit_id: H256::from(&[0x02; 32]),
                node_data_type: D::NodeMap { key: k(&[1, 2, 3]) },
            },
            K {
                node_height: 1,
                node_commit_id: H256::from(&[0x02; 32]),
                node_data_type: D::NodeMap { key: k(&[1, 2]) },
            },
            K {
                node_height: 1,
                node_commit_id: H256::from(&[0x01; 32]),
                node_data_type: D::NodeMeta,
            },
            K {
                node_height: 1,
                node_commit_id: H256::from(&[0x01; 32]),
                node_data_type: D::NodeMap { key: k(&[0x00]) },
            },
            K {
                node_height: 2,
                node_commit_id: H256::from(&[0x02; 32]),
                node_data_type: D::NodeMeta,
            },
            K {
                node_height: 1,
                node_commit_id: H256::from(&[0x01; 32]),
                node_data_type: D::NodeMap { key: k(&[0xFF]) },
            },
            K {
                node_height: 0,
                node_commit_id: H256::from(&[0x00; 32]),
                node_data_type: D::NodeMeta,
            },
        ];

        items.sort();

        assert_eq!(
            items,
            vec![
                // height 0
                K {
                    node_height: 0,
                    node_commit_id: H256::from(&[0x00; 32]),
                    node_data_type: D::NodeMeta
                },
                // height 1, commit 0x01.. < 0x02..
                K {
                    node_height: 1,
                    node_commit_id: H256::from(&[0x01; 32]),
                    node_data_type: D::NodeMeta
                },
                K {
                    node_height: 1,
                    node_commit_id: H256::from(&[0x01; 32]),
                    node_data_type: D::NodeMap { key: k(&[0x00]) }
                },
                K {
                    node_height: 1,
                    node_commit_id: H256::from(&[0x01; 32]),
                    node_data_type: D::NodeMap { key: k(&[0xFF]) }
                },
                // height 1, commit 0x02..
                K {
                    node_height: 1,
                    node_commit_id: H256::from(&[0x02; 32]),
                    node_data_type: D::NodeMeta
                },
                K {
                    node_height: 1,
                    node_commit_id: H256::from(&[0x02; 32]),
                    node_data_type: D::NodeMap { key: k(&[1, 2]) }
                },
                K {
                    node_height: 1,
                    node_commit_id: H256::from(&[0x02; 32]),
                    node_data_type: D::NodeMap { key: k(&[1, 2, 3]) }
                },
                // height 2
                K {
                    node_height: 2,
                    node_commit_id: H256::from(&[0x02; 32]),
                    node_data_type: D::NodeMeta
                },
                K {
                    node_height: 2,
                    node_commit_id: H256::from(&[0x02; 32]),
                    node_data_type: D::NodeMap { key: k(&[2]) }
                },
            ]
        );
    }

    #[test]
    fn keytree_encode_layout_and_decode() {
        // Construct a key: height | commit_id | dtype(tag+payload)
        let value = K {
            node_height: 42,
            node_commit_id: H256::from(&[0xAB; 32]),
            node_data_type: D::NodeMap {
                key: k(&[0x10, 0x20]),
            },
        };

        // Encode
        let enc = value.encode();

        // Decode (borrowed)
        let decoded = K::decode(enc.as_ref()).unwrap();
        assert_eq!(decoded.as_ref(), &value);

        // Decode (owned)
        let decoded_owned = K::decode_owned(enc.into_owned()).unwrap();
        assert_eq!(decoded_owned, value);
    }

    #[test]
    fn keytree_decode_errors() {
        // Too short for fixed prefix
        let fixed = u64::LENGTH + <TestSchema as PendingKeyValueSchema>::CommitId::LENGTH;
        let short = vec![0u8; fixed - 1];
        assert_eq!(K::decode(&short), Err(DecodeError::IncorrectLength));
        assert_eq!(K::decode_owned(short), Err(DecodeError::IncorrectLength));

        // Valid fixed prefix but invalid dtype tag
        // Compose: height(8) | commit(CommitId::LENGTH) | tag(0xFF)
        let mut buf = Vec::new();
        buf.extend_from_slice(u64::encode(&123u64).as_ref());
        buf.extend_from_slice(
            <TestSchema as PendingKeyValueSchema>::CommitId::encode(&H256::from(&[0x11; 32]))
                .as_ref(),
        );
        buf.push(0xFF);

        assert_eq!(
            K::decode(&buf),
            Err(DecodeError::Custom(
                "Invalid SnapshotNodeDataType variant prefix"
            ))
        );
        assert_eq!(
            K::decode_owned(buf),
            Err(DecodeError::Custom(
                "Invalid SnapshotNodeDataType variant prefix"
            ))
        );
    }

    #[test]
    fn keytree_round_trip_encode_decode() {
        let cases = vec![
            K {
                node_height: 0,
                node_commit_id: H256::from(&[0x00; 32]),
                node_data_type: D::NodeMeta,
            },
            K {
                node_height: 0,
                node_commit_id: H256::from(&[0x00; 32]),
                node_data_type: D::NodeMap { key: k(&[]) },
            },
            K {
                node_height: 0,
                node_commit_id: H256::from(&[0x00; 32]),
                node_data_type: D::NodeMap { key: k(&[1, 2, 3]) },
            },
            K {
                node_height: 5,
                node_commit_id: H256::from(&[0xAB; 32]),
                node_data_type: D::NodeMeta,
            },
            K {
                node_height: 5,
                node_commit_id: H256::from(&[0xAB; 32]),
                node_data_type: D::NodeMap { key: k(&[0xFF]) },
            },
            K {
                node_height: u64::MAX,
                node_commit_id: H256::from(&[0xFF; 32]),
                node_data_type: D::NodeMeta,
            },
        ];

        test_encode_decode_round_trip::<SnapshotKeyTreePart<TestSchema>>(cases);
    }
}
