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

#[cfg(test)]
mod tests {
    use ethereum_types::H256;

    use super::super::super::test_util::*;
    use super::*;

    type D = SnapshotNodeDataType<TestSchema>;
    type KTree = SnapshotKeyTreePart<TestSchema>;
    type R = SnapshotRecordType<TestSchema>;
    type K = SnapshotKey<TestSchema>;

    // --------------------- SnapshotRecordType ---------------------

    #[test]
    fn snapshot_record_type_eq_and_ord_basic() {
        // Equality among identical unit and tuple variants
        assert_eq!(R::Meta, R::Meta);

        let map_a = R::Map(KTree {
            node_height: 0,
            node_commit_id: H256::from(&[0x00; 32]),
            node_data_type: D::NodeMeta,
        });
        let map_b = R::Map(KTree {
            node_height: 0,
            node_commit_id: H256::from(&[0x00; 32]),
            node_data_type: D::NodeMeta,
        });
        let map_c = R::Map(KTree {
            node_height: 1,
            node_commit_id: H256::from(&[0x00; 32]),
            node_data_type: D::NodeMeta,
        });

        assert_eq!(map_a, map_b);
        assert_ne!(map_a, map_c);
        assert_ne!(R::Meta, map_a);

        // Ord: Meta < Map
        assert!(R::Meta < map_a);

        // Ord within Map follows KTree ordering
        let map_k1 = R::Map(KTree {
            node_height: 2,
            node_commit_id: H256::from(&[0x10; 32]),
            node_data_type: D::NodeMap { key: k(&[1]) },
        });
        let map_k2 = R::Map(KTree {
            node_height: 2,
            node_commit_id: H256::from(&[0x10; 32]),
            node_data_type: D::NodeMap { key: k(&[1, 0]) },
        });
        assert!(map_k1 < map_k2);
    }

    #[test]
    fn snapshot_record_type_encode_tags_and_payloads() {
        // Meta tag only
        assert_eq!(R::Meta.encode().as_ref(), &[0x00]);

        // Map: 0x01 + encoded KTree
        let part = KTree {
            node_height: 42,
            node_commit_id: H256::from(&[0xAB; 32]),
            node_data_type: D::NodeMap {
                key: k(&[0x10, 0x20]),
            },
        };
        let r = R::Map(part.clone());
        let enc = r.encode();

        // Verify prefix tag and that the tail decodes back to the same KTree
        assert_eq!(enc.as_ref()[0], 0x01);
        let decoded_tail = KTree::decode(&enc.as_ref()[1..]).unwrap();
        assert_eq!(decoded_tail.as_ref(), &part);
    }

    #[test]
    fn snapshot_record_type_decode_cases() {
        // Empty input -> error
        assert_eq!(R::decode(&[]), Err(DecodeError::IncorrectLength));

        // Meta single-byte tag
        assert!(matches!(R::decode(&[0x00]), Ok(c) if matches!(c.as_ref(), R::Meta)));

        // Map with valid KTree payload
        let kt = KTree {
            node_height: 7,
            node_commit_id: H256::from(&[0xCD; 32]),
            node_data_type: D::NodeMeta,
        };
        let mut buf = vec![0x01];
        buf.extend_from_slice(kt.encode().as_ref());
        let decoded = R::decode(&buf).unwrap();
        assert!(matches!(decoded.as_ref(), R::Map(p) if p == &kt));

        // Unknown tag
        assert_eq!(
            R::decode(&[0xFF]),
            Err(DecodeError::Custom(
                "Invalid SnapshotRecordType variant prefix"
            ))
        );
    }

    #[test]
    fn snapshot_record_type_decode_owned_matches_borrowed() {
        // Meta
        let owned_meta = R::decode_owned(vec![0x00]).unwrap();
        assert!(matches!(owned_meta, R::Meta));

        // Map with empty NodeMap key
        let kt = KTree {
            node_height: 0,
            node_commit_id: H256::from(&[0x11; 32]),
            node_data_type: D::NodeMap { key: k(&[]) },
        };
        let mut buf = vec![0x01];
        buf.extend_from_slice(kt.encode().as_ref());
        let owned_map = R::decode_owned(buf).unwrap();
        assert!(matches!(owned_map, R::Map(p) if p == kt));
    }

    #[test]
    fn snapshot_record_type_round_trip_encode_decode() {
        let cases = vec![
            R::Meta,
            R::Map(KTree {
                node_height: 0,
                node_commit_id: H256::from(&[0x00; 32]),
                node_data_type: D::NodeMeta,
            }),
            R::Map(KTree {
                node_height: 1,
                node_commit_id: H256::from(&[0x01; 32]),
                node_data_type: D::NodeMap { key: k(&[1, 2, 3]) },
            }),
        ];

        test_encode_decode_round_trip::<SnapshotRecordType<TestSchema>>(cases);
    }

    // --------------------- SnapshotKey ---------------------

    #[test]
    fn snapshot_key_eq_and_ord_basic() {
        // Equality
        let a = K {
            snapshot_root_height: 0,
            record_type: R::Meta,
        };
        let b = K {
            snapshot_root_height: 0,
            record_type: R::Meta,
        };
        assert_eq!(a, b);

        // Ordering by snapshot_root_height first
        let h0 = K {
            snapshot_root_height: 0,
            record_type: R::Meta,
        };
        let h1 = K {
            snapshot_root_height: 1,
            record_type: R::Meta,
        };
        assert!(h0 < h1);

        // Same height, Meta < Map
        let m0 = K {
            snapshot_root_height: 5,
            record_type: R::Meta,
        };
        let m1 = K {
            snapshot_root_height: 5,
            record_type: R::Map(KTree {
                node_height: 0,
                node_commit_id: H256::from(&[0x00; 32]),
                node_data_type: D::NodeMeta,
            }),
        };
        assert!(m0 < m1);

        // Same height and both Map: compare inner KTree
        let k1 = K {
            snapshot_root_height: 10,
            record_type: R::Map(KTree {
                node_height: 1,
                node_commit_id: H256::from(&[0xAA; 32]),
                node_data_type: D::NodeMap { key: k(&[1]) },
            }),
        };
        let k2 = K {
            snapshot_root_height: 10,
            record_type: R::Map(KTree {
                node_height: 1,
                node_commit_id: H256::from(&[0xAA; 32]),
                node_data_type: D::NodeMap { key: k(&[1, 0]) },
            }),
        };
        assert!(k1 < k2);
    }

    #[test]
    fn snapshot_key_sort_sequence() {
        // Expected order by:
        // 1) snapshot_root_height
        // 2) record_type (Meta before Map; Map ordered by KTree fields)
        let mut items = vec![
            K {
                snapshot_root_height: 2,
                record_type: R::Map(KTree {
                    node_height: 0,
                    node_commit_id: H256::from(&[0x02; 32]),
                    node_data_type: D::NodeMap { key: k(&[3]) },
                }),
            },
            K {
                snapshot_root_height: 1,
                record_type: R::Map(KTree {
                    node_height: 1,
                    node_commit_id: H256::from(&[0x02; 32]),
                    node_data_type: D::NodeMap { key: k(&[1, 2, 3]) },
                }),
            },
            K {
                snapshot_root_height: 1,
                record_type: R::Map(KTree {
                    node_height: 1,
                    node_commit_id: H256::from(&[0x02; 32]),
                    node_data_type: D::NodeMap { key: k(&[1, 2]) },
                }),
            },
            K {
                snapshot_root_height: 1,
                record_type: R::Meta,
            },
            K {
                snapshot_root_height: 0,
                record_type: R::Map(KTree {
                    node_height: 0,
                    node_commit_id: H256::from(&[0x00; 32]),
                    node_data_type: D::NodeMeta,
                }),
            },
            K {
                snapshot_root_height: 0,
                record_type: R::Meta,
            },
            K {
                snapshot_root_height: 2,
                record_type: R::Meta,
            },
        ];

        items.sort();

        assert_eq!(
            items,
            vec![
                // height 0
                K {
                    snapshot_root_height: 0,
                    record_type: R::Meta
                },
                K {
                    snapshot_root_height: 0,
                    record_type: R::Map(KTree {
                        node_height: 0,
                        node_commit_id: H256::from(&[0x00; 32]),
                        node_data_type: D::NodeMeta,
                    }),
                },
                // height 1
                K {
                    snapshot_root_height: 1,
                    record_type: R::Meta
                },
                K {
                    snapshot_root_height: 1,
                    record_type: R::Map(KTree {
                        node_height: 1,
                        node_commit_id: H256::from(&[0x02; 32]),
                        node_data_type: D::NodeMap { key: k(&[1, 2]) },
                    }),
                },
                K {
                    snapshot_root_height: 1,
                    record_type: R::Map(KTree {
                        node_height: 1,
                        node_commit_id: H256::from(&[0x02; 32]),
                        node_data_type: D::NodeMap { key: k(&[1, 2, 3]) },
                    }),
                },
                // height 2
                K {
                    snapshot_root_height: 2,
                    record_type: R::Meta
                },
                K {
                    snapshot_root_height: 2,
                    record_type: R::Map(KTree {
                        node_height: 0,
                        node_commit_id: H256::from(&[0x02; 32]),
                        node_data_type: D::NodeMap { key: k(&[3]) },
                    }),
                },
            ]
        );
    }

    #[test]
    fn snapshot_key_encode_layout_and_decode() {
        // Compose a SnapshotKey: encode(height) | encode(record_type)
        let value = K {
            snapshot_root_height: 123,
            record_type: R::Map(KTree {
                node_height: 42,
                node_commit_id: H256::from(&[0xAB; 32]),
                node_data_type: D::NodeMap {
                    key: k(&[0x10, 0x20]),
                },
            }),
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
    fn snapshot_key_decode_errors() {
        // Too short: must be at least u64::LENGTH
        let short = vec![0u8; u64::LENGTH - 1];
        assert_eq!(K::decode(&short), Err(DecodeError::IncorrectLength));
        assert_eq!(K::decode_owned(short), Err(DecodeError::IncorrectLength));

        // Valid u64 but invalid record_type tag afterwards
        let mut buf = Vec::new();
        buf.extend_from_slice(u64::encode(&777u64).as_ref());
        buf.push(0xFF); // invalid SnapshotRecordType tag

        assert_eq!(
            K::decode(&buf),
            Err(DecodeError::Custom(
                "Invalid SnapshotRecordType variant prefix"
            ))
        );
        assert_eq!(
            K::decode_owned(buf),
            Err(DecodeError::Custom(
                "Invalid SnapshotRecordType variant prefix"
            ))
        );
    }

    #[test]
    fn snapshot_key_encode_subkey_roundtrip_and_prefix_suffix() {
        // Ensure encode_subkey splits into the same bytes as encode
        let key = K {
            snapshot_root_height: 9,
            record_type: R::Map(KTree {
                node_height: 1,
                node_commit_id: H256::from(&[0xEE; 32]),
                node_data_type: D::NodeMeta,
            }),
        };

        let full = key.encode().into_owned();
        let (prefix, suffix) = key.encode_subkey();

        // Full should equal prefix || suffix
        let mut joined = prefix.into_owned();
        joined.extend_from_slice(suffix.as_ref());
        assert_eq!(joined, full);

        // encode_subkey_owned should produce identical parts
        let (prefix_o, suffix_o) = K::encode_subkey_owned(key.clone());
        let mut joined_o = prefix_o.clone();
        joined_o.extend_from_slice(&suffix_o);
        assert_eq!(joined_o, full);

        // Also verify decoding from concatenated parts matches original key
        let decoded = K::decode(&joined_o).unwrap();
        assert_eq!(decoded.into_owned(), key);
    }

    #[test]
    fn snapshot_key_round_trip_encode_decode() {
        let cases = vec![
            K {
                snapshot_root_height: 0,
                record_type: R::Meta,
            },
            K {
                snapshot_root_height: 0,
                record_type: R::Map(KTree {
                    node_height: 0,
                    node_commit_id: H256::from(&[0x00; 32]),
                    node_data_type: D::NodeMap { key: k(&[]) },
                }),
            },
            K {
                snapshot_root_height: 5,
                record_type: R::Map(KTree {
                    node_height: 7,
                    node_commit_id: H256::from(&[0xAB; 32]),
                    node_data_type: D::NodeMap { key: k(&[0xFF]) },
                }),
            },
            K {
                snapshot_root_height: u64::MAX,
                record_type: R::Meta,
            },
        ];

        test_encode_decode_round_trip::<SnapshotKey<TestSchema>>(cases);
    }
}
