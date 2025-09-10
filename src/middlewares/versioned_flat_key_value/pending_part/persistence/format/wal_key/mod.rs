mod specific_part;

use std::borrow::Cow;

pub use self::specific_part::WalKeySpecificPart;

use super::{
    super::{
        DecResult, Decode, DecodeError, Encode, EncodeSubKey, FixedLengthEncoded,
        PendingKeyValueSchema, SeekKey,
    },
    identifiers::{ModificationId, SnapshotId},
};

#[derive(Clone, Debug)]
pub struct WalKey<S: PendingKeyValueSchema> {
    pub snapshot_id: SnapshotId,
    pub modification_id: ModificationId,
    pub operation_specific_parts: WalKeySpecificPart<S>,
}

impl<S: PendingKeyValueSchema> WalKey<S> {
    /// Generates the `SeekKey` to start an iteration from the first possible entry
    /// of a given `snapshot_id` and `modification_id`.
    ///
    /// The returned `SeekKey` contains a fully-formed `WalKey` instance that represents
    /// the lexicographically smallest possible key for that snapshot and modification.
    ///
    /// # How to Use
    /// Pass `seek_key.key` to a `TableRead::iter` method.
    ///
    /// ```
    /// // let seek_key = WalKey::seek_key_for_snap_mod_id(snapshot_id, modification_id);
    /// // let iterator = table_reader.iter(&seek_key.key)?;
    /// ```
    ///
    /// **Note**: This only provides a starting point. The caller is responsible for
    /// checking the keys during iteration and stopping once the key's
    /// `(snapshot_id, modification_id)` no longer matches the target.
    pub fn seek_key_for_snap_mod_id(
        snapshot_id: SnapshotId,
        modification_id: ModificationId,
    ) -> SeekKey<Self> {
        // Construct the logical starting point of the scan.
        let start_key = WalKey {
            snapshot_id,
            modification_id,
            operation_specific_parts: WalKeySpecificPart::AddNodeMeta,
        };

        SeekKey { key: start_key }
    }

    /// Generates the `SeekKey` to start an iteration from the first possible entry
    /// of a given `snapshot_id`.
    ///
    /// The returned `SeekKey` contains a fully-formed `WalKey` instance that represents
    /// the lexicographically smallest possible key for that snapshot.
    ///
    /// # How to Use
    /// Pass `seek_key.key` to a `TableRead::iter` method.
    ///
    /// ```
    /// // let seek_key = WalKey::seek_key_for_snapshot(snapshot_id);
    /// // let iterator = table_reader.iter(&seek_key.key)?;
    /// ```
    ///
    /// **Note**: This only provides a starting point. The caller is responsible for
    /// checking the keys during iteration and stopping once the key's
    /// `snapshot_id` no longer matches the target.
    pub fn seek_key_for_snapshot(snapshot_id: SnapshotId) -> SeekKey<Self> {
        // Construct the logical starting point of the scan.
        let start_key = WalKey {
            snapshot_id,
            modification_id: ModificationId(0),
            operation_specific_parts: WalKeySpecificPart::AddNodeMeta,
        };

        SeekKey { key: start_key }
    }
}

impl<S: PendingKeyValueSchema> PartialEq for WalKey<S>
where
    S::Key: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.snapshot_id == other.snapshot_id
            && self.modification_id == other.modification_id
            && self.operation_specific_parts == other.operation_specific_parts
    }
}

impl<S: PendingKeyValueSchema> Eq for WalKey<S> where S::Key: Eq {}

impl<S: PendingKeyValueSchema> PartialOrd for WalKey<S>
where
    S::Key: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<S: PendingKeyValueSchema> Ord for WalKey<S>
where
    S::Key: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.snapshot_id
            .cmp(&other.snapshot_id)
            .then_with(|| self.modification_id.cmp(&other.modification_id))
            .then_with(|| {
                self.operation_specific_parts
                    .cmp(&other.operation_specific_parts)
            })
    }
}

impl<S: PendingKeyValueSchema> Encode for WalKey<S> {
    fn encode(&self) -> Cow<[u8]> {
        let encoded_snapshot_id = self.snapshot_id.encode();
        let encoded_modification_id = self.modification_id.encode();
        let encoded_specific_parts = self.operation_specific_parts.encode();

        let mut vec = Vec::with_capacity(
            SnapshotId::LENGTH + ModificationId::LENGTH + encoded_specific_parts.len(),
        );

        vec.extend_from_slice(encoded_snapshot_id.as_ref());
        vec.extend_from_slice(encoded_modification_id.as_ref());
        vec.extend_from_slice(encoded_specific_parts.as_ref());

        Cow::Owned(vec)
    }
}

impl<S: PendingKeyValueSchema> EncodeSubKey for WalKey<S> {
    const HAVE_SUBKEY: bool = true;

    fn encode_subkey(&self) -> (Cow<[u8]>, Cow<[u8]>) {
        let encoded_snapshot_id = self.snapshot_id.encode();
        let encoded_modification_id = self.modification_id.encode();

        let mut prefix_vec =
            Vec::with_capacity(encoded_snapshot_id.len() + encoded_modification_id.len());
        prefix_vec.extend_from_slice(encoded_snapshot_id.as_ref());
        prefix_vec.extend_from_slice(encoded_modification_id.as_ref());

        let prefix = Cow::Owned(prefix_vec);

        let suffix = self.operation_specific_parts.encode();

        (prefix, suffix)
    }

    fn encode_subkey_owned(input: <Self as ToOwned>::Owned) -> (Vec<u8>, Vec<u8>) {
        let mut prefix_vec = SnapshotId::encode_owned(input.snapshot_id);
        let encoded_modification_id = ModificationId::encode_owned(input.modification_id);
        prefix_vec.extend_from_slice(&encoded_modification_id);

        let suffix_vec = WalKeySpecificPart::<S>::encode_owned(input.operation_specific_parts);

        (prefix_vec, suffix_vec)
    }
}

impl<S: PendingKeyValueSchema> Decode for WalKey<S> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        let fixed_part_len = SnapshotId::LENGTH + ModificationId::LENGTH;

        if input.len() < fixed_part_len {
            return Err(DecodeError::IncorrectLength);
        }

        let mut cursor = 0;

        let snapshot_id_slice = &input[cursor..cursor + SnapshotId::LENGTH];
        let snapshot_id = SnapshotId::decode(snapshot_id_slice)?.into_owned();
        cursor += SnapshotId::LENGTH;

        let modification_id_slice = &input[cursor..cursor + ModificationId::LENGTH];
        let modification_id = ModificationId::decode(modification_id_slice)?.into_owned();
        cursor += ModificationId::LENGTH;

        let specific_parts_slice = &input[cursor..];
        let operation_specific_parts =
            WalKeySpecificPart::<S>::decode(specific_parts_slice)?.into_owned();

        Ok(Cow::Owned(WalKey {
            snapshot_id,
            modification_id,
            operation_specific_parts,
        }))
    }

    fn decode_owned(mut input: Vec<u8>) -> DecResult<Self> {
        let fixed_part_len = SnapshotId::LENGTH + ModificationId::LENGTH;
        if input.len() < fixed_part_len {
            return Err(DecodeError::IncorrectLength);
        }

        let specific_parts_data = input.split_off(fixed_part_len);

        let mod_id_data = input.split_off(SnapshotId::LENGTH);

        let snapshot_id = SnapshotId::decode_owned(input)?;
        let modification_id = ModificationId::decode_owned(mod_id_data)?;
        let operation_specific_parts = WalKeySpecificPart::<S>::decode_owned(specific_parts_data)?;

        Ok(WalKey {
            snapshot_id,
            modification_id,
            operation_specific_parts,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::test_util::*;
    use super::*;
    use std::cmp::Ordering;

    type WKey = WalKey<TestSchema>;
    type WPart = WalKeySpecificPart<TestSchema>;

    fn sid(n: u64) -> SnapshotId {
        SnapshotId(n)
    }

    fn mid(n: u64) -> ModificationId {
        ModificationId(n)
    }

    #[test]
    fn ord_and_eq_across_fields() {
        // Same everything
        let a = WKey {
            snapshot_id: sid(1),
            modification_id: mid(2),
            operation_specific_parts: WPart::AddNodeMeta,
        };
        let b = a.clone();
        assert_eq!(a, b);
        assert_eq!(a.cmp(&b), Ordering::Equal);

        // SnapshotId decides first
        let s_small = WKey {
            snapshot_id: sid(1),
            modification_id: mid(100),
            operation_specific_parts: WPart::DiscardMeta,
        };
        let s_big = WKey {
            snapshot_id: sid(2),
            modification_id: mid(0),
            operation_specific_parts: WPart::AddNodeMeta,
        };
        assert!(s_small < s_big);

        // Same snapshot, ModificationId decides
        let m_small = WKey {
            snapshot_id: sid(7),
            modification_id: mid(1),
            operation_specific_parts: WPart::MakePivotMeta,
        };
        let m_big = WKey {
            snapshot_id: sid(7),
            modification_id: mid(2),
            operation_specific_parts: WPart::AddNodeMeta,
        };
        assert!(m_small < m_big);

        // Same snapshot & mod, operation_specific_parts decides
        let o1 = WKey {
            snapshot_id: sid(9),
            modification_id: mid(9),
            operation_specific_parts: WPart::AddNodeMeta, // tag 0
        };
        let o2 = WKey {
            snapshot_id: sid(9),
            modification_id: mid(9),
            operation_specific_parts: WPart::AddNodeMapKey(k(&[0x00])), // tag 1
        };
        let o3 = WKey {
            snapshot_id: sid(9),
            modification_id: mid(9),
            operation_specific_parts: WPart::ChangeRootMeta, // tag 2
        };
        assert!(o1 < o2);
        assert!(o2 < o3);

        // Within AddNodeMapKey, compare by key ordering
        let k1 = WKey {
            snapshot_id: sid(5),
            modification_id: mid(5),
            operation_specific_parts: WPart::AddNodeMapKey(k(&[1, 2])),
        };
        let k2 = WKey {
            snapshot_id: sid(5),
            modification_id: mid(5),
            operation_specific_parts: WPart::AddNodeMapKey(k(&[1, 2, 0])),
        };
        assert!(k1 < k2);
    }

    #[test]
    fn encode_and_decode_round_trip() {
        let cases = vec![
            WKey {
                snapshot_id: sid(0),
                modification_id: mid(0),
                operation_specific_parts: WPart::AddNodeMeta,
            },
            WKey {
                snapshot_id: sid(1),
                modification_id: mid(2),
                operation_specific_parts: WPart::AddNodeMapKey(k(&[])),
            },
            WKey {
                snapshot_id: sid(1),
                modification_id: mid(2),
                operation_specific_parts: WPart::AddNodeMapKey(k(&[0xAA, 0xBB])),
            },
            WKey {
                snapshot_id: sid(9),
                modification_id: mid(3),
                operation_specific_parts: WPart::ChangeRootMeta,
            },
            WKey {
                snapshot_id: sid(9),
                modification_id: mid(3),
                operation_specific_parts: WPart::MakePivotMeta,
            },
            WKey {
                snapshot_id: sid(9),
                modification_id: mid(3),
                operation_specific_parts: WPart::DiscardMeta,
            },
        ];

        for case in cases {
            // encode / decode
            let enc = case.encode();
            let dec = WKey::decode(enc.as_ref()).expect("decode should succeed");
            assert_eq!(dec.as_ref(), &case);

            // decode_owned path
            let owned = enc.into_owned();
            let dec_owned = WKey::decode_owned(owned).expect("decode_owned should succeed");
            assert_eq!(dec_owned, case);
        }
    }

    #[test]
    fn encode_layout_prefix_suffix() {
        // Verify EncodeSubKey splits into prefix(snapshot_id+mod_id) and suffix(specific parts)
        let key = WKey {
            snapshot_id: sid(42),
            modification_id: mid(7),
            operation_specific_parts: WPart::AddNodeMapKey(k(&[0xDE, 0xAD])),
        };

        // Full encoding must equal prefix + suffix
        let full = key.encode().into_owned();
        let (prefix, suffix) = key.encode_subkey();
        let mut joined = Vec::with_capacity(prefix.len() + suffix.len());
        joined.extend_from_slice(prefix.as_ref());
        joined.extend_from_slice(suffix.as_ref());
        assert_eq!(joined, full);

        // encode_subkey_owned path yields same bytes
        let (prefix_o, suffix_o) = WKey::encode_subkey_owned(key.clone());
        let mut joined_o = Vec::with_capacity(prefix_o.len() + suffix_o.len());
        joined_o.extend_from_slice(&prefix_o);
        joined_o.extend_from_slice(&suffix_o);
        assert_eq!(joined_o, full);
    }

    #[test]
    fn decode_errors_incomplete_input() {
        // Fixed part is SnapshotId::LENGTH + ModificationId::LENGTH bytes; using fewer should error.
        // We don’t know lengths here, but we can craft minimal wrong input:
        // Try with empty and with too-short buffer.
        assert_eq!(WKey::decode(&[]), Err(DecodeError::IncorrectLength));
        assert_eq!(WKey::decode(&[0x00]), Err(DecodeError::IncorrectLength));

        // For decode_owned as well
        assert_eq!(
            WKey::decode_owned(vec![]),
            Err(DecodeError::IncorrectLength)
        );
        assert_eq!(
            WKey::decode_owned(vec![0x00]),
            Err(DecodeError::IncorrectLength)
        );
    }

    #[test]
    fn seek_key_for_snap_mod_id_produces_minimal_variant() {
        let s = sid(77);
        let m = mid(88);
        let seek = WKey::seek_key_for_snap_mod_id(s, m);

        // Should be the minimal operation-specific variant: AddNodeMeta
        assert_eq!(seek.key.snapshot_id, s);
        assert_eq!(seek.key.modification_id, m);
        assert!(matches!(
            seek.key.operation_specific_parts,
            WPart::AddNodeMeta
        ));

        // Check that any other key with same (s, m) is >= this seek key
        let greater = WKey {
            snapshot_id: s,
            modification_id: m,
            operation_specific_parts: WPart::AddNodeMapKey(k(&[])),
        };
        assert!(seek.key <= greater);
    }

    #[test]
    fn seek_key_for_snapshot_produces_minimal_pair() {
        let s = sid(5);
        let seek = WKey::seek_key_for_snapshot(s);

        // modification_id should be zero and part minimal
        assert_eq!(seek.key.snapshot_id, s);
        assert_eq!(seek.key.modification_id, ModificationId(0));
        assert!(matches!(
            seek.key.operation_specific_parts,
            WPart::AddNodeMeta
        ));

        // For same snapshot, any key with higher mod id or same mod but higher part is >= seek
        let same_snap_higher_mod = WKey {
            snapshot_id: s,
            modification_id: ModificationId(1),
            operation_specific_parts: WPart::AddNodeMeta,
        };
        let same_snap_same_mod_higher_part = WKey {
            snapshot_id: s,
            modification_id: ModificationId(0),
            operation_specific_parts: WPart::AddNodeMapKey(k(&[0x00])),
        };
        assert!(seek.key <= same_snap_higher_mod);
        assert!(seek.key <= same_snap_same_mod_higher_part);

        // Smaller snapshot should be strictly less than seek key
        let smaller_snapshot = WKey {
            snapshot_id: SnapshotId(4),
            modification_id: ModificationId(999),
            operation_specific_parts: WPart::DiscardMeta,
        };
        assert!(smaller_snapshot < seek.key);
    }

    #[test]
    fn full_key_ordering_sequence() {
        let mut items = vec![
            WKey {
                snapshot_id: sid(2),
                modification_id: mid(0),
                operation_specific_parts: WPart::DiscardMeta,
            },
            WKey {
                snapshot_id: sid(1),
                modification_id: mid(0),
                operation_specific_parts: WPart::AddNodeMapKey(k(&[2])),
            },
            WKey {
                snapshot_id: sid(1),
                modification_id: mid(0),
                operation_specific_parts: WPart::AddNodeMeta,
            },
            WKey {
                snapshot_id: sid(0),
                modification_id: mid(9),
                operation_specific_parts: WPart::ChangeRootMeta,
            },
            WKey {
                snapshot_id: sid(1),
                modification_id: mid(0),
                operation_specific_parts: WPart::AddNodeMapKey(k(&[1, 2])),
            },
            WKey {
                snapshot_id: sid(1),
                modification_id: mid(1),
                operation_specific_parts: WPart::AddNodeMeta,
            },
            WKey {
                snapshot_id: sid(0),
                modification_id: mid(9),
                operation_specific_parts: WPart::MakePivotMeta,
            },
        ];

        items.sort();

        assert_eq!(
            items,
            vec![
                // snapshot 0 first, order by mod then part
                WKey {
                    snapshot_id: sid(0),
                    modification_id: mid(9),
                    operation_specific_parts: WPart::ChangeRootMeta
                },
                WKey {
                    snapshot_id: sid(0),
                    modification_id: mid(9),
                    operation_specific_parts: WPart::MakePivotMeta
                },
                // snapshot 1, mod 0: AddNodeMeta < AddNodeMapKey([...]) with lexicographic key order
                WKey {
                    snapshot_id: sid(1),
                    modification_id: mid(0),
                    operation_specific_parts: WPart::AddNodeMeta
                },
                WKey {
                    snapshot_id: sid(1),
                    modification_id: mid(0),
                    operation_specific_parts: WPart::AddNodeMapKey(k(&[1, 2]))
                },
                WKey {
                    snapshot_id: sid(1),
                    modification_id: mid(0),
                    operation_specific_parts: WPart::AddNodeMapKey(k(&[2]))
                },
                // snapshot 1, mod 1
                WKey {
                    snapshot_id: sid(1),
                    modification_id: mid(1),
                    operation_specific_parts: WPart::AddNodeMeta
                },
                // snapshot 2
                WKey {
                    snapshot_id: sid(2),
                    modification_id: mid(0),
                    operation_specific_parts: WPart::DiscardMeta
                },
            ]
        );
    }
}
