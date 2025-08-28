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

        let suffix_vec = input.operation_specific_parts.encode().into_owned();

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
