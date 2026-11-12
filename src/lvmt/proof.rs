//! Proofs of a key's value at a commit, against the commit's state root.
//!
//! A key is held in AMT `A_d` at the depth `d ≥ 1` it was allocated at, where `A_j`
//! is the AMT whose id is the first `j` 16-bit words of `blake2s(key)` (`A_0` is
//! the root AMT) and the key's leaf in `A_j` is word `j`. A proof opens the key's
//! leaf in `A_0, A_1, …` down to where the answer is decided:
//!
//! - Each leaf opening is an AMT proof of the packed six slot versions against the
//!   AMT's commitment at the queried commit `Q`. The root commitment comes from
//!   `Q`'s state root; slot 5 of the leaf in `A_j` is the version of `A_{j+1}`,
//!   whose commitment is shown in the auth-change tree of the commit that wrote
//!   it (its "anchor").
//! - **Present** (also a deleted key, which keeps its slot): the leaf of `A_d`
//!   shows the key's `LvmtValue`, anchored, with the slot's version.
//! - **Absent** (never written): at every depth the leaf's occupied key slots are
//!   shown to hold other keys, down to a leaf with a free key slot, or with all
//!   five taken and no child AMT (slot 5 = 0). Slots are allocated in order and
//!   never freed, so the key would have been allocated there.
//!
//! An item with a version is current at `Q` because its version equals the slot
//! version at `Q` and versions only go up. An anchor is the item's Merkle path in
//! the auth-change tree of its commit `c`; if `c ≠ Q`, also `c`'s state root
//! parts and an MMR proof that `c`'s state root is accumulated in `Q`'s `hroot`.

use amt::{pack_slots, AmtParams, AmtProofError, Proof};
use ethereum_types::H256;

use super::{
    auth_changes::{amt_change_hash, key_value_hash},
    crypto::PE,
    mmr::MmrProof,
    state_root::{compute_state_root, StateRoot},
    storage::LvmtStoreReader,
    types::{
        auth_changes::hash_of_entries, compute_amt_node_id, key_digest, AmtId, AuthChangeKey,
        CurvePointWithVersion, LvmtValue, KEY_SLOT_SIZE, SLOT_SIZE,
    },
};
use crate::{
    backends::{DatabaseTrait, PendingTableName},
    errors::Result,
    middlewares::CommitID,
    traits::KeyValueStoreRead,
    StorageError,
};

/// The parts the state root of a commit is hashed from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRootParts {
    pub root_commitment: CurvePointWithVersion,
    pub auth_change_root: H256,
    pub hroot: H256,
}

impl StateRootParts {
    pub fn state_root(&self) -> StateRoot {
        compute_state_root(&self.root_commitment, &self.auth_change_root, &self.hroot)
    }
}

/// Path of an item in an auth-change tree: the entries of every node from the root
/// down to the leaf node holding the item, each with the position taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthChangeProof {
    pub path: Vec<(Vec<H256>, u8)>,
}

impl AuthChangeProof {
    /// The root of the tree the path puts `item` in, if the path is well formed.
    pub fn root_for(&self, item: &H256) -> Option<H256> {
        let mut hash = *item;
        for (entries, position) in self.path.iter().rev() {
            if entries.get(*position as usize) != Some(&hash) {
                return None;
            }
            hash = hash_of_entries(entries);
        }
        (!self.path.is_empty()).then_some(hash)
    }
}

/// Where an item (a key-value or an AMT commitment) was written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Anchor {
    /// The writing commit's state root parts and the proof that its state root is
    /// accumulated in the queried commit's `hroot`; `None` if the queried commit
    /// wrote the item.
    pub commit: Option<(StateRootParts, MmrProof)>,
    pub merkle: AuthChangeProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnchoredKey {
    pub key: Box<[u8]>,
    pub value: LvmtValue,
    pub anchor: Anchor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnchoredAmt {
    pub commitment: CurvePointWithVersion,
    pub anchor: Anchor,
}

/// The opening of the queried key's leaf in one AMT.
#[derive(Debug, Clone, PartialEq)]
pub struct LayerProof {
    pub slots: [u64; SLOT_SIZE],
    pub amt_proof: Proof<PE>,
    /// The keys in this leaf's key slots: the queried key if it is here, else the
    /// key of every occupied slot.
    pub keys: Vec<AnchoredKey>,
    /// The next AMT's commitment, if the proof goes one level deeper.
    pub child: Option<AnchoredAmt>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LvmtProof {
    pub commit: StateRootParts,
    /// From the root AMT down.
    pub layers: Vec<LayerProof>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LvmtProofError {
    #[error("state root mismatch")]
    StateRoot,
    #[error("AMT proof at depth {0}: {1:?}")]
    Amt(usize, AmtProofError),
    #[error("slot version does not fit at depth {0}")]
    SlotOverflow(usize),
    #[error("the root AMT leaf has key slots at depth 0")]
    RootKeySlots,
    #[error("key shown at depth {0} is not in this leaf or slot")]
    KeyPosition(usize),
    #[error("anchor of an item at depth {0} does not verify")]
    Anchor(usize),
    #[error("occupied slot without its key at depth {0}")]
    MissingSlotKey(usize),
    #[error("child AMT commitment at depth {0} does not match")]
    Child(usize),
    #[error("proof is longer or shorter than the decision needs")]
    Shape,
}

fn verify_anchor(
    queried: &StateRootParts,
    anchor: &Anchor,
    item: &H256,
) -> std::result::Result<(), ()> {
    let auth_change_root = match &anchor.commit {
        None => queried.auth_change_root,
        Some((parts, mmr)) => {
            if !mmr.verify(&queried.hroot, &parts.state_root()) {
                return Err(());
            }
            parts.auth_change_root
        }
    };
    if anchor.merkle.root_for(item) == Some(auth_change_root) {
        Ok(())
    } else {
        Err(())
    }
}

/// Checks `proof` for `key` against `state_root` and returns the proven value:
/// `Some` if the key is present (its `value` is `None` if deleted), `None` if the
/// key was never written.
pub fn verify_lvmt_proof(
    params: &AmtParams<PE>,
    state_root: &StateRoot,
    key: &[u8],
    proof: &LvmtProof,
) -> std::result::Result<Option<LvmtValue>, LvmtProofError> {
    use LvmtProofError::*;

    if proof.commit.state_root() != *state_root {
        return Err(StateRoot);
    }
    let digest = key_digest(key);
    let mut commitment = proof.commit.root_commitment.clone();
    let last = proof.layers.len().checked_sub(1).ok_or(Shape)?;
    if last > 15 {
        return Err(Shape);
    }

    // Whether every occupied key slot on the path so far is shown with its key,
    // as an absence needs: otherwise the key could hide in an unshown slot.
    let mut complete = true;
    for (j, layer) in proof.layers.iter().enumerate() {
        let mut amt_id = compute_amt_node_id(digest, j);
        let leaf = amt_id.pop().unwrap();
        let value = pack_slots(&layer.slots).ok_or(SlotOverflow(j))?;
        params
            .verify(
                &commitment.point.affine(),
                leaf as usize,
                value,
                &layer.amt_proof,
            )
            .map_err(|e| Amt(j, e))?;

        let key_slots = &layer.slots[..KEY_SLOT_SIZE];
        if j == 0 {
            if key_slots.iter().any(|v| *v != 0) || !layer.keys.is_empty() {
                return Err(RootKeySlots);
            }
        } else {
            let mut shown = [false; KEY_SLOT_SIZE];
            for item in &layer.keys {
                // Checked first: `amt_info` needs a depth an AMT id can hold.
                if item.value.allocation.depth as usize != j {
                    return Err(KeyPosition(j));
                }
                let (item_amt, item_leaf, slot) = item.value.allocation.amt_info(&item.key);
                let slot = slot as usize;
                if item_amt != amt_id
                    || item_leaf != leaf
                    || slot >= KEY_SLOT_SIZE
                    || key_slots[slot] == 0
                    || item.value.version != key_slots[slot]
                {
                    return Err(KeyPosition(j));
                }
                verify_anchor(
                    &proof.commit,
                    &item.anchor,
                    &key_value_hash(&item.key, &item.value),
                )
                .map_err(|_| Anchor(j))?;
                if *item.key == *key {
                    // Present: the current value of the key, in its own slot (the
                    // allocation was checked against the key's digest above).
                    if j != last || layer.child.is_some() {
                        return Err(Shape);
                    }
                    return Ok(Some(item.value.clone()));
                }
                shown[slot] = true;
            }
            complete &= key_slots
                .iter()
                .zip(shown)
                .all(|(version, shown)| *version == 0 || shown);
        }

        // Absent if the leaf has a free key slot (the key would have taken it), or
        // all are taken and there is no child AMT.
        if (j > 0 && key_slots.contains(&0)) || layer.slots[SLOT_SIZE - 1] == 0 {
            if j != last || layer.child.is_some() {
                return Err(Shape);
            }
            if !complete {
                return Err(MissingSlotKey(j));
            }
            return Ok(None);
        }

        let child = layer.child.as_ref().ok_or(Shape)?;
        if child.commitment.version != layer.slots[SLOT_SIZE - 1] {
            return Err(Child(j));
        }
        let child_id = amt_id.child(leaf);
        verify_anchor(
            &proof.commit,
            &child.anchor,
            &amt_change_hash(&child_id, &child.commitment),
        )
        .map_err(|_| Anchor(j))?;
        commitment = child.commitment.clone();
    }
    Err(Shape)
}

// Prover
impl<P: DatabaseTrait<PendingTableName>> LvmtStoreReader<'_, '_, P> {
    /// The value of `key` at `commit` (`None` if never written) with a proof
    /// against `commit`'s state root, see [`verify_lvmt_proof`].
    ///
    /// Historical commits more than `history_depth` below the historical tip are
    /// refused. Proof computation uses rayon (amt), in the calling thread pool.
    pub fn get_with_proof(
        &self,
        commit: CommitID,
        key: &[u8],
        params: &AmtParams<PE>,
    ) -> Result<(Option<LvmtValue>, LvmtProof)> {
        self.check_proof_depth(commit)?;
        let parts = self.state_root_parts(commit)?;
        let digest = key_digest(key);
        let key_box: Box<[u8]> = key.into();
        let value = self
            .key_value_reader()
            .get_versioned_store(&commit, false)?
            .get(&key_box)?;

        let root_versions = self.get_root_leaf_versions(commit)?;
        let mut layers = vec![];
        for j in 0.. {
            let mut amt_id = compute_amt_node_id(digest, j);
            let leaf = amt_id.pop().unwrap();

            let leaf_values = if j == 0 {
                root_versions
                    .iter()
                    .enumerate()
                    .filter(|(_, v)| **v > 0)
                    .map(|(i, v)| {
                        let mut slots = [0; SLOT_SIZE];
                        slots[SLOT_SIZE - 1] = *v;
                        (i as u16, slots)
                    })
                    .collect()
            } else {
                self.amt_leaf_slots(commit, &amt_id)?
            };
            let slots = leaf_values
                .iter()
                .find(|(i, _)| *i == leaf)
                .map_or([0; SLOT_SIZE], |(_, s)| *s);
            let amt_proof = params.prove_sparse(
                leaf_values
                    .iter()
                    .map(|(i, s)| (*i as usize, pack_slots(s).expect("versions fit in a slot"))),
                leaf as usize,
            );

            // Present: only the key, at its depth. Absent: every occupied slot.
            let mut keys = vec![];
            let mut done = false;
            if j > 0 {
                let present_here = value
                    .as_ref()
                    .is_some_and(|v| v.allocation.depth as usize == j);
                let leaf_keys = self.get_leaf_keys(commit, &amt_id, leaf)?;
                let leaf_keys = leaf_keys.as_ref().map_or(&[][..], |info| info.keys());
                let shown = leaf_keys.iter().filter(|k| {
                    if present_here {
                        ***k == *key
                    } else {
                        value.is_none()
                    }
                });
                for slot_key in shown {
                    let (written_at, slot_value) = self
                        .get_key_last_modified(commit, slot_key)?
                        .ok_or(StorageError::ConsistencyCheckFailure)?;
                    let item = key_value_hash(slot_key, &slot_value);
                    keys.push(AnchoredKey {
                        key: slot_key.clone(),
                        value: slot_value,
                        anchor: self.anchor(commit, written_at, &item)?,
                    });
                }
                done = present_here || slots[..KEY_SLOT_SIZE].contains(&0);
            }
            done = done || slots[SLOT_SIZE - 1] == 0;

            let child = if done {
                None
            } else {
                let child_id = amt_id.child(leaf);
                let (written_at, commitment) = self
                    .get_amt_last_modified(commit, &child_id)?
                    .ok_or(StorageError::ConsistencyCheckFailure)?;
                let item = amt_change_hash(&child_id, &commitment);
                Some(AnchoredAmt {
                    commitment,
                    anchor: self.anchor(commit, written_at, &item)?,
                })
            };
            layers.push(LayerProof {
                slots,
                amt_proof,
                keys,
                child,
            });
            if done {
                break;
            }
        }
        Ok((
            value,
            LvmtProof {
                commit: parts,
                layers,
            },
        ))
    }

    fn check_proof_depth(&self, commit: CommitID) -> Result<()> {
        let reader = self.key_value_reader();
        if reader.query_commit_existence(&commit)? && !reader.is_in_historical_part(&commit)? {
            return Ok(());
        }
        let q = reader.get_history_number_by_commit_id(commit)?;
        let tip = reader
            .get_latest_historical_commit()
            .expect("a historical commit exists");
        let tip = reader.get_history_number_by_commit_id(tip)?;
        if tip - q > self.proof_config().history_depth {
            return Err(StorageError::ProofUnavailable(format!(
                "{commit:?} is {} commits below the historical tip, beyond the history depth {}",
                tip - q,
                self.proof_config().history_depth
            )));
        }
        Ok(())
    }

    /// The state root parts of `commit`.
    pub fn state_root_parts(&self, commit: CommitID) -> Result<StateRootParts> {
        Ok(StateRootParts {
            root_commitment: self.get_root_commitment(commit)?,
            auth_change_root: self
                .get_auth_change_root(commit)?
                .ok_or(StorageError::CommitIDNotFound)?,
            hroot: self.get_hroot(commit)?,
        })
    }

    /// The six slot versions at `commit` of every leaf of AMT `amt_id` (not the
    /// root) with an allocated key slot, in increasing leaf order.
    fn amt_leaf_slots(
        &self,
        commit: CommitID,
        amt_id: &AmtId,
    ) -> Result<Vec<(u16, [u64; SLOT_SIZE])>> {
        let key_values = self
            .key_value_reader()
            .get_versioned_store(&commit, false)?;
        let amt_nodes = self.amt_node_reader().get_versioned_store(&commit, false)?;
        let mut out = vec![];
        for (leaf, info) in self.get_amt_slot_keys(commit, amt_id)? {
            let mut slots = [0; SLOT_SIZE];
            for (slot, key) in info.keys().iter().enumerate() {
                slots[slot] = key_values
                    .get(key)?
                    .ok_or(StorageError::ConsistencyCheckFailure)?
                    .version;
            }
            if info.keys().len() == KEY_SLOT_SIZE {
                if let Some(child) = amt_nodes.get(&amt_id.child(leaf))? {
                    slots[SLOT_SIZE - 1] = child.version;
                }
            }
            out.push((leaf, slots));
        }
        Ok(out)
    }

    /// The anchor of `item`, written at `written_at`, for a proof at `commit`.
    fn anchor(&self, commit: CommitID, written_at: CommitID, item: &H256) -> Result<Anchor> {
        let merkle = self.prove_auth_change(written_at, item)?;
        let at = if written_at == commit {
            None
        } else {
            Some((
                self.state_root_parts(written_at)?,
                self.prove_hroot_leaf(commit, written_at)?,
            ))
        };
        Ok(Anchor { commit: at, merkle })
    }

    /// The path of `item` in the auth-change tree of `commit`.
    pub fn prove_auth_change(&self, commit: CommitID, item: &H256) -> Result<AuthChangeProof> {
        let missing = || {
            StorageError::ProofUnavailable(format!(
                "item {item:?} is not in the auth-change tree of {commit:?}"
            ))
        };
        let mut path = vec![];
        let mut key = AuthChangeKey::root();
        loop {
            let node = self
                .get_auth_change_node(commit, &key)?
                .ok_or_else(missing)?;
            let position = match node.child_for(item) {
                Some(child) => child,
                None => node
                    .hashes()
                    .iter()
                    .position(|h| h == item)
                    .ok_or_else(missing)?,
            };
            path.push((node.hashes().to_vec(), position as u8));
            if node.is_leaf() {
                return Ok(AuthChangeProof { path });
            }
            key = key.child(position);
        }
    }
}

// Byte format. Integers are big-endian; every G1 point is compressed (48 bytes)
// and checked (on the curve, in the subgroup) when decoded, as the bytes come from
// an untrusted prover.
//
// proof       = 0x01 parts u8:n_layers layer*
// parts       = commitment h256:auth_change_root h256:hroot
// commitment  = u64:version g1
// layer       = u64[6]:slots amt_proof u8:n_keys key* u8:has_child [commitment anchor]
// amt_proof   = u8:n_levels (g1:commitment g1:quotient)*
// key         = u32:len bytes u32:len lvmt_value anchor
// anchor      = u8:has_commit [parts mmr] merkle
// mmr         = u64:leaf_count u64:leaf_index u8:n h256* u8:n h256*
// merkle      = u8:n_nodes (u8:n_entries h256* u8:position)*
mod bytes {
    use super::*;
    use crate::{
        backends::serde::{Decode, Encode},
        errors::DecodeError,
        lvmt::{crypto::G1Aff, types::CurvePoint},
    };
    use ark_serialize::{CanonicalDeserialize, CanonicalSerialize};

    const FORMAT: u8 = 1;
    type DResult<T> = std::result::Result<T, DecodeError>;

    #[derive(Default)]
    pub(super) struct Writer(pub Vec<u8>);

    impl Writer {
        fn u8(&mut self, v: u8) {
            self.0.push(v);
        }
        fn u32(&mut self, v: u32) {
            self.0.extend_from_slice(&v.to_be_bytes());
        }
        fn u64(&mut self, v: u64) {
            self.0.extend_from_slice(&v.to_be_bytes());
        }
        fn len(&mut self, n: usize) {
            self.u8(u8::try_from(n).expect("short list"));
        }
        fn bytes(&mut self, b: &[u8]) {
            self.u32(b.len() as u32);
            self.0.extend_from_slice(b);
        }
        fn h256(&mut self, h: &H256) {
            self.0.extend_from_slice(h.as_bytes());
        }
        fn hashes(&mut self, hs: &[H256]) {
            self.len(hs.len());
            hs.iter().for_each(|h| self.h256(h));
        }
        fn g1(&mut self, p: &G1Aff) {
            p.serialize_compressed(&mut self.0).unwrap();
        }
        fn commitment(&mut self, c: &CurvePointWithVersion) {
            self.u64(c.version);
            self.g1(&c.point.affine());
        }
        fn parts(&mut self, p: &StateRootParts) {
            self.commitment(&p.root_commitment);
            self.h256(&p.auth_change_root);
            self.h256(&p.hroot);
        }
        fn anchor(&mut self, a: &Anchor) {
            match &a.commit {
                None => self.u8(0),
                Some((parts, mmr)) => {
                    self.u8(1);
                    self.parts(parts);
                    self.u64(mmr.leaf_count);
                    self.u64(mmr.leaf_index);
                    self.hashes(&mmr.siblings);
                    self.hashes(&mmr.peaks);
                }
            }
            self.len(a.merkle.path.len());
            for (entries, position) in &a.merkle.path {
                self.hashes(entries);
                self.u8(*position);
            }
        }
        pub(super) fn proof(&mut self, p: &LvmtProof) {
            self.u8(FORMAT);
            self.parts(&p.commit);
            self.len(p.layers.len());
            for layer in &p.layers {
                layer.slots.iter().for_each(|s| self.u64(*s));
                self.len(layer.amt_proof.len());
                for (commitment, quotient) in layer.amt_proof.iter() {
                    self.g1(commitment);
                    self.g1(quotient);
                }
                self.len(layer.keys.len());
                for key in &layer.keys {
                    self.bytes(&key.key);
                    self.bytes(&key.value.encode());
                    self.anchor(&key.anchor);
                }
                match &layer.child {
                    None => self.u8(0),
                    Some(child) => {
                        self.u8(1);
                        self.commitment(&child.commitment);
                        self.anchor(&child.anchor);
                    }
                }
            }
        }
    }

    pub(super) struct Reader<'a>(pub &'a [u8]);

    impl Reader<'_> {
        fn take(&mut self, n: usize) -> DResult<&[u8]> {
            if self.0.len() < n {
                return Err(DecodeError::IncorrectLength);
            }
            let (head, rest) = self.0.split_at(n);
            self.0 = rest;
            Ok(head)
        }
        fn u8(&mut self) -> DResult<u8> {
            Ok(self.take(1)?[0])
        }
        fn u32(&mut self) -> DResult<u32> {
            Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
        }
        fn u64(&mut self) -> DResult<u64> {
            Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
        }
        fn bytes(&mut self) -> DResult<&[u8]> {
            let n = self.u32()? as usize;
            self.take(n)
        }
        fn flag(&mut self) -> DResult<bool> {
            match self.u8()? {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(DecodeError::Custom("bad flag")),
            }
        }
        fn h256(&mut self) -> DResult<H256> {
            Ok(H256::from_slice(self.take(32)?))
        }
        fn hashes(&mut self) -> DResult<Vec<H256>> {
            let n = self.u8()?;
            (0..n).map(|_| self.h256()).collect()
        }
        fn g1(&mut self) -> DResult<G1Aff> {
            let mut raw = self.take(48)?;
            Ok(G1Aff::deserialize_compressed(&mut raw)?)
        }
        fn commitment(&mut self) -> DResult<CurvePointWithVersion> {
            let version = self.u64()?;
            Ok(CurvePointWithVersion {
                version,
                point: CurvePoint::Affine(self.g1()?),
            })
        }
        fn parts(&mut self) -> DResult<StateRootParts> {
            Ok(StateRootParts {
                root_commitment: self.commitment()?,
                auth_change_root: self.h256()?,
                hroot: self.h256()?,
            })
        }
        fn anchor(&mut self) -> DResult<Anchor> {
            let commit = if self.flag()? {
                let parts = self.parts()?;
                let mmr = MmrProof {
                    leaf_count: self.u64()?,
                    leaf_index: self.u64()?,
                    siblings: self.hashes()?,
                    peaks: self.hashes()?,
                };
                Some((parts, mmr))
            } else {
                None
            };
            let n = self.u8()?;
            let path = (0..n)
                .map(|_| Ok((self.hashes()?, self.u8()?)))
                .collect::<DResult<_>>()?;
            Ok(Anchor {
                commit,
                merkle: AuthChangeProof { path },
            })
        }
        pub(super) fn proof(&mut self) -> DResult<LvmtProof> {
            if self.u8()? != FORMAT {
                return Err(DecodeError::Custom("unknown proof format"));
            }
            let commit = self.parts()?;
            let n_layers = self.u8()?;
            let mut layers = vec![];
            for _ in 0..n_layers {
                let mut slots = [0; SLOT_SIZE];
                for s in &mut slots {
                    *s = self.u64()?;
                }
                let n = self.u8()?;
                let levels = (0..n)
                    .map(|_| Ok((self.g1()?, self.g1()?)))
                    .collect::<DResult<_>>()?;
                let n_keys = self.u8()?;
                let mut keys = vec![];
                for _ in 0..n_keys {
                    let key: Box<[u8]> = self.bytes()?.into();
                    let value = LvmtValue::decode(self.bytes()?)?.into_owned();
                    keys.push(AnchoredKey {
                        key,
                        value,
                        anchor: self.anchor()?,
                    });
                }
                let child = if self.flag()? {
                    Some(AnchoredAmt {
                        commitment: self.commitment()?,
                        anchor: self.anchor()?,
                    })
                } else {
                    None
                };
                layers.push(LayerProof {
                    slots,
                    amt_proof: Proof::new(levels),
                    keys,
                    child,
                });
            }
            if !self.0.is_empty() {
                return Err(DecodeError::Custom("trailing bytes"));
            }
            Ok(LvmtProof { commit, layers })
        }
    }
}

impl LvmtProof {
    /// The proof in the byte format above.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut writer = bytes::Writer::default();
        writer.proof(self);
        writer.0
    }

    /// Decodes [`Self::to_bytes`], checking every curve point.
    pub fn from_bytes(input: &[u8]) -> std::result::Result<Self, crate::errors::DecodeError> {
        bytes::Reader(input).proof()
    }
}
