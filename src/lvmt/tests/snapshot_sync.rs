//! The snapshot sync of an LVMT commit (`lvmt::snapshot_export`,
//! `lvmt::snapshot_sync`): export a pending or historical commit from one store,
//! check that the chunks carry every item of the commit with everything its
//! anchor needs, import them in another store, and the errors on the way.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use rand_chacha::ChaChaRng;

use crate::{
    backends::{HistoricalTableName, PendingTableName, WrappedInMemoryDb},
    errors::{Result, SnapshotSyncError},
    lvmt::{
        auth_changes::{amt_change_hash, key_value_hash},
        mmr::{bag_peaks, MmrProof},
        snapshot_sync::{Applied, ChunkId, Manifest, ManifestHeader, SnapshotChunk},
        types::{key_digest, AmtId, AuthChangeKey, AuthChangeNode},
    },
    middlewares::{get_rng_for_test, CommitID},
    StorageError,
};

use super::{gen_novel_commit_id, u64_to_boxed_u8, LvmtStorage, AMT};

type Db = LvmtStorage<WrappedInMemoryDb<HistoricalTableName>, WrappedInMemoryDb<PendingTableName>>;

const MAX_CHUNK: usize = 1 << 20;
const MAX_MANIFEST: usize = 1 << 22;

fn new_db() -> Db {
    Db::new(
        Arc::new(WrappedInMemoryDb::empty()),
        Arc::new(WrappedInMemoryDb::empty()),
    )
    .unwrap()
}

struct Chain {
    rng: ChaChaRng,
    previous: HashSet<CommitID>,
    next: u64,
}

impl Chain {
    fn new() -> Self {
        Self {
            rng: get_rng_for_test(),
            previous: HashSet::new(),
            next: 0,
        }
    }

    fn commit_changes(
        &mut self,
        db: &mut Db,
        parent: Option<CommitID>,
        changes: Vec<(Box<[u8]>, Option<Box<[u8]>>)>,
    ) -> CommitID {
        let id = gen_novel_commit_id(&mut self.rng, &mut self.previous);
        db.as_manager()
            .unwrap()
            .commit(parent, id, changes.into_iter(), &AMT)
            .unwrap();
        id
    }

    /// A commit on `parent` writing 5 new keys, overwriting one key of the
    /// previous commit and deleting one key of the commit before.
    fn commit(&mut self, db: &mut Db, parent: Option<CommitID>) -> CommitID {
        self.next += 1;
        let n = self.next;
        let mut changes: Vec<_> = (0..5)
            .map(|i| (u64_to_boxed_u8(n * 10 + i), Some(u64_to_boxed_u8(i))))
            .collect();
        if n > 1 {
            changes.push((u64_to_boxed_u8((n - 1) * 10), Some(u64_to_boxed_u8(n))));
        }
        if n > 2 {
            changes.push((u64_to_boxed_u8((n - 2) * 10 + 1), None));
        }
        self.commit_changes(db, parent, changes)
    }

    fn chain(&mut self, db: &mut Db, parent: Option<CommitID>, n: usize) -> Vec<CommitID> {
        let mut out: Vec<CommitID> = vec![];
        for _ in 0..n {
            let p = out.last().copied().or(parent);
            out.push(self.commit(db, p));
        }
        out
    }
}

fn sync_err<T>(result: Result<T>) -> SnapshotSyncError {
    match result {
        Err(StorageError::SnapshotSync(e)) => e,
        Err(e) => panic!("not a snapshot sync error: {e:?}"),
        Ok(_) => panic!("expected an error"),
    }
}

/// The manifest and every chunk of the snapshot of `n`, chunks of at most
/// `budget` bytes, checked by [`check_snapshot`].
fn export_with(sender: &Db, n: CommitID, budget: usize) -> (Vec<u8>, Vec<Vec<u8>>) {
    let reader = sender.as_reader().unwrap();
    let export = reader.prepare_snapshot(n, budget).unwrap();
    let manifest_bytes = export.manifest_bytes(MAX_MANIFEST).unwrap();
    let manifest = Manifest::from_bytes(&manifest_bytes).unwrap();
    assert_eq!(manifest.to_bytes(), manifest_bytes);
    assert_eq!(&manifest, export.manifest());
    assert_eq!(export.header(), manifest.header);
    let chunks: Vec<_> = manifest
        .chunk_id_bytes()
        .iter()
        .map(|id| {
            let chunk = export.load_chunk(&reader, id, 2 * budget).unwrap();
            assert!(chunk.len() <= budget, "{} > {budget}", chunk.len());
            assert_eq!(
                &SnapshotChunk::from_bytes(&chunk).unwrap().id().to_bytes(),
                id
            );
            chunk
        })
        .collect();
    check_snapshot(sender, n, &manifest_bytes, &chunks);
    (manifest_bytes, chunks)
}

fn export(sender: &Db, n: CommitID) -> (Vec<u8>, Vec<Vec<u8>>) {
    export_with(sender, n, MAX_CHUNK)
}

/// Checks a snapshot of `n` against the sender's own reads, the way a verifier
/// would use it: the manifest against the state root of `n`; every anchor part
/// closed under ancestors and hashing up to its commit's auth-change root; the
/// data chunks holding exactly the keys and non-root AMTs of `n`, each in the
/// chunk of its `w0`, and found in the received nodes of its anchor commit,
/// which is the commit that last wrote it; the MMR nodes proving every anchor
/// commit's state root in the `hroot` of `n`.
fn check_snapshot(sender: &Db, n: CommitID, manifest_bytes: &[u8], chunks: &[Vec<u8>]) {
    let reader = sender.as_reader().unwrap();
    let manifest = Manifest::from_bytes(manifest_bytes).unwrap();
    let header = ManifestHeader::from_manifest_bytes(manifest_bytes).unwrap();
    assert_eq!(manifest.header, header);
    assert_eq!(header.commit_id, n);
    assert_eq!(header.height, reader.commit_height(n).unwrap());
    assert_eq!(Some(header.state_root), reader.get_state_root(n).unwrap());
    let parts = &manifest.state_root_parts;
    assert_eq!(parts, &reader.state_root_parts(n).unwrap());
    assert_eq!(parts.state_root(), header.state_root);
    assert_eq!(
        bag_peaks(header.height, &manifest.parent_mmr_peaks),
        parts.hroot
    );
    assert_eq!(
        manifest.root_leaf_versions,
        reader.get_root_leaf_versions(n).unwrap()
    );

    // Anchor commits: ordered by height, with their parts, proven in the hroot.
    let mmr_nodes: BTreeMap<_, _> = manifest.mmr_nodes.iter().copied().collect();
    let mut previous_height = None;
    for (c, height, c_parts) in &manifest.anchor_commits {
        assert!(previous_height < Some(*height));
        previous_height = Some(*height);
        assert_eq!(*height, reader.commit_height(*c).unwrap());
        assert_eq!(c_parts, &reader.state_root_parts(*c).unwrap());
        if *c == n {
            continue;
        }
        let siblings = MmrProof::sibling_positions(header.height, *height)
            .unwrap()
            .iter()
            .map(|position| mmr_nodes[position])
            .collect();
        let proof = MmrProof {
            leaf_count: header.height,
            leaf_index: *height,
            siblings,
            peaks: manifest.parent_mmr_peaks.clone(),
        };
        assert!(proof.verify(&parts.hroot, &c_parts.state_root()));
    }

    // Chunks.
    let mut trees: Vec<HashMap<AuthChangeKey, AuthChangeNode>> =
        vec![HashMap::new(); manifest.anchor_commits.len()];
    let mut keys = BTreeMap::new();
    let mut amts = BTreeMap::new();
    let ids: Vec<ChunkId> = chunks
        .iter()
        .map(|chunk| SnapshotChunk::from_bytes(chunk).unwrap().id())
        .collect();
    assert_eq!(ids, manifest.chunks);
    let first_data = ids
        .iter()
        .position(|id| matches!(id, ChunkId::Data { .. }))
        .unwrap();
    assert!(ids[first_data..]
        .iter()
        .all(|id| matches!(id, ChunkId::Data { .. })));
    let mut next_w0 = 0u32;
    for chunk in chunks {
        match SnapshotChunk::from_bytes(chunk).unwrap() {
            SnapshotChunk::Anchor {
                commit_index,
                nodes,
                ..
            } => {
                let root = manifest.anchor_commits[commit_index as usize]
                    .2
                    .auth_change_root;
                let part: HashMap<_, _> = nodes.into_iter().collect();
                assert_eq!(part[&AuthChangeKey::root()].hash(), root);
                for (key, node) in &part {
                    if let Some((parent, position)) = key.parent() {
                        assert_eq!(part[&parent].hashes()[position], node.hash());
                    }
                }
                trees[commit_index as usize].extend(part);
            }
            SnapshotChunk::Data {
                w0_start,
                w0_end,
                flat_kv_pairs,
                amt_versions,
            } => {
                assert_eq!(w0_start as u32, next_w0);
                next_w0 = w0_end;
                let in_range = |w0: u16| (w0_start as u32..w0_end).contains(&(w0 as u32));
                for (key, value, anchor) in flat_kv_pairs {
                    let digest = key_digest(&key);
                    assert!(in_range(u16::from_be_bytes([digest[0], digest[1]])));
                    assert!(keys.insert(key, (value, anchor)).is_none());
                }
                for (amt_id, version, anchor) in amt_versions {
                    assert!(in_range(amt_id[0]));
                    assert!(amts.insert(amt_id, (version, anchor)).is_none());
                }
            }
        }
    }
    assert_eq!(next_w0, 1 << 16);

    let in_tree = |anchor: u32, hash| {
        let tree = &trees[anchor as usize];
        let mut key = AuthChangeKey::root();
        loop {
            let node = &tree[&key];
            if node.is_leaf() {
                return node.hashes().contains(&hash);
            }
            key = key.child(node.child_for(&hash).unwrap());
        }
    };
    let state = reader.get_state(n, false).unwrap();
    let expected_keys: Vec<_> = state
        .iter_range(Box::from(&[][..]), None)
        .unwrap()
        .collect();
    assert_eq!(expected_keys.len(), keys.len());
    for (key, value) in expected_keys {
        let (got, anchor) = &keys[&key];
        assert_eq!(got, &value);
        let (c, _) = reader.get_key_last_modified(n, &key).unwrap().unwrap();
        assert_eq!(manifest.anchor_commits[*anchor as usize].0, c);
        assert!(in_tree(*anchor, key_value_hash(&key, &value)));
    }
    let amt_view = reader
        .amt_node_reader()
        .get_versioned_store(&n, false)
        .unwrap();
    let expected_amts: Vec<_> = amt_view
        .iter_range(AmtId::root().child(0), None)
        .unwrap()
        .collect();
    assert_eq!(expected_amts.len(), amts.len());
    for (amt_id, point) in expected_amts {
        let (version, anchor) = amts[&amt_id];
        assert_eq!(version, point.version);
        let (c, _) = reader.get_amt_last_modified(n, &amt_id).unwrap().unwrap();
        assert_eq!(manifest.anchor_commits[anchor as usize].0, c);
        assert!(in_tree(anchor, amt_change_hash(&amt_id, &point)));
    }
}

/// Imports a snapshot, applying every chunk twice.
fn import(receiver: &mut Db, manifest: &[u8], chunks: &[Vec<u8>]) -> CommitID {
    let mut session = receiver.begin_import(manifest).unwrap();
    assert_eq!(session.missing_chunks(), chunks.len());
    for chunk in chunks {
        assert_eq!(session.apply_chunk(chunk).unwrap(), Applied::Accepted);
        assert_eq!(session.apply_chunk(chunk).unwrap(), Applied::AlreadyApplied);
    }
    assert_eq!(session.missing_chunks(), 0);
    session.finalize(receiver).unwrap()
}

#[test]
fn test_lvmt_snapshot_sync_round_trip() {
    let mut sender = new_db();
    let mut chain = Chain::new();
    let main = chain.chain(&mut sender, None, 12);
    sender
        .confirmed_pending_to_history_with_commit_id(main[5])
        .unwrap();

    // Historical N.
    let (manifest, chunks) = export(&sender, main[4]);
    let mut receiver = new_db();
    assert_eq!(import(&mut receiver, &manifest, &chunks), main[4]);

    // Pending N, moving to the historical part between two exports.
    let n = main[9];
    let is_pending = |db: &Db| {
        let reader = db.as_reader().unwrap();
        reader.is_newer_than_pending_root(reader.commit_height(n).unwrap())
    };
    assert!(is_pending(&sender));
    let before = export(&sender, n);
    sender
        .confirmed_pending_to_history_with_commit_id(main[11])
        .unwrap();
    assert!(!is_pending(&sender));
    assert_eq!(export(&sender, n), before);
    let mut receiver = new_db();
    assert_eq!(import(&mut receiver, &before.0, &before.1), n);
}

#[test]
fn test_lvmt_snapshot_sync_chunks_by_budget() {
    // A large first commit, then small ones; small chunks.
    let mut sender = new_db();
    let mut chain = Chain::new();
    let load = (0..3000u64)
        .map(|i| (u64_to_boxed_u8(1_000_000 + i), Some(u64_to_boxed_u8(i))))
        .collect();
    let first = chain.commit_changes(&mut sender, None, load);
    let main = chain.chain(&mut sender, Some(first), 6);
    sender
        .confirmed_pending_to_history_with_commit_id(main[2])
        .unwrap();
    let n = main[4];

    let budget = 4096;
    let (manifest_bytes, chunks) = export_with(&sender, n, budget);
    let manifest = Manifest::from_bytes(&manifest_bytes).unwrap();
    let parts_of_first = manifest
        .chunks
        .iter()
        .filter(|id| {
            matches!(
                id,
                ChunkId::Anchor {
                    commit_index: 0,
                    ..
                }
            )
        })
        .count();
    assert_eq!(manifest.anchor_commits[0].0, first);
    assert!(parts_of_first > 1, "{parts_of_first}");
    let data_chunks = manifest
        .chunks
        .iter()
        .filter(|id| matches!(id, ChunkId::Data { .. }))
        .count();
    assert!(data_chunks > 1, "{data_chunks}");

    // The same plan and bytes on every call.
    assert_eq!(
        export_with(&sender, n, budget),
        (manifest_bytes.clone(), chunks.clone())
    );
    let mut receiver = new_db();
    assert_eq!(import(&mut receiver, &manifest_bytes, &chunks), n);
}

#[test]
fn test_lvmt_snapshot_sync_sender_errors() {
    let mut sender = new_db();
    let mut chain = Chain::new();
    let main = chain.chain(&mut sender, None, 8);
    let fork = chain.chain(&mut sender, Some(main[3]), 2);
    let n = main[6];
    let reader = sender.as_reader().unwrap();
    let export = reader.prepare_snapshot(n, MAX_CHUNK).unwrap();
    let ids = export.manifest().chunk_id_bytes();
    let data_id = ids.last().unwrap().clone();
    let fork_export = reader.prepare_snapshot(fork[1], MAX_CHUNK).unwrap();
    let fork_id = fork_export.manifest().chunk_id_bytes()[0].clone();
    fork_export
        .load_chunk(&reader, &fork_id, MAX_CHUNK)
        .unwrap();

    let unknown = CommitID::repeat_byte(0xee);
    assert_eq!(
        sync_err(reader.prepare_snapshot(unknown, MAX_CHUNK)),
        SnapshotSyncError::CommitNotFound(unknown)
    );
    assert!(matches!(
        sync_err(export.manifest_bytes(100)),
        SnapshotSyncError::ManifestTooLarge { max: 100, .. }
    ));
    assert!(matches!(
        sync_err(export.load_chunk(&reader, &data_id, 5)),
        SnapshotSyncError::ChunkTooLarge { max: 5, .. }
    ));
    let n_anchors = export.manifest().anchor_commits.len() as u32;
    for id in [
        ChunkId::Data {
            w0_start: 0,
            w0_end: 7,
        },
        ChunkId::Anchor {
            commit_index: n_anchors,
            part: 0,
        },
        ChunkId::Anchor {
            commit_index: 0,
            part: 1000,
        },
    ] {
        assert_eq!(
            sync_err(export.load_chunk(&reader, &id.to_bytes(), MAX_CHUNK)),
            SnapshotSyncError::UnknownChunk
        );
    }
    let mut bad_version = data_id.clone();
    bad_version[0] += 1;
    assert_eq!(
        sync_err(export.load_chunk(&reader, &bad_version, MAX_CHUNK)),
        SnapshotSyncError::UnsupportedFormatVersion(bad_version[0])
    );
    drop(reader);

    // A discarded commit can no longer be exported or served.
    assert!(sender.as_manager().unwrap().make_pivot(main[7]).unwrap());
    let reader = sender.as_reader().unwrap();
    assert_eq!(
        sync_err(reader.prepare_snapshot(fork[1], MAX_CHUNK)),
        SnapshotSyncError::CommitNotFound(fork[1])
    );
    assert_eq!(
        sync_err(fork_export.load_chunk(&reader, &fork_id, MAX_CHUNK)),
        SnapshotSyncError::CommitNotFound(fork[1])
    );
}

#[test]
fn test_lvmt_snapshot_sync_receiver_errors_and_deferral() {
    let mut sender = new_db();
    let mut chain = Chain::new();
    let main = chain.chain(&mut sender, None, 4);
    let n = main[3];
    let (manifest_bytes, chunks) = export(&sender, n);
    let mut receiver = new_db();

    // Format errors.
    let mut bad_version = manifest_bytes.clone();
    bad_version[0] = 0;
    assert_eq!(
        sync_err(ManifestHeader::from_manifest_bytes(&bad_version)),
        SnapshotSyncError::UnsupportedFormatVersion(0)
    );
    assert_eq!(
        sync_err(receiver.begin_import(&bad_version)),
        SnapshotSyncError::UnsupportedFormatVersion(0)
    );
    let truncated = &manifest_bytes[..manifest_bytes.len() - 1];
    assert_eq!(
        sync_err(receiver.begin_import(truncated)),
        SnapshotSyncError::Malformed("truncated")
    );
    let mut trailing = manifest_bytes.clone();
    trailing.push(0);
    assert_eq!(
        sync_err(receiver.begin_import(&trailing)),
        SnapshotSyncError::Malformed("trailing bytes")
    );
    assert_eq!(
        sync_err(receiver.begin_import(&chunks[0])),
        SnapshotSyncError::Malformed("kind")
    );

    // Unknown chunks and an incomplete import.
    let mut session = receiver.begin_import(&manifest_bytes).unwrap();
    let mut bad_chunk = chunks[0].clone();
    bad_chunk[0] = 9;
    assert_eq!(
        sync_err(session.apply_chunk(&bad_chunk)),
        SnapshotSyncError::UnsupportedFormatVersion(9)
    );
    let unlisted = SnapshotChunk::Data {
        w0_start: 1,
        w0_end: 2,
        flat_kv_pairs: vec![],
        amt_versions: vec![],
    };
    assert_eq!(
        sync_err(session.apply_chunk(&unlisted.to_bytes())),
        SnapshotSyncError::UnknownChunk
    );
    session.apply_chunk(&chunks[0]).unwrap();
    assert_eq!(
        sync_err(session.finalize(&mut receiver)),
        SnapshotSyncError::Incomplete(chunks.len() - 1)
    );

    // A data chunk waits for every anchor chunk of the anchor commits it uses.
    let reader = sender.as_reader().unwrap();
    let mut manifest = Manifest::from_bytes(&manifest_bytes).unwrap();
    let parts = manifest.state_root_parts.clone();
    manifest.anchor_commits = vec![(n, manifest.header.height, parts)];
    let anchor = |part| SnapshotChunk::Anchor {
        commit_index: 0,
        part,
        nodes: vec![(
            AuthChangeKey::root(),
            reader
                .get_auth_change_node(n, &AuthChangeKey::root())
                .unwrap()
                .unwrap(),
        )],
    };
    let key = u64_to_boxed_u8(41);
    let value = reader.get(n, key.clone()).unwrap().unwrap();
    let data = SnapshotChunk::Data {
        w0_start: 0,
        w0_end: 1 << 16,
        flat_kv_pairs: vec![(key, value, 0)],
        amt_versions: vec![(AmtId::root().child(7), 3, 0)],
    };
    for chunk in [anchor(0), anchor(1), data.clone()] {
        assert_eq!(SnapshotChunk::from_bytes(&chunk.to_bytes()).unwrap(), chunk);
    }
    manifest.chunks = vec![anchor(0).id(), anchor(1).id(), data.id()];
    let mut session = receiver.begin_import(&manifest.to_bytes()).unwrap();
    assert_eq!(
        session.apply_chunk(&data.to_bytes()).unwrap(),
        Applied::Deferred
    );
    assert_eq!(
        session.apply_chunk(&anchor(1).to_bytes()).unwrap(),
        Applied::Accepted
    );
    assert_eq!(
        session.apply_chunk(&data.to_bytes()).unwrap(),
        Applied::Deferred
    );
    assert_eq!(
        session.apply_chunk(&anchor(0).to_bytes()).unwrap(),
        Applied::Accepted
    );
    assert_eq!(
        session.apply_chunk(&data.to_bytes()).unwrap(),
        Applied::Accepted
    );
    assert_eq!(session.finalize(&mut receiver).unwrap(), n);

    // An anchor index out of range, and an anchor chunk of an unknown commit.
    let SnapshotChunk::Data {
        w0_start,
        w0_end,
        flat_kv_pairs,
        ..
    } = data
    else {
        unreachable!()
    };
    let out_of_range = SnapshotChunk::Data {
        w0_start,
        w0_end,
        flat_kv_pairs,
        amt_versions: vec![(AmtId::root().child(7), 3, 1)],
    };
    let mut session = receiver.begin_import(&manifest.to_bytes()).unwrap();
    assert_eq!(
        sync_err(session.apply_chunk(&out_of_range.to_bytes())),
        SnapshotSyncError::Malformed("anchor index out of range")
    );
    manifest.chunks.push(ChunkId::Anchor {
        commit_index: 1,
        part: 0,
    });
    assert_eq!(
        sync_err(receiver.begin_import(&manifest.to_bytes())),
        SnapshotSyncError::Malformed("anchor chunk of an unknown commit")
    );
}
