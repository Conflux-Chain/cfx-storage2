//! The call sequence and the formats of the snapshot sync skeleton
//! (`lvmt::snapshot_sync`): export a pending or historical commit from one store,
//! import it in another, and the errors on the way.

use std::{collections::HashSet, sync::Arc};

use rand_chacha::ChaChaRng;

use crate::{
    backends::{HistoricalTableName, PendingTableName, WrappedInMemoryDb},
    errors::{Result, SnapshotSyncError},
    lvmt::{
        mmr::bag_peaks,
        snapshot_sync::{Applied, ChunkId, Manifest, ManifestHeader, SnapshotChunk},
        types::{AmtId, AuthChangeKey},
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

    /// A commit on `parent` writing 5 new keys and overwriting one key of the
    /// previous commit.
    fn commit(&mut self, db: &mut Db, parent: Option<CommitID>) -> CommitID {
        self.next += 1;
        let n = self.next;
        let changes = (0..5)
            .map(|i| (u64_to_boxed_u8(n * 10 + i), Some(u64_to_boxed_u8(i))))
            .chain((n > 1).then(|| (u64_to_boxed_u8((n - 1) * 10), Some(u64_to_boxed_u8(n)))));
        let id = gen_novel_commit_id(&mut self.rng, &mut self.previous);
        db.as_manager()
            .unwrap()
            .commit(parent, id, changes, &AMT)
            .unwrap();
        id
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

/// The manifest and every chunk of the snapshot of `n`, checking the manifest
/// against the sender's own reads.
fn export(sender: &Db, n: CommitID) -> (Vec<u8>, Vec<Vec<u8>>) {
    let reader = sender.as_reader().unwrap();
    let manifest_bytes = reader.load_manifest(n, MAX_CHUNK, MAX_MANIFEST).unwrap();

    let header = ManifestHeader::from_manifest_bytes(&manifest_bytes).unwrap();
    assert_eq!(header.commit_id, n);
    assert_eq!(header.height, reader.commit_height(n).unwrap());
    assert_eq!(Some(header.state_root), reader.get_state_root(n).unwrap());

    let manifest = Manifest::from_bytes(&manifest_bytes).unwrap();
    assert_eq!(manifest.to_bytes(), manifest_bytes);
    assert_eq!(manifest.header, header);
    assert_eq!(
        manifest.state_root_parts,
        reader.state_root_parts(n).unwrap()
    );
    assert_eq!(manifest.state_root_parts.state_root(), header.state_root);
    assert_eq!(
        bag_peaks(header.height, &manifest.parent_mmr_peaks),
        manifest.state_root_parts.hroot
    );
    assert_eq!(
        manifest.root_leaf_versions,
        reader.get_root_leaf_versions(n).unwrap()
    );
    assert!(!manifest.chunks.is_empty());

    let chunks = manifest
        .chunk_id_bytes()
        .iter()
        .map(|id| {
            let chunk = reader.load_chunk(n, id, MAX_CHUNK).unwrap();
            assert_eq!(
                &SnapshotChunk::from_bytes(&chunk).unwrap().id().to_bytes(),
                id
            );
            chunk
        })
        .collect();
    (manifest_bytes, chunks)
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
fn test_lvmt_snapshot_sync_sender_errors() {
    let mut sender = new_db();
    let mut chain = Chain::new();
    let main = chain.chain(&mut sender, None, 8);
    let fork = chain.chain(&mut sender, Some(main[3]), 2);
    let n = main[6];
    let reader = sender.as_reader().unwrap();
    let manifest = reader.load_manifest(n, MAX_CHUNK, MAX_MANIFEST).unwrap();
    let data_id = Manifest::from_bytes(&manifest).unwrap().chunk_id_bytes()[0].clone();
    reader
        .load_manifest(fork[1], MAX_CHUNK, MAX_MANIFEST)
        .unwrap();

    let unknown = CommitID::repeat_byte(0xee);
    assert_eq!(
        sync_err(reader.load_manifest(unknown, MAX_CHUNK, MAX_MANIFEST)),
        SnapshotSyncError::CommitNotFound(unknown)
    );
    assert_eq!(
        sync_err(reader.load_chunk(unknown, &data_id, MAX_CHUNK)),
        SnapshotSyncError::CommitNotFound(unknown)
    );
    assert!(matches!(
        sync_err(reader.load_manifest(n, MAX_CHUNK, 100)),
        SnapshotSyncError::ManifestTooLarge { max: 100, .. }
    ));
    assert!(matches!(
        sync_err(reader.load_chunk(n, &data_id, 5)),
        SnapshotSyncError::ChunkTooLarge { max: 5, .. }
    ));
    for id in [
        ChunkId::Data {
            w0_start: 0,
            w0_end: 7,
        },
        ChunkId::Anchor {
            commit_index: 0,
            part: 0,
        },
    ] {
        assert_eq!(
            sync_err(reader.load_chunk(n, &id.to_bytes(), MAX_CHUNK)),
            SnapshotSyncError::UnknownChunk
        );
    }
    let mut bad_version = data_id.clone();
    bad_version[0] += 1;
    assert_eq!(
        sync_err(reader.load_chunk(n, &bad_version, MAX_CHUNK)),
        SnapshotSyncError::UnsupportedFormatVersion(bad_version[0])
    );
    drop(reader);

    // A discarded commit can no longer be exported.
    assert!(sender.as_manager().unwrap().make_pivot(main[7]).unwrap());
    let reader = sender.as_reader().unwrap();
    assert_eq!(
        sync_err(reader.load_manifest(fork[1], MAX_CHUNK, MAX_MANIFEST)),
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
