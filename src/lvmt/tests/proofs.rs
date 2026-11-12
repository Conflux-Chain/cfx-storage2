//! Value proofs: present, deleted and absent keys at depths 0–3, at pending and
//! historical commits, and tampering.

use std::{collections::HashSet, sync::Arc};

use ethereum_types::H256;
use rand::{rngs::StdRng, Rng, SeedableRng};

use super::{gen_novel_commit_id, u64_to_boxed_u8, AMT};
use crate::{
    backends::{impls::kvdb_rocksdb::WrappedRocksDb, HistoricalTableName, PendingTableName},
    lvmt::{
        checkpoint::ProofConfig,
        example::LvmtStorage,
        proof::{verify_lvmt_proof, LvmtProof, LvmtProofError},
        types::{LvmtValue, TEST_DIGEST_PREFIX},
    },
    middlewares::{clear_dir, clear_dir_then_create, get_rng_for_test, CommitID},
    StorageError,
};

type Db = LvmtStorage<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>;

/// A key with the given digest: `words` are its first 16-bit words, the rest
/// comes from `tail`.
fn crafted_key(words: &[u16], tail: u64) -> Box<[u8]> {
    let mut digest = [0u8; 32];
    for (i, w) in words.iter().enumerate() {
        digest[2 * i..2 * i + 2].copy_from_slice(&w.to_be_bytes());
    }
    digest[24..].copy_from_slice(&tail.to_be_bytes());
    [TEST_DIGEST_PREFIX, &digest[..]].concat().into()
}

fn check(db: &Db, commit: CommitID, key: &[u8]) -> (Option<LvmtValue>, LvmtProof) {
    let reader = db.as_reader().unwrap();
    let (value, proof) = reader.get_with_proof(commit, key, &AMT).unwrap();
    assert_eq!(value, reader.get(commit, key.into()).unwrap());
    let state_root = reader.get_state_root(commit).unwrap().unwrap();
    assert_eq!(
        verify_lvmt_proof(&AMT, &state_root, key, &proof),
        Ok(value.clone()),
        "key {key:?}"
    );
    let bytes = proof.to_bytes();
    assert_eq!(LvmtProof::from_bytes(&bytes).unwrap(), proof);
    (value, proof)
}

#[test]
fn test_lvmt_value_proofs() {
    let historical_path = "__test_lvmt_value_proofs_historical";
    let pending_path = "__test_lvmt_value_proofs_pending";
    clear_dir_then_create(historical_path);
    clear_dir_then_create(pending_path);
    let historical_db = Arc::new(WrappedRocksDb::open(historical_path).unwrap());
    let pending_db = Arc::new(WrappedRocksDb::open(pending_path).unwrap());
    let mut db = Db::new(historical_db, pending_db).unwrap();
    db.set_proof_config(ProofConfig {
        history_depth: 12,
        checkpoint_interval: 4,
    });

    // 12 keys share words 0..3 and 2 more only words 0..2, so all 14 compete for
    // the depth-1 leaf. Deep keys 0..3 and one side key fill it at commit 3; deep
    // keys 4..8 then fill the depth-2 leaf, 9..11 go to depth 3; the other side
    // key is at depth 2 in another leaf.
    let shared = [7u16, 9, 11];
    let deep: Vec<Box<[u8]>> = (0..12).map(|i| crafted_key(&shared, i)).collect();
    let side: Vec<Box<[u8]>> = (0..2).map(|i| crafted_key(&[7, 9, 12], 100 + i)).collect();
    let plain: Vec<Box<[u8]>> = (0..300).map(u64_to_boxed_u8).collect();

    let mut rng = get_rng_for_test();
    let mut previous_commits = HashSet::new();
    let mut value_rng = StdRng::seed_from_u64(14);
    let mut chain: Vec<CommitID> = vec![];
    for c in 0..24usize {
        let mut changes: Vec<(Box<[u8]>, Option<Box<[u8]>>)> = (0..40)
            .map(|_| {
                let k = plain[value_rng.gen_range(0..plain.len())].clone();
                (k, Some(u64_to_boxed_u8(value_rng.gen())))
            })
            .collect();
        // The deep keys arrive one per commit, and are rewritten or deleted later.
        if c < deep.len() {
            changes.push((deep[c].clone(), Some(u64_to_boxed_u8(c as u64))));
        }
        if c == 3 || c == 13 {
            changes.extend(side.iter().map(|k| (k.clone(), Some(u64_to_boxed_u8(1)))));
        }
        if c == 15 {
            changes.push((deep[2].clone(), None));
            changes.push((deep[11].clone(), Some(u64_to_boxed_u8(99))));
        }
        let id = gen_novel_commit_id(&mut rng, &mut previous_commits);
        db.as_manager()
            .unwrap()
            .commit(chain.last().copied(), id, changes.into_iter(), &AMT)
            .unwrap();
        chain.push(id);
    }
    db.confirmed_pending_to_history_with_commit_id(chain[18])
        .unwrap();

    let depth_of = |db: &Db, commit, key: &[u8]| {
        db.as_reader()
            .unwrap()
            .get(commit, key.into())
            .unwrap()
            .map(|v| v.allocation.depth)
    };
    let tip = *chain.last().unwrap();
    assert_eq!(depth_of(&db, tip, &deep[0]), Some(1));
    assert_eq!(depth_of(&db, tip, &deep[5]), Some(2));
    assert_eq!(depth_of(&db, tip, &deep[11]), Some(3));
    assert_eq!(depth_of(&db, tip, &side[1]), Some(2));

    let absent: Vec<Box<[u8]>> = vec![
        crafted_key(&shared, 1000),  // depth 3, free slot
        crafted_key(&[7, 9, 13], 0), // depth 2, empty leaf
        crafted_key(&[7, 10], 0),    // depth 1, empty leaf
        u64_to_boxed_u8(1_000_000),  // most likely an empty root leaf
        crafted_key(&[7, 9, 12], 0), // depth 2, leaf with free slots
    ];

    // Pending tip and middle, historical tip, older historical commits.
    for q in [23usize, 20, 17, 12, 8] {
        let commit = chain[q];
        for key in deep.iter().chain(&side).chain(&plain[..30]).chain(&absent) {
            let (value, proof) = check(&db, commit, key);
            let n = proof.layers.len();
            if let Some(v) = &value {
                assert_eq!(n, v.allocation.depth as usize + 1);
            }
        }
        // Deep keys not yet written at `q` are absent, deeper down.
        if q < deep.len() {
            let (value, proof) = check(&db, commit, &deep[q + 1..][0]);
            assert!(value.is_none());
            assert!(proof.layers.len() >= 2);
        }
    }

    let (deleted, _) = check(&db, tip, &deep[2]);
    assert_eq!(deleted.unwrap().value, None);
    let (absent_value, proof) = check(&db, tip, &absent[0]);
    assert!(absent_value.is_none());
    assert_eq!(proof.layers.len(), 4);
    assert_eq!(proof.layers[3].keys.len(), 3);

    // Beyond the history depth: tip at 17, K = 12.
    let reader = db.as_reader().unwrap();
    assert!(matches!(
        reader.get_with_proof(chain[4], &deep[0], &AMT),
        Err(StorageError::ProofUnavailable(_))
    ));
    drop(reader);

    // Tampering.
    let state_root = db
        .as_reader()
        .unwrap()
        .get_state_root(tip)
        .unwrap()
        .unwrap();
    let verify = |key: &[u8], proof: &LvmtProof| verify_lvmt_proof(&AMT, &state_root, key, proof);
    let (_, proof) = check(&db, tip, &deep[11]);
    assert!(verify(&deep[10], &proof).is_err());
    assert!(verify(&absent[0], &proof).is_err());

    let mut p = proof.clone();
    p.layers[3].keys[0].value.value = Some(u64_to_boxed_u8(98));
    assert_eq!(verify(&deep[11], &p), Err(LvmtProofError::Anchor(3)));

    let mut p = proof.clone();
    p.layers[3].keys[0].value.version += 1;
    assert_eq!(verify(&deep[11], &p), Err(LvmtProofError::KeyPosition(3)));

    // An allocation deeper than any AMT id is rejected, not a panic.
    let mut p = proof.clone();
    p.layers[3].keys[0].value.allocation.depth = 31;
    assert_eq!(verify(&deep[11], &p), Err(LvmtProofError::KeyPosition(3)));

    let mut p = proof.clone();
    p.layers[1].slots[4] += 1;
    assert!(matches!(
        verify(&deep[11], &p),
        Err(LvmtProofError::Amt(1, _))
    ));

    let mut p = proof.clone();
    p.layers[2].child.as_mut().unwrap().anchor.merkle.path[0].0[0] = H256::repeat_byte(1);
    assert!(verify(&deep[11], &p).is_err());

    let mut p = proof.clone();
    p.commit.hroot = H256::repeat_byte(2);
    assert_eq!(verify(&deep[11], &p), Err(LvmtProofError::StateRoot));

    let mut p = proof.clone();
    p.layers.pop();
    assert!(verify(&deep[11], &p).is_err());

    // Truncated or corrupted bytes do not decode; a point off the curve is caught.
    let bytes = proof.to_bytes();
    assert!(LvmtProof::from_bytes(&bytes[..bytes.len() - 1]).is_err());
    let mut corrupted = bytes.clone();
    corrupted.push(0);
    assert!(LvmtProof::from_bytes(&corrupted).is_err());
    let mut corrupted = bytes.clone();
    corrupted[1 + 8 + 47] ^= 0x01; // last byte of the root commitment point
    assert!(match LvmtProof::from_bytes(&corrupted) {
        Err(_) => true,
        Ok(p) => verify(&deep[11], &p).is_err(),
    });

    // An absence proof cannot leave out a slot's key, nor stop early.
    let (_, proof) = check(&db, tip, &absent[0]);
    let mut p = proof.clone();
    p.layers[3].keys.pop();
    assert_eq!(
        verify(&absent[0], &p),
        Err(LvmtProofError::MissingSlotKey(3))
    );
    let mut p = proof.clone();
    p.layers[1].keys.clear();
    assert_eq!(
        verify(&absent[0], &p),
        Err(LvmtProofError::MissingSlotKey(3))
    );
    // A present key's proof does not prove a different key absent or present.
    let (_, proof) = check(&db, tip, &deep[0]);
    assert!(verify(&deep[1], &proof).is_err());

    drop(db);
    clear_dir(historical_path);
    clear_dir(pending_path);
}
