//! Microbenchmark of the reads behind a root-AMT proof at a historical version Q
//! (hroot design draft, section 5). Two ways to get the root AMT at Q:
//!
//! - **full read**: read the value of all 65536 root leaves at Q (each is the
//!   version of a depth-1 AMT, in slot 5), then compute the path from scratch;
//! - **checkpoint derive**: from a checkpoint S commits after Q, scan the AMT
//!   changes of those S commits and subtract them.
//!
//! Not run by default. It builds its own database under `BENCH_DIR`:
//!
//! ```text
//! BENCH_DIR=/some/dir cargo test -r --lib bench_root_amt_proof_reads -- --ignored --nocapture
//! ```
//!
//! Other knobs (env): `BENCH_KEYS` initial keys (default 1_000_000),
//! `BENCH_WRITES` writes per commit (1500), `BENCH_COMMITS` commits after the
//! initial load (2300), `BENCH_NEW_PERCENT` share of writes creating a new key (10).

use std::{collections::BTreeMap, time::Instant};

use rand::{rngs::StdRng, Rng, SeedableRng};

use super::*;
use crate::lvmt::{
    crypto::Fr,
    types::{AmtId, SLOT_SIZE},
};

fn env_or(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |v| v.parse().unwrap())
}

/// Leaf value of the root AMT: only slot 5 (the child AMT version) is used.
fn root_leaf_value(version: u64) -> Fr {
    let mut slots = [0u64; SLOT_SIZE];
    slots[SLOT_SIZE - 1] = version;
    amt::pack_slots(&slots).unwrap()
}

fn ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1e3
}

#[test]
#[ignore]
fn bench_root_amt_proof_reads() {
    let dir = std::env::var("BENCH_DIR").expect("set BENCH_DIR");
    let keys = env_or("BENCH_KEYS", 1_000_000) as u64;
    let writes = env_or("BENCH_WRITES", 1500);
    let commits = env_or("BENCH_COMMITS", 2300);
    let new_percent = env_or("BENCH_NEW_PERCENT", 10);

    let historical_path = format!("{dir}/historical");
    let pending_path = format!("{dir}/pending");
    clear_dir_then_create(&historical_path);
    clear_dir_then_create(&pending_path);
    let historical_db = Arc::new(WrappedRocksDb::open(&historical_path).unwrap());
    let pending_db = Arc::new(WrappedRocksDb::open(&pending_path).unwrap());
    let mut db =
        LvmtStorage::<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>::new(
            historical_db,
            pending_db,
        )
        .unwrap();

    // ---- Build ----
    let mut rng = get_rng_for_test();
    let mut previous_commits = HashSet::new();
    let mut chain: Vec<CommitID> = Vec::new();
    let mut confirmed_upto = 0; // chain[..confirmed_upto] is in the historical part
    let mut next_key = 0u64;

    let build_start = Instant::now();
    let mut commit = |db: &mut LvmtStorage<_, _>,
                      chain: &mut Vec<CommitID>,
                      changes: Vec<(Box<[u8]>, Option<Box<[u8]>>)>| {
        let id = gen_novel_commit_id(&mut rng, &mut previous_commits);
        let mut lvmt = db.as_manager().unwrap();
        lvmt.commit(chain.last().copied(), id, changes.into_iter(), &AMT)
            .unwrap();
        chain.push(id);
    };

    while next_key < keys {
        let end = (next_key + 50_000).min(keys);
        let changes = (next_key..end)
            .map(|k| (u64_to_boxed_u8(k), Some(u64_to_boxed_u8(k))))
            .collect();
        commit(&mut db, &mut chain, changes);
        next_key = end;
    }
    let load_commits = chain.len();
    eprintln!(
        "initial load: {keys} keys in {load_commits} commits, {:.1} s",
        build_start.elapsed().as_secs_f64()
    );

    let mut value_rng = StdRng::seed_from_u64(7);
    for i in 0..commits {
        let changes = (0..writes)
            .map(|_| {
                let k = if value_rng.gen_range(0..100) < new_percent {
                    next_key += 1;
                    next_key - 1
                } else {
                    value_rng.gen_range(0..next_key)
                };
                (u64_to_boxed_u8(k), Some(u64_to_boxed_u8(value_rng.gen())))
            })
            .collect();
        commit(&mut db, &mut chain, changes);

        // Confirm every 100 commits, keeping the last 10 pending.
        if (i + 1) % 100 == 0 {
            confirmed_upto = chain.len() - 10;
            db.confirmed_pending_to_history_with_commit_id(chain[confirmed_upto])
                .unwrap();
        }
    }
    eprintln!(
        "build: {} commits, {next_key} keys, {:.1} s",
        chain.len(),
        build_start.elapsed().as_secs_f64()
    );

    // ---- Measure ----
    let lvmt = db.as_manager().unwrap();
    let tip = confirmed_upto - 1; // latest historical commit
    let amt_nodes = lvmt.get_amt_node_store();

    // Root leaf versions at chain[q], by point lookups and by a range scan.
    let read_point = |q: usize| -> Vec<u64> {
        let view = amt_nodes.get_versioned_store(&chain[q], false).unwrap();
        (0..=u16::MAX)
            .map(|i| {
                view.get(&AmtId::root().child(i))
                    .unwrap()
                    .map_or(0, |p| p.version)
            })
            .collect()
    };
    let read_range = |q: usize| -> Vec<u64> {
        let view = amt_nodes.get_versioned_store(&chain[q], false).unwrap();
        let mut versions = vec![0u64; 1 << 16];
        for (id, p) in view.iter_range(AmtId::root().child(0), None).unwrap() {
            if id.len() == 1 {
                versions[id[0] as usize] = p.version;
            }
        }
        versions
    };
    // Per-leaf count of child-AMT updates over chain[q+1..=q+s].
    let scan_changes = |q: usize, s: usize| -> BTreeMap<u16, u64> {
        let mut counts = BTreeMap::new();
        for c in &chain[q + 1..=q + s] {
            for (id, _) in lvmt.get_historical_amt_changes(*c).unwrap() {
                if id.len() == 1 {
                    *counts.entry(id[0]).or_insert(0) += 1;
                }
            }
        }
        counts
    };

    let proof_idx = 12345usize;
    eprintln!("\nhistorical tip = chain[{tip}]; leaf for proofs = {proof_idx}");
    eprintln!("distance | round | point gets (ms) | range scan (ms) | prove_sparse (ms)");
    for d in [0usize, 100, 1000, 2000] {
        if d > tip - load_commits {
            continue;
        }
        let q = tip - d;
        for round in 1..=2 {
            let t = Instant::now();
            let by_point = read_point(q);
            let point_ms = ms(t);

            let t = Instant::now();
            let by_range = read_range(q);
            let range_ms = ms(t);
            assert_eq!(by_point, by_range);

            let values: Vec<(usize, Fr)> = by_point
                .iter()
                .enumerate()
                .filter(|(_, v)| **v > 0)
                .map(|(i, v)| (i, root_leaf_value(*v)))
                .collect();
            let t = Instant::now();
            let proof = AMT.prove_sparse(values.iter().copied(), proof_idx);
            let prove_ms = ms(t);

            // Sanity: the proof verifies against the stored root commitment.
            let root = lvmt.get_root_commitment(chain[q]).unwrap();
            AMT.verify(
                &root.point.affine(),
                proof_idx,
                root_leaf_value(by_point[proof_idx]),
                &proof,
            )
            .unwrap();

            eprintln!(
                "{d:>8} | {round:>5} | {point_ms:>15.1} | {range_ms:>15.1} | {prove_ms:>17.1}   ({} non-zero leaves)",
                values.len()
            );
        }
    }

    eprintln!("\nS | Q | round | scan S commits (ms) | touched leaves | derive diffs (ms)");
    for s in [50usize, 100, 200] {
        let q = tip - s; // checkpoint at the historical tip
        for round in 1..=2 {
            let t = Instant::now();
            let counts = scan_changes(q, s);
            let scan_ms = ms(t);

            // Subtract the changes, path siblings only. Diffs are computed for the
            // positive counts and negated afterwards: negated field elements are
            // full-width scalars, which makes the MSMs much slower.
            let deltas: Vec<(usize, Fr)> = counts
                .iter()
                .map(|(i, c)| (*i as usize, root_leaf_value(*c)))
                .collect();
            let t = Instant::now();
            let diffs: Vec<_> = AMT
                .proof_tree_diffs_weighted(deltas, AMT.path_siblings(proof_idx))
                .into_iter()
                .map(|d| (d.level, d.index, -d.commitment, -d.proof))
                .collect();
            let derive_ms = ms(t);
            assert!(diffs.len() <= TEST_LEVEL);

            // Sanity: version at the checkpoint minus the counts = version at Q.
            if round == 1 {
                let at_q = read_point(q);
                let at_checkpoint = read_point(tip);
                for i in 0..1 << 16 {
                    let c = counts.get(&(i as u16)).copied().unwrap_or(0);
                    assert_eq!(at_checkpoint[i] - c, at_q[i]);
                }
            }

            eprintln!(
                "{s:>3} | chain[{q}] | {round:>5} | {scan_ms:>19.1} | {:>14} | {derive_ms:>17.1}",
                counts.len()
            );
        }
    }

    drop(lvmt);
    drop(db);
    clear_dir(&historical_path);
    clear_dir(&pending_path);
}
