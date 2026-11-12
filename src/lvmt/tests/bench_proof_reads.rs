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
//! initial load (2300), `BENCH_NEW_PERCENT` share of writes creating a new key (10),
//! `BENCH_PENDING` commits left pending on top of the build (100, on top of the 10
//! kept pending by the last confirmation), `BENCH_KEEP` (set to keep the database
//! afterwards).
//!
//! Also measured: root-AMT reads at a pending commit, the computation on a single
//! thread, and the reads and proof of one depth-1 AMT.

use std::{collections::BTreeMap, time::Instant};

use rand::{rngs::StdRng, Rng, SeedableRng};

use super::*;
use crate::lvmt::{
    crypto::Fr,
    types::{AmtId, KEY_SLOT_SIZE, SLOT_SIZE},
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
    let pending_extra = env_or("BENCH_PENDING", 100);

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
    for i in 0..commits + pending_extra {
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
        if i < commits && (i + 1) % 100 == 0 {
            confirmed_upto = chain.len() - 10;
            db.confirmed_pending_to_history_with_commit_id(chain[confirmed_upto])
                .unwrap();
        }
    }
    eprintln!(
        "build: {} commits ({} pending), {next_key} keys, {:.1} s",
        chain.len(),
        chain.len() - confirmed_upto,
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
            assert_eq!(by_point, lvmt.get_root_leaf_versions(chain[q]).unwrap());

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

    // ---- Pending commits ----
    // Point reads of the root leaves at a pending commit Q. Without a checkout, each
    // read walks the pending tree from Q up to the commit currently checked out (here
    // the pending root) and falls back to the history tip; after checking out Q, a
    // read is one map lookup plus the fallback.
    let pending_root = confirmed_upto;
    let pending_tip = chain.len() - 1;
    let check_root_proof = |q: usize, versions: &[u64]| -> Vec<(usize, Fr)> {
        let values: Vec<(usize, Fr)> = versions
            .iter()
            .enumerate()
            .filter(|(_, v)| **v > 0)
            .map(|(i, v)| (i, root_leaf_value(*v)))
            .collect();
        let proof = AMT.prove_sparse(values.iter().copied(), proof_idx);
        let root = lvmt.get_root_commitment(chain[q]).unwrap();
        AMT.verify(
            &root.point.affine(),
            proof_idx,
            root_leaf_value(versions[proof_idx]),
            &proof,
        )
        .unwrap();
        values
    };
    eprintln!(
        "\npending: chain[{pending_root}..={pending_tip}] ({} commits)",
        pending_tip - pending_root + 1
    );
    eprintln!("Q | round | checkout (ms) | point gets (ms)");
    for (name, q) in [
        ("pending tip", pending_tip),
        ("pending middle", (pending_root + pending_tip) / 2),
    ] {
        for round in 1..=2 {
            // Walk: the checked-out commit is the pending root, an ancestor of Q.
            amt_nodes.checkout_current(chain[pending_root]).unwrap();
            let t = Instant::now();
            let walked = read_point(q);
            let walk_ms = ms(t);

            // Checkout from the pending root to Q, then read.
            let t = Instant::now();
            amt_nodes.checkout_current(chain[q]).unwrap();
            let checkout_ms = ms(t);
            let t = Instant::now();
            let checked_out = read_point(q);
            let read_ms = ms(t);
            assert_eq!(walked, checked_out);

            // History tip + the changes on the pending path, without a checkout.
            amt_nodes.checkout_current(chain[pending_root]).unwrap();
            let t = Instant::now();
            let overlaid = lvmt.get_root_leaf_versions(chain[q]).unwrap();
            let overlay_ms = ms(t);
            assert_eq!(walked, overlaid);
            if round == 1 {
                check_root_proof(q, &checked_out);
            }
            eprintln!("{name} chain[{q}] | {round} | - | {walk_ms:.1} (walk from Q)");
            eprintln!(
                "{name} chain[{q}] | {round} | - | {overlay_ms:.1} (history tip + pending path changes)"
            );
            eprintln!(
                "{name} chain[{q}] | {round} | {checkout_ms:.1} | {read_ms:.1} (after checkout)"
            );
        }
    }

    // ---- Single-thread computation ----
    let single = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();
    let tip_values = check_root_proof(tip, &read_point(tip));
    let deltas: Vec<(usize, Fr)> = scan_changes(tip - 100, 100)
        .iter()
        .map(|(i, c)| (*i as usize, root_leaf_value(*c)))
        .collect();
    let compute = || {
        let t = Instant::now();
        AMT.prove_sparse(tip_values.iter().copied(), proof_idx);
        let prove_ms = ms(t);
        let t = Instant::now();
        AMT.proof_tree_diffs_weighted(deltas.iter().copied(), AMT.path_siblings(proof_idx));
        (prove_ms, ms(t))
    };
    eprintln!("\nthreads | prove_sparse 65536 leaves (ms) | derive diffs S=100 (ms)");
    for _ in 0..2 {
        let (prove_ms, derive_ms) = compute();
        let threads = rayon::current_num_threads();
        eprintln!("{threads:>7} | {prove_ms:>31.1} | {derive_ms:>23.1}");
    }
    for _ in 0..2 {
        let (prove_ms, derive_ms) = single.install(compute);
        eprintln!("{:>7} | {prove_ms:>31.1} | {derive_ms:>23.1}", 1);
    }

    // ---- One depth-1 AMT ----
    // Leaf values of depth-1 AMT `amt` at chain[q]: the leaves come from the
    // SlotAllocations prefix scan (G1), slot i holds the version of the key in slot
    // i, and slot 5 the version of the child AMT (which can only exist once all key
    // slots of the leaf are allocated).
    let kv_store = lvmt.get_key_value_store();
    let amt = AmtId::root().child(proof_idx as u16);
    let read_level1 = |q: usize| -> (Vec<(usize, Fr)>, f64, f64, usize) {
        let t = Instant::now();
        let leaves = lvmt.get_amt_slot_keys(chain[q], &amt).unwrap();
        let scan_ms = ms(t);
        let t = Instant::now();
        let kv = kv_store.get_versioned_store(&chain[q], false).unwrap();
        let nodes = amt_nodes.get_versioned_store(&chain[q], false).unwrap();
        let mut n_keys = 0;
        let values = leaves
            .iter()
            .map(|(j, info)| {
                let mut slots = [0u64; SLOT_SIZE];
                for (s, key) in info.keys().iter().enumerate() {
                    slots[s] = kv.get(key).unwrap().unwrap().version;
                    n_keys += 1;
                }
                if info.keys().len() == KEY_SLOT_SIZE {
                    slots[SLOT_SIZE - 1] =
                        nodes.get(&amt.child(*j)).unwrap().map_or(0, |p| p.version);
                }
                (*j as usize, amt::pack_slots(&slots).unwrap())
            })
            .collect();
        (values, scan_ms, ms(t), n_keys)
    };
    eprintln!("\ndepth-1 AMT {amt:?}");
    eprintln!(
        "Q | round | leaves | keys | slot-key scan (ms) | version gets (ms) | prove_sparse (ms) | 1 thread (ms)"
    );
    amt_nodes.checkout_current(chain[pending_root]).unwrap();
    for (name, q) in [
        ("pending tip", pending_tip),
        ("tip", tip),
        ("tip-100", tip - 100),
        ("tip-2000", tip.saturating_sub(2000).max(load_commits)),
    ] {
        for round in 1..=2 {
            let (values, scan_ms, get_ms, n_keys) = read_level1(q);
            let leaf = values[values.len() / 2];
            let t = Instant::now();
            let proof = AMT.prove_sparse(values.iter().copied(), leaf.0);
            let prove_ms = ms(t);
            let t = Instant::now();
            single.install(|| AMT.prove_sparse(values.iter().copied(), leaf.0));
            let single_ms = ms(t);
            let commitment = amt_nodes
                .get_versioned_store(&chain[q], false)
                .unwrap()
                .get(&amt)
                .unwrap()
                .unwrap();
            AMT.verify(&commitment.point.affine(), leaf.0, leaf.1, &proof)
                .unwrap();
            eprintln!(
                "{name} chain[{q}] | {round} | {:>6} | {n_keys:>4} | {scan_ms:>18.1} | {get_ms:>17.1} | {prove_ms:>17.1} | {single_ms:>13.1}",
                values.len()
            );
        }
    }

    drop(lvmt);
    drop(db);
    if std::env::var("BENCH_KEEP").is_err() {
        clear_dir(&historical_path);
        clear_dir(&pending_path);
    }
}
