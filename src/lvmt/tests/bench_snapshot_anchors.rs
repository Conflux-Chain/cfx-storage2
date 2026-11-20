//! How much anchor data a verifiable snapshot of a commit `N` needs (snapshot
//! sync design discussion).
//!
//! The root commitment only commits to the versions of the depth-1 AMTs. The
//! value of a key and the commitment of a non-root AMT are shown by an anchor
//! (see [`crate::lvmt::proof`]): the item's path in the auth-change tree of the
//! commit `c` that last wrote it, and if `c ≠ N`, also `c`'s state root parts
//! and an MMR proof of `c`'s state root in `N`'s `hroot`. A snapshot of `N`
//! needs an anchor for every key and every non-root AMT. This counts, in the
//! byte format of [`crate::lvmt::proof`]:
//!
//! - **per item**: one auth-change path per item, as `get_with_proof` sends it;
//! - **per commit, shared nodes once**: the union of the path nodes of the
//!   items anchored in `c`, each node once;
//! - **per commit, whole tree**: every node of the auth-change tree of `c`;
//!
//! plus, for every `c ≠ N`, its state root parts and MMR proof (summed, and
//! with the MMR hashes shared across commits counted once), against the size of
//! the flat key-values and AMT versions themselves.
//!
//! Not run by default. It builds its own database under `BENCH_DIR`:
//!
//! ```text
//! BENCH_DIR=/some/dir cargo test -r --lib bench_snapshot_anchor_volume -- --ignored --nocapture
//! ```
//!
//! Other knobs (env): `BENCH_KEYS` initial keys (default 1_000_000), loaded in
//! commits of `BENCH_LOAD_BATCH` keys (50_000); then `BENCH_COMMITS` commits
//! (2300) of `BENCH_WRITES` writes (1500), `BENCH_NEW_PERCENT` of them creating
//! a new key (10). Every 100 of these commits are confirmed, keeping the last 10
//! pending. `N` is the latest historical commit. `BENCH_KEEP` keeps the database.
//! With the defaults scaled to 40M keys, this is the workload of
//! `bench_root_amt_proof_reads`.

use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

use ethereum_types::H256;
use rand::{rngs::StdRng, Rng, SeedableRng};

use super::*;
use crate::backends::serde::Encode;
use crate::lvmt::{
    auth_changes::{amt_change_hash, key_value_hash},
    types::{AmtId, AuthChangeKey, AuthChangeNode},
};

fn env_or(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |v| v.parse().unwrap())
}

/// A node in the byte format of a proof: `u8:n_entries h256*`.
fn node_bytes(node: &AuthChangeNode) -> u64 {
    1 + 32 * node.hashes().len() as u64
}

fn mb(bytes: u64) -> String {
    match bytes {
        b if b >= 1_000_000_000 => format!("{:.2} GB", b as f64 / 1e9),
        b if b >= 1_000_000 => format!("{:.1} MB", b as f64 / 1e6),
        b => format!("{:.1} KB", b as f64 / 1e3),
    }
}

#[test]
#[ignore]
fn bench_snapshot_anchor_volume() {
    let dir = std::env::var("BENCH_DIR").expect("set BENCH_DIR");
    let keys = env_or("BENCH_KEYS", 1_000_000) as u64;
    let load_batch = env_or("BENCH_LOAD_BATCH", 50_000) as u64;
    let writes = env_or("BENCH_WRITES", 1500);
    let commits = env_or("BENCH_COMMITS", 2300);
    let new_percent = env_or("BENCH_NEW_PERCENT", 10);

    let historical_path = format!("{dir}/historical");
    let pending_path = format!("{dir}/pending");
    clear_dir_then_create(&historical_path);
    clear_dir_then_create(&pending_path);
    let mut db =
        LvmtStorage::<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>::new(
            Arc::new(WrappedRocksDb::open(&historical_path).unwrap()),
            Arc::new(WrappedRocksDb::open(&pending_path).unwrap()),
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
        let end = (next_key + load_batch).min(keys);
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
    assert!(confirmed_upto > 0, "nothing confirmed: raise BENCH_COMMITS");
    eprintln!(
        "build: {} commits ({} pending), {next_key} keys, {:.1} s",
        chain.len(),
        chain.len() - confirmed_upto,
        build_start.elapsed().as_secs_f64()
    );

    // ---- Measure ----
    let lvmt = db.as_manager().unwrap();
    let n_index = confirmed_upto - 1; // latest historical commit; height = index
    let n = chain[n_index];
    let height_of: HashMap<CommitID, usize> =
        chain.iter().enumerate().map(|(i, c)| (*c, i)).collect();
    eprintln!("\nN = chain[{n_index}] (latest historical commit)");

    // Items of the snapshot, grouped by the commit that last wrote them.
    let t = Instant::now();
    let mut by_commit: HashMap<CommitID, Vec<H256>> = HashMap::new();
    let (mut n_keys, mut key_data) = (0u64, 0u64);
    let state = lvmt.get_state(n, false).unwrap();
    // Keys are 8 bytes; iterate them by first byte to bound the memory of a range.
    for first in 0..=u8::MAX {
        let upper = (first < u8::MAX).then(|| Box::<[u8]>::from(&[first + 1][..]));
        for (key, value) in state.iter_range(Box::from(&[first][..]), upper).unwrap() {
            let (c, last) = lvmt.get_key_last_modified(n, &key).unwrap().unwrap();
            assert_eq!(last, value);
            // key = u32:len bytes u32:len lvmt_value
            key_data += 8 + key.len() as u64 + value.encode().len() as u64;
            n_keys += 1;
            by_commit
                .entry(c)
                .or_default()
                .push(key_value_hash(&key, &value));
        }
    }
    drop(state);
    let (mut n_amts, mut amt_data) = (0u64, 0u64);
    let amt_view = lvmt
        .get_amt_node_store()
        .get_versioned_store(&n, false)
        .unwrap();
    for (id, point) in amt_view.iter_range(AmtId::root().child(0), None).unwrap() {
        let (c, last) = lvmt.get_amt_last_modified(n, &id).unwrap().unwrap();
        assert_eq!(last, point);
        // amt id (u16 words) and u64:version
        amt_data += 2 * id.len() as u64 + 8;
        n_amts += 1;
        by_commit
            .entry(c)
            .or_default()
            .push(amt_change_hash(&id, &point));
    }
    drop(amt_view);
    eprintln!(
        "items: {n_keys} keys, {n_amts} non-root AMTs, anchored in {} commits ({:.1} s)",
        by_commit.len(),
        t.elapsed().as_secs_f64()
    );

    // Anchors.
    let t = Instant::now();
    let reader = lvmt.reader();
    let (mut per_item, mut shared, mut whole) = (0u64, 0u64, 0u64);
    let (mut path_nodes, mut checked) = (0u64, 0u64);
    let (mut commit_parts, mut mmr_sum) = (0u64, 0u64);
    let mut mmr_hashes: HashSet<H256> = HashSet::new();
    let mut peaks_bytes = 0u64;
    // Items per anchor commit: [1, 10), [10, 100), ..., [100_000, ∞)
    let mut buckets = [(0u64, 0u64); 6];
    let (mut items_in_load, mut items_total) = (0u64, 0u64);
    let mut sample = 0u64;
    for (c, items) in &by_commit {
        // The whole auth-change tree of c.
        let mut nodes: HashMap<AuthChangeKey, AuthChangeNode> = HashMap::new();
        let mut stack = vec![AuthChangeKey::root()];
        while let Some(k) = stack.pop() {
            let node = lvmt.get_auth_change_node(*c, &k).unwrap().unwrap();
            if !node.is_leaf() {
                stack.extend((0..node.hashes().len()).map(|i| k.child(i)));
            }
            whole += node_bytes(&node);
            nodes.insert(k, node);
        }
        assert_eq!(
            nodes[&AuthChangeKey::root()].hash(),
            lvmt.get_auth_change_root(*c).unwrap().unwrap()
        );

        let mut used: HashSet<AuthChangeKey> = HashSet::new();
        for item in items {
            // merkle = u8:n_nodes (u8:n_entries h256* u8:position)*
            let mut bytes = 1u64;
            let mut depth = 0u64;
            let mut k = AuthChangeKey::root();
            loop {
                let node = &nodes[&k];
                bytes += node_bytes(node) + 1;
                depth += 1;
                used.insert(k);
                if node.is_leaf() {
                    assert!(node.hashes().contains(item));
                    break;
                }
                k = k.child(node.child_for(item).unwrap());
            }
            per_item += bytes;
            path_nodes += depth;
            // Check the counted path against the prover's on a sample.
            sample += 1;
            if sample % 100_003 == 1 {
                let proof = reader.prove_auth_change(*c, item).unwrap();
                assert_eq!(proof.path.len() as u64, depth);
                let proof_bytes: u64 = 1 + proof
                    .path
                    .iter()
                    .map(|(e, _)| 2 + 32 * e.len() as u64)
                    .sum::<u64>();
                assert_eq!(proof_bytes, bytes);
                assert_eq!(proof.root_for(item), lvmt.get_auth_change_root(*c).unwrap());
                checked += 1;
            }
        }
        shared += used.iter().map(|k| node_bytes(&nodes[k])).sum::<u64>();

        if *c != n {
            // parts = u64:version g1 h256 h256
            commit_parts += 8 + 48 + 32 + 32;
            let mmr = lvmt.prove_hroot_leaf(n, *c).unwrap();
            // mmr = u64 u64 u8:n h256* u8:n h256*
            mmr_sum += 8 + 8 + 1 + 32 * mmr.siblings.len() as u64 + 1 + 32 * mmr.peaks.len() as u64;
            mmr_hashes.extend(mmr.siblings.iter().copied());
            peaks_bytes = 32 * mmr.peaks.len() as u64;
        }

        let size = items.len() as u64;
        let bucket = ((size as f64).log10().floor() as usize).min(buckets.len() - 1);
        buckets[bucket].0 += 1;
        buckets[bucket].1 += size;
        items_total += size;
        if height_of[c] < load_commits {
            items_in_load += size;
        }
    }
    let mmr_shared = 32 * mmr_hashes.len() as u64 + peaks_bytes;
    eprintln!(
        "anchors counted in {:.1} s; {checked} sampled paths checked against prove_auth_change",
        t.elapsed().as_secs_f64()
    );

    // ---- Report ----
    let data = key_data + amt_data;
    let ratio = |b: u64| format!("{:.2}×", b as f64 / data as f64);
    eprintln!("\nsnapshot data:");
    eprintln!(
        "  flat key-values      {:>12}  ({n_keys} keys)",
        mb(key_data)
    );
    eprintln!(
        "  AMT ids + versions   {:>12}  ({n_amts} AMTs)",
        mb(amt_data)
    );
    eprintln!("  total                {:>12}", mb(data));
    eprintln!("\nanchor commits by items anchored in them (commits, items):");
    for (i, (commits, items)) in buckets.iter().enumerate() {
        let lo = 10u64.pow(i as u32);
        let range = if i + 1 < buckets.len() {
            format!("[{lo}, {})", lo * 10)
        } else {
            format!("[{lo}, ∞)")
        };
        eprintln!("  {range:>16}  {commits:>6}  {items:>10}");
    }
    eprintln!(
        "  items anchored in the {load_commits} initial-load commits: {items_in_load} of {items_total}"
    );
    eprintln!("\nauth-change paths (vs snapshot data):");
    eprintln!(
        "  per item             {:>12}  {}  ({:.2} nodes, {:.0} B per item)",
        mb(per_item),
        ratio(per_item),
        path_nodes as f64 / items_total as f64,
        per_item as f64 / items_total as f64
    );
    eprintln!(
        "  per commit, shared   {:>12}  {}",
        mb(shared),
        ratio(shared)
    );
    eprintln!("  per commit, whole    {:>12}  {}", mb(whole), ratio(whole));
    eprintln!(
        "\nstate root parts of anchor commits ≠ N: {}",
        mb(commit_parts)
    );
    eprintln!(
        "MMR proofs: {} summed, {} with shared hashes once",
        mb(mmr_sum),
        mb(mmr_shared)
    );

    drop(reader);
    drop(lvmt);
    drop(db);
    if std::env::var("BENCH_KEEP").is_err() {
        clear_dir(&historical_path);
        clear_dir(&pending_path);
    }
}
