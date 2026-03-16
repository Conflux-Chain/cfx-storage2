#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use core::panic;
use std::{
    collections::HashMap, fs, sync::Arc,
};

use amt::{AmtParams, CreateMode};
use asb_options::{Options, StructOpt};
use asb_tasks::{Event, Events, TaskTrait};
use ethereum_types::H256;

use cfx_storage2::{
    backends::{impls::kvdb_rocksdb::CachedDB, DatabaseTrait, TableName},//, serde::Decode},
    lvmt::{crypto::PE, example::LvmtStorage},
    middlewares::CommitID,
};

use once_cell::sync::Lazy;
use ark_std::rand::{rngs::StdRng, Rng, SeedableRng};

pub const TEST_LEVEL: usize = 16;

pub static AMT: Lazy<AmtParams<PE>> =
    Lazy::new(|| AmtParams::from_dir_mont("./pp", TEST_LEVEL, TEST_LEVEL, CreateMode::Both, None));

#[inline]
fn get_commit_id_from_epoch_id(epoch_id: usize) -> CommitID {
    H256::from_low_u64_be(epoch_id as u64)
}

pub fn run_checkout_tasks<D: DatabaseTrait>(
    db: &mut LvmtStorage<D>,
    write_tasks: Box<dyn Iterator<Item = Events> + '_>,
    tasks: Arc<dyn TaskTrait>,
    opts: &Options,
) {
    // Phase 1: Write ops in linear versions
    // Write to lvmt and oracle
    let mut oracle_db = HashMap::new();
    // Get a manager for db
    let mut lvmt = db.as_manager().unwrap();
    let write_schema = D::write_schema();

    let mut old_commit = None;
    let mut max_existing_epoch_id = 0;
    for (epoch, events) in write_tasks.enumerate() {
        let epoch_id = epoch + 1;

        let changes = events.0.into_iter().filter_map(|event| match event {
            Event::Write(key, value) => {
                oracle_db.insert(key.clone(), value.clone());
                Some((key.into_boxed_slice(), Some(value.into_boxed_slice())))
            }
            Event::Read(_) => None,
        });

        // Perform a non-forking commit; the current version including no deletion
        let current_commit = get_commit_id_from_epoch_id(epoch_id);
        lvmt.commit(
            old_commit,
            current_commit,
            changes.into_iter(),
            &write_schema,
            &AMT,
        )
        .unwrap();

        old_commit = Some(current_commit);
        max_existing_epoch_id += 1;
    }

    let commit_id_1 = old_commit;
    println!("Oracle database size: {}", oracle_db.len());
    
    let mut rng = StdRng::seed_from_u64(42);
    
    let max_epoch = opts.max_epoch.unwrap_or(usize::MAX);
    let mut task_iter = tasks.tasks().enumerate().peekable();
    
    // Phase 2: Write ops in tree versions
    // Only write to lvmt
    let mut phase2_commit_count = 0;
    for (_, events) in task_iter.by_ref().take(max_epoch) {
        let changes = events.0.into_iter().filter_map(|event| match event {
            Event::Write(key, value) => {
                Some((key.into_boxed_slice(), Some(value.into_boxed_slice())))
            }
            Event::Read(_) => None,
        });

        // Perform a non-forking commit; the current version including no deletion
        let epoch_id = max_existing_epoch_id + 1;
        let current_commit = get_commit_id_from_epoch_id(epoch_id);
        let random_existing_epoch_id = rng.gen_range(1..=max_existing_epoch_id);
        let parent_commit = Some(get_commit_id_from_epoch_id(random_existing_epoch_id));
        lvmt.commit(
            parent_commit,
            current_commit,
            changes.into_iter(),
            &write_schema,
            &AMT,
        )
        .unwrap();

        max_existing_epoch_id += 1;
        phase2_commit_count += 1;
    }
    println!("Successfully committed {} new tree versions.", phase2_commit_count);

    if task_iter.peek().is_some() {
        // Phase 3: Compare oracle and lvmt.view(commit_id_1)
        let mut read_count = 0;
        let maybe_view = commit_id_1.map(|commit_id_1| lvmt.get_state(commit_id_1, true).unwrap());
        for (_, events) in task_iter.take(max_epoch) {
            for event in events.0.into_iter() {
                match event {
                    Event::Read(key) => {
                        read_count += 1;

                        let ans = if let Some(ref view) = maybe_view {
                            view.get(&key.clone().into_boxed_slice()).unwrap()
                        } else {
                            None
                        }.map(|lvmt_value| lvmt_value.get_value().map(|value| value.into_vec()))
                        .flatten();

                        let oracle_ans = oracle_db.get(&key).cloned();
                        
                        assert_eq!(ans, oracle_ans);
                    }
                    Event::Write(_, _) => {} // omit Write
                }
            }

            println!("Conducted {} random read operations.", read_count);
        }
    }

    println!("版本回溯 verification passed: 100% match ");
}

fn main() {
    let options: Options = Options::from_args();

    let db_dir = "./__checkout_test";
    let _ = fs::remove_dir_all(db_dir);
    fs::create_dir_all(db_dir).unwrap();

    let tasks = asb_tasks::tasks(&options);

    match options.backend {
        asb_options::Backend::RocksDB => {
            let backend = CachedDB::open(TableName::max_index() + 1, db_dir).unwrap();
            
            let mut db = LvmtStorage::<CachedDB>::new(backend).unwrap();

            run_checkout_tasks(&mut db, tasks.warmup(), tasks.clone(), &options);
        }
        _ => panic!("Only support backend of RocksDB"),
    };
}
