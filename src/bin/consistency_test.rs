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

pub const TEST_LEVEL: usize = 16;

pub static AMT: Lazy<AmtParams<PE>> =
    Lazy::new(|| AmtParams::from_dir_mont("./pp", TEST_LEVEL, TEST_LEVEL, CreateMode::Both, None));

#[inline]
fn get_commit_id_from_epoch_id(epoch_id: usize) -> CommitID {
    H256::from_low_u64_be(epoch_id as u64)
}

pub fn run_consistency_tasks<D: DatabaseTrait>(
    db: &mut LvmtStorage<D>,
    write_tasks: Box<dyn Iterator<Item = Events> + '_>,
    tasks: Arc<dyn TaskTrait>,
    opts: &Options,
) {
    // Write to lvmt and oracle
    let mut oracle_db = HashMap::new();
    // Get a manager for db
    let mut lvmt = db.as_manager().unwrap();
    let mut write_schema = D::write_schema();

    let mut old_commit = None;
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

        if epoch_id % opts.commit_epoch == 0 {
            // Persist confirmed commits from caches to the backend.
            // Must drop the manager first because it holds a read reference to the backend.
            drop(lvmt);
            if let Some(last_commit) = old_commit {
                db.confirmed_pending_to_history(last_commit, &write_schema)
                    .unwrap();
                db.commit(write_schema).unwrap();
            }

            // Get a new manager for db
            lvmt = db.as_manager().unwrap();
            write_schema = D::write_schema();
        }
    }

    println!("Oracle database size: {}", oracle_db.len());

    // consistency_check
    let mut read_count = 0;
    let maybe_view = old_commit.map(|old_commit| lvmt.get_state(old_commit, true).unwrap());
    for (epoch, events) in tasks.tasks().enumerate() {
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

        if epoch + 1 >= opts.max_epoch.unwrap_or(usize::MAX)
        {
            break;
        }
    }

    println!("Consistency check passed: 100% match");
}

fn main() {
    let options: Options = Options::from_args();

    let db_dir = "./__consistency_test";
    let _ = fs::remove_dir_all(db_dir);
    fs::create_dir_all(db_dir).unwrap();

    let tasks = asb_tasks::tasks(&options);

    match options.backend {
        asb_options::Backend::RocksDB => {
            let backend = CachedDB::open(TableName::max_index() + 1, db_dir).unwrap();
            
            let mut db = LvmtStorage::<CachedDB>::new(backend).unwrap();

            run_consistency_tasks(&mut db, tasks.warmup(), tasks.clone(), &options);
        }
        _ => panic!("Only support backend of RocksDB"),
    };
}
