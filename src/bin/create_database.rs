#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::{
    fs,
    sync::Arc,
    thread::sleep,
    time::{Duration, Instant},
};

use amt::{AmtParams, CreateMode};
use asb_options::{Options, StructOpt};
use asb_tasks::{Event, Events, TaskTrait};
use ethereum_types::H256;
use fs_extra::dir::CopyOptions;

use cfx_storage2::{
    backends::{impls::kvdb_rocksdb::CachedDB, DatabaseTrait, TableName},
    lvmt::{crypto::PE, example::LvmtStorage},
    middlewares::CommitID,
};

use once_cell::sync::Lazy;

pub const TEST_LEVEL: usize = 16;

pub static AMT: Lazy<AmtParams<PE>> =
    Lazy::new(|| AmtParams::from_dir_mont("./pp", TEST_LEVEL, TEST_LEVEL, CreateMode::Both, None));

fn warmup<D: DatabaseTrait>(
    db: &mut LvmtStorage<D>,
    tasks: Box<dyn Iterator<Item = Events> + '_>,
    opts: &Options,
) {
    let time = Instant::now();

    // Get a manager for db
    let mut lvmt = db.as_manager().unwrap();
    let mut write_schema = D::write_schema();

    let mut old_commit = None;
    let mut num_epochs = 0;
    let mut batched_changes = Vec::new();
    for (epoch, events) in tasks.enumerate() {
        let changes = events.0.into_iter().filter_map(|event| match event {
            Event::Write(key, value) => {
                Some((key.into_boxed_slice(), Some(value.into_boxed_slice())))
            }
            Event::Read(_) => None,
        });

        batched_changes.extend(changes);

        num_epochs += 1;

        // Perform a non-forking commit; the current version including no deletion

        let current_commit = get_commit_id_from_epoch_id(num_epochs);
        lvmt.commit(
            old_commit,
            current_commit,
            batched_changes.into_iter(),
            &write_schema,
            &AMT,
        )
        .unwrap();

        old_commit = Some(current_commit);

        batched_changes = Vec::new();

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
        
        if (epoch + 1) % opts.report_epoch == 0 {
            println!(
                "Time {:>7.3?}s, Warming up epoch: {:>5}",
                time.elapsed().as_secs_f64(),
                epoch + 1
            );
        }
    }

    // Persist confirmed commits from caches to the backend.
    // Must drop the manager first because it holds a read reference to the backend.
    if !batched_changes.is_empty() {
        num_epochs += 1;

        // Perform a non-forking commit; the current version including no deletion

        let current_commit = get_commit_id_from_epoch_id(num_epochs);
        lvmt.commit(
            old_commit,
            current_commit,
            batched_changes.into_iter(),
            &write_schema,
            &AMT,
        )
        .unwrap();

        old_commit = Some(current_commit);

        // Persist confirmed commits from caches to the backend.
        // Must drop the manager first because it holds a read reference to the backend.
        drop(lvmt);
        if let Some(last_commit) = old_commit {
            db.confirmed_pending_to_history(last_commit, &write_schema)
                .unwrap();
            db.remove_pending_to_history(last_commit, &write_schema)
                .unwrap();
            db.commit(write_schema).unwrap();
        }
    } else {
        drop(lvmt);
        if let Some(last_commit) = old_commit {
            db.remove_pending_to_history(last_commit, &write_schema)
                .unwrap();
            db.commit(write_schema).unwrap();
        }
    }
}

#[inline]
fn get_commit_id_from_epoch_id(epoch_id: usize) -> CommitID {
    H256::from_low_u64_be(epoch_id as u64)
}

pub fn creating_database<D: DatabaseTrait>(
    db: &mut LvmtStorage<D>,
    // _backend_any: Arc<dyn Any>,
    tasks: Arc<dyn TaskTrait>,
    opts: &Options,
) {
    warmup(db, tasks.warmup(), opts);
    
    if let Some(ref warmup_dir) = opts.warmup_to() {
        println!("Waiting for post ops");

        sleep(Duration::from_secs_f64(f64::max(
            1.0,
            opts.total_keys as f64 / 1e6,
        )));

        let _ = fs::remove_dir_all(warmup_dir);
        fs::create_dir_all(warmup_dir).unwrap();

        let mut copy_options = CopyOptions::new();
        copy_options.overwrite = true;
        copy_options.copy_inside = true;
        copy_options.content_only = true;
        println!("Writing database to {}", warmup_dir);
        let mut retry_cnt = 0usize;
        while retry_cnt < 10 {
            if let Err(e) = fs_extra::dir::copy(&opts.db_dir, warmup_dir, &copy_options) {
                println!("Fail to save database file {:?}. Retry...", e);
                retry_cnt += 1;
            } else {
                println!("Writing done");
                return;
            }
        }

        panic!("Retry limit exceeds!");
    }
}

fn main() {
    let options: Options = Options::from_args();
    if options.stat_mem && !options.no_stat {
        panic!("Stat will introduce memory cost")
    }
    println!(
        "Creating database with {}",
        if options.real_trace {
            "real trace".into()
        } else {
            format!("{:e} addresses", options.total_keys)
        }
    );

    let db_dir = &options.db_dir;
    let _ = fs::remove_dir_all(db_dir);
    fs::create_dir_all(db_dir).unwrap();

    assert!(options.warmup_from.is_none() && options.warmup_to.is_some() && !options.no_warmup, "This program is for creating the database. The input parameters require no `warmup_from` but include `warmup_to`, and `no_warmup` must be false."); 

    if let Some(ref dir) = options.report_dir {
        fs::create_dir_all(dir).unwrap()
    }

    let tasks = asb_tasks::tasks(&options);

    match options.backend {
        asb_options::Backend::RocksDB => {
            let backend = CachedDB::open(TableName::max_index() + 1, db_dir).unwrap();
            
            let mut db = LvmtStorage::<CachedDB>::new(backend).unwrap();

            creating_database(&mut db, tasks.clone(), &options);
        }
        _ => panic!("Only support backend of RocksDB"),
    };
}
