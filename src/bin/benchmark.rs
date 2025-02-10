use std::{
    fs,
    sync::Arc,
    thread::sleep,
    time::{Duration, Instant},
};

use amt::{AmtParams, CreateMode};
use asb_options::{Options, StructOpt};
use asb_profile::{Counter, Profiler, Reporter};
use asb_tasks::{Event, Events, TaskTrait};
use ethereum_types::H256;
use fs_extra::dir::CopyOptions;

use cfx_storage2::{
    backends::{impls::kvdb_rocksdb::open_database, DatabaseTrait, InMemoryDatabase, TableName},
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
) -> (Option<CommitID>, usize) {
    let time = Instant::now();

    // Get a manager for db
    let mut lvmt = db.as_manager().unwrap();
    let write_schema = D::write_schema();

    let mut old_commit = None;
    let mut num_epochs = 0;
    for (epoch, events) in tasks.enumerate() {
        num_epochs += 1;

        // Perform a non-forking commit; the current version including no deletion
        let changes = events.0.into_iter().filter_map(|event| match event {
            Event::Write(key, value) => {
                Some((key.into_boxed_slice(), Some(value.into_boxed_slice())))
            }
            Event::Read(_) => None,
        });
        let current_commit = get_commit_id_from_epoch_id(epoch);
        lvmt.commit(old_commit, current_commit, changes, &write_schema, &AMT)
            .unwrap();

        old_commit = Some(current_commit);

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
    drop(lvmt);
    if let Some(last_commit) = old_commit {
        db.confirmed_pending_to_history(last_commit, &write_schema)
            .unwrap();
        db.commit(write_schema).unwrap();
    }

    (old_commit, num_epochs)
}

fn get_commit_id_from_epoch_id(epoch_id: usize) -> CommitID {
    H256::from_low_u64_be(epoch_id as u64)
}

pub fn run_tasks<D: DatabaseTrait>(
    db: &mut LvmtStorage<D>,
    // _backend_any: Arc<dyn Any>,
    tasks: Arc<dyn TaskTrait>,
    mut reporter: Reporter,
    opts: &Options,
) {
    println!("Start warming up");
    let (mut old_commit, num_warmup_epochs) = if opts.warmup_from.is_none() && !opts.no_warmup {
        let old_commit = warmup(db, tasks.warmup(), opts);
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
            println!("Writing warmup to {}", warmup_dir);
            let mut retry_cnt = 0usize;
            while retry_cnt < 10 {
                if let Err(e) = fs_extra::dir::copy(&opts.db_dir, warmup_dir, &copy_options) {
                    println!("Fail to save warmup file {:?}. Retry...", e);
                    retry_cnt += 1;
                } else {
                    println!("Writing done");
                    return;
                }
            }

            panic!("Retry limit exceeds!");
        }
        old_commit
    } else {
        (None, 0)
    };
    println!("Warm up done");

    let frequency = if opts.report_dir.is_none() { -1 } else { 250 };
    let mut profiler = Profiler::new(frequency);
    reporter.start();

    // Get a manager for db
    let mut lvmt = db.as_manager().unwrap();
    let write_schema = D::write_schema();

    for (delta_epoch, events) in tasks.tasks().enumerate() {
        // epoch should be different from those in warmup
        let epoch = delta_epoch + num_warmup_epochs;

        if reporter.start_time.elapsed().as_secs() >= opts.max_time.unwrap_or(u64::MAX)
            || delta_epoch + 1 >= opts.max_epoch.unwrap_or(usize::MAX)
        {
            profiler.tick();
            break;
        }

        if (delta_epoch + 1) % opts.profile_epoch == 0 {
            profiler.tick();
        }

        let mut read_count = 0;
        let mut write_count = 0;

        // Perform a non-forking commit; the current version including no deletion
        let mut changes = Vec::new();
        for event in events.0.into_iter() {
            match event {
                Event::Read(key) => {
                    read_count += 1;

                    let ans = if let Some(old_commit) = old_commit {
                        // lvmt.get_key(old_commit, &key).unwrap()
                        lvmt.get_state(old_commit)
                            .unwrap()
                            .get(&key.into_boxed_slice())
                            .unwrap()
                    } else {
                        None
                    };

                    if ans.is_none() {
                        reporter.notify_empty_read();
                    }
                }
                Event::Write(key, value) => {
                    write_count += 1;
                    changes.push((key.into_boxed_slice(), Some(value.into_boxed_slice())))
                }
            }
        }

        let current_commit = get_commit_id_from_epoch_id(epoch);
        lvmt.commit(
            old_commit,
            current_commit,
            changes.into_iter(),
            &write_schema,
            &AMT,
        )
        .unwrap();

        old_commit = Some(current_commit);

        reporter.notify_epoch(epoch, read_count, write_count, opts);
    }

    // Persist confirmed commits from caches to the backend.
    // Must drop the manager first because it holds a read reference to the backend.
    drop(lvmt);
    if let Some(last_commit) = old_commit {
        db.confirmed_pending_to_history(last_commit, &write_schema)
            .unwrap();
        db.commit(write_schema).unwrap();
    }

    reporter.collect_profiling(profiler);
}

pub fn initialize_lvmt<'a, D: DatabaseTrait>(
    backend: D,
    opts: &'a Options,
) -> (LvmtStorage<D>, Reporter<'a>) {
    // omit opts.algorithm, use LVMT directly
    let db = LvmtStorage::<D>::new(backend).unwrap();
    let counter = Box::new(Counter::default());

    let mut reporter = Reporter::new(opts);
    reporter.set_counter(counter);

    return (db, reporter);
}

fn main() {
    let options: Options = Options::from_args();
    if options.stat_mem && !options.no_stat {
        panic!("Stat will introduce memory cost")
    }
    println!(
        "Testing {:?} with {}",
        options.algorithm,
        if options.real_trace {
            "real trace".into()
        } else {
            format!("{:e} addresses", options.total_keys)
        }
    );

    let db_dir = &options.db_dir;
    let _ = fs::remove_dir_all(db_dir);
    fs::create_dir_all(db_dir).unwrap();

    if let Some(ref warmup_dir) = options.warmup_from() {
        println!("warmup from {}", warmup_dir);
        let mut options = CopyOptions::new();
        options.content_only = true;
        fs_extra::dir::copy(warmup_dir, db_dir, &options).unwrap();
    }

    if let Some(ref dir) = options.report_dir {
        fs::create_dir_all(dir).unwrap()
    }

    let tasks = asb_tasks::tasks(&options);

    match options.backend {
        asb_options::Backend::RocksDB => {
            let backend = open_database(TableName::max_index() + 1, db_dir).unwrap();
            let (mut db, reporter) = initialize_lvmt(backend, &options);
            run_tasks(&mut db, tasks, reporter, &options);
        }
        asb_options::Backend::InMemoryDB => {
            let backend = InMemoryDatabase::empty();
            let (mut db, reporter) = initialize_lvmt(backend, &options);
            run_tasks(&mut db, tasks, reporter, &options);
        }
        asb_options::Backend::MDBX => panic!("Only support backend of RocksDB or InMemoryDatabase"),
    };
}
