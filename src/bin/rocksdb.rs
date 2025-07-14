use std::{
    borrow::Cow, fs, sync::Arc, thread::sleep, time::{Duration, Instant}
};

use asb_options::{Options, StructOpt};
use asb_profile::{Counter, Profiler, Reporter};
use asb_tasks::{Event, Events, TaskTrait};
use fs_extra::dir::CopyOptions;

use cfx_storage2::backends::{impls::kvdb_rocksdb::open_database, DatabaseTrait, InMemoryDatabase, TableName, TableRead, TableSchema, WriteSchemaTrait};

#[derive(Clone, Copy)]
struct MockTable;
impl TableSchema for MockTable {
    const NAME: TableName = TableName::CommitID;
    type Key = Vec<u8>;
    type Value = Vec<u8>;
}

fn warmup<D: DatabaseTrait>(
    db: &mut D,
    tasks: Box<dyn Iterator<Item = Events> + '_>,
    opts: &Options,
) {
    let time = Instant::now();
    
    for (epoch, events) in tasks.enumerate() {
        let changes = events.0.into_iter().filter_map(|event| match event {
            Event::Write(key, value) => {
                Some((Cow::Owned::<Vec<u8>>(key), Some(Cow::Owned::<Vec<u8>>(value))))
            }
            Event::Read(_) => None,
        });

        let write_schema = D::write_schema();
        write_schema.write_batch::<MockTable>(changes);

        db.commit(write_schema)
                .unwrap();

        if (epoch + 1) % opts.report_epoch == 0 {
            println!(
                "Time {:>7.3?}s, Warming up epoch: {:>5}",
                time.elapsed().as_secs_f64(),
                epoch + 1
            );
        }
    }
}

pub fn run_tasks<D: DatabaseTrait>(
    db: &mut D,
    // _backend_any: Arc<dyn Any>,
    tasks: Arc<dyn TaskTrait>,
    mut reporter: Reporter,
    opts: &Options,
) {
    println!("Start warming up");
    if opts.warmup_from.is_none() && !opts.no_warmup {
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
    } 
    println!("Warm up done");

    let frequency = if opts.report_dir.is_none() { -1 } else { 250 };
    let mut profiler = Profiler::new(frequency);
    reporter.start();

    for (delta_epoch, events) in tasks.tasks().enumerate() {
        // epoch should be different from those in warmup
        let epoch = delta_epoch;

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

        let view = db.view::<MockTable>().unwrap();
        for event in events.0.into_iter() {
            match event {
                Event::Read(key) => {
                    read_count += 1;

                    let ans = view.get(&key).unwrap();

                    if ans.is_none() {
                        reporter.notify_empty_read();
                    }
                }
                Event::Write(key, value) => {
                    if write_count <= 1 {
                        write_count += 1;
                        changes.push((Cow::Owned::<Vec<u8>>(key), Some(Cow::Owned::<Vec<u8>>(value))))
                    }
                }
            }
        }

        drop(view);

        let write_schema = D::write_schema();
        write_schema.write_batch::<MockTable>(changes.into_iter());
        db.commit(write_schema).unwrap();

        reporter.notify_epoch(epoch, read_count, write_count, opts);
    }

    reporter.collect_profiling(profiler);
}

pub fn initialize_lvmt<D: DatabaseTrait>(
    backend: D,
    opts: &Options,
) -> (D, Reporter<'_>) {
    // omit opts.algorithm, use LVMT directly
    let counter = Box::<Counter>::default();

    let mut reporter = Reporter::new(opts);
    reporter.set_counter(counter);

    (backend, reporter)
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
            let backend = open_database(1, db_dir).unwrap();
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