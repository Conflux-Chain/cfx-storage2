use std::{
    fs,
    sync::Arc,
    thread::sleep,
    time::{Duration, Instant},
};

use asb_options::{Options, StructOpt};
use asb_profile::{Counter, Profiler, Reporter};
use asb_tasks::{Event, Events, TaskTrait};
use ethereum_types::H256;
use fs_extra::dir::CopyOptions;

use cfx_storage2::{
    backends::{impls::kvdb_rocksdb::open_database, DatabaseTrait, InMemoryDatabase, TableName},
    errors::Result,
    example::FlatKeyValue,
    middlewares::{
        confirm_ids_to_history, confirm_maps_to_history, CommitID, VersionedStore,
        VersionedStoreCache,
    },
    traits::{KeyValueStoreManager, KeyValueStoreRead},
};

pub struct FlatStorage<D: DatabaseTrait> {
    backend: D,
    cache: VersionedStoreCache<FlatKeyValue>,
}

impl<D: DatabaseTrait> FlatStorage<D> {
    pub fn new(backend: D) -> Result<Self> {
        Ok(Self {
            backend,
            cache: VersionedStoreCache::new_empty(),
        })
    }

    pub fn as_manager(&mut self) -> Result<VersionedStore<'_, '_, FlatKeyValue>> {
        VersionedStore::new(&self.backend, &mut self.cache)
    }

    pub fn commit(&mut self, write_schema: <D as DatabaseTrait>::WriteSchema) -> Result<()> {
        self.backend.commit(write_schema)
    }

    pub fn confirmed_pending_to_history(
        &mut self,
        new_root_commit_id: CommitID,
        write_schema: &D::WriteSchema,
    ) -> Result<()> {
        let confirmed_path = self.cache.change_root(new_root_commit_id)?;

        let start_height = confirmed_path.start_height;
        let commit_ids = &confirmed_path.commit_ids;

        confirm_ids_to_history::<D>(&self.backend, start_height, commit_ids, write_schema)?;

        confirm_maps_to_history::<D, FlatKeyValue>(
            &self.backend,
            start_height,
            confirmed_path.key_value_maps,
            write_schema,
        )?;

        Ok(())
    }
}

fn warmup<D: DatabaseTrait>(
    db: &mut FlatStorage<D>,
    tasks: Box<dyn Iterator<Item = Events> + '_>,
    opts: &Options,
) -> (Option<CommitID>, usize) {
    let time = Instant::now();

    // Get a manager for db
    let mut manager = db.as_manager().unwrap();
    let write_schema = D::write_schema();

    let mut old_commit = None;
    let mut num_epochs = 0;
    for (epoch, events) in tasks.enumerate() {
        num_epochs += 1;

        // Perform a non-forking commit; the current version including no deletion
        let changes = events
            .0
            .into_iter()
            .filter_map(|event| match event {
                Event::Write(key, value) => {
                    Some((key.into_boxed_slice(), Some(value.into_boxed_slice())))
                }
                Event::Read(_) => None,
            })
            .collect();
        let current_commit = get_commit_id_from_epoch_id(epoch);
        manager
            .add_to_pending_part(old_commit, current_commit, changes)
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
    drop(manager);
    if let Some(last_commit) = old_commit {
        db.confirmed_pending_to_history(last_commit, &write_schema)
            .unwrap();
        db.commit(write_schema).unwrap();
    }

    (old_commit, num_epochs)
}

#[inline]
fn get_commit_id_from_epoch_id(epoch_id: usize) -> CommitID {
    H256::from_low_u64_be(epoch_id as u64)
}

pub fn run_tasks<D: DatabaseTrait>(
    db: &mut FlatStorage<D>,
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
    let mut manager = db.as_manager().unwrap();
    let mut write_schema = D::write_schema();

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
                        manager
                            .get_versioned_store(&old_commit)
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
        manager
            .add_to_pending_part(old_commit, current_commit, changes.into_iter().collect())
            .unwrap();

        old_commit = Some(current_commit);

        reporter.notify_epoch(epoch, read_count, write_count, opts);

        if (delta_epoch + 1) % opts.commit_epoch == 0 {
            // Persist confirmed commits from caches to the backend.
            // Must drop the manager first because it holds a read reference to the backend.
            drop(manager);
            if let Some(last_commit) = old_commit {
                db.confirmed_pending_to_history(last_commit, &write_schema)
                    .unwrap();
                db.commit(write_schema).unwrap();
            }

            // Get a new manager for db
            manager = db.as_manager().unwrap();
            write_schema = D::write_schema();
        }
    }

    reporter.collect_profiling(profiler);
}

pub fn initialize_storage<D: DatabaseTrait>(
    backend: D,
    opts: &Options,
) -> (FlatStorage<D>, Reporter<'_>) {
    // omit opts.algorithm, use RAW directly
    let db = FlatStorage::<D>::new(backend).unwrap();
    let counter = Box::<Counter>::default();

    let mut reporter = Reporter::new(opts);
    reporter.set_counter(counter);

    (db, reporter)
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
            let (mut db, reporter) = initialize_storage(backend, &options);
            run_tasks(&mut db, tasks, reporter, &options);
        }
        asb_options::Backend::InMemoryDB => {
            let backend = InMemoryDatabase::empty();
            let (mut db, reporter) = initialize_storage(backend, &options);
            run_tasks(&mut db, tasks, reporter, &options);
        }
        asb_options::Backend::MDBX => panic!("Only support backend of RocksDB or InMemoryDatabase"),
    };
}
