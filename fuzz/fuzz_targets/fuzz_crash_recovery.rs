#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use once_cell::sync::Lazy;
use std::sync::Arc;
use std::{fs, path::PathBuf};
use tempfile::tempdir;

// 被测系统依赖
use cfx_storage2::backends::{
    impls::kvdb_rocksdb::WrappedRocksDb, DatabaseTrait, HistoricalTableName, PendingTableName,
};
use cfx_storage2::errors::Result as LvmtResult;
use cfx_storage2::lvmt::crypto::PE;
use cfx_storage2::middlewares::CommitID;
use cfx_storage2::LvmtStorage;

use amt::{AmtParams, CreateMode};
use ethereum_types::H256;

// 直接复用被测代码中现成的逻辑（请确保这些路径与您的 crate 一致）
use cfx_storage2::lvmt::tests::{
    // 这些辅助函数/类型是从您提供的集成测试代码中抽象导入的别名。
    // 如果它们位于测试模块，请在 crate 中为 fuzz 公开一个 tests_supports_for_crash_recovery mod。
    TestSetup,
    run_phase_1,
    run_verification_after_successful_promotion,
    run_verification_after_failed_promotion,
    ConstructionMethod,
    // 为 fuzz 复用的大规模参数与 AMT
    TEST_LEVEL,
    get_setup,
};

/// 若您尚未把上述测试内函数/类型抽到公共模块，
/// 可以直接把它们对应实现复制到一个 `tests_supports_for_crash_recovery` 公共模块中，
/// 以便 fuzz 代码重用。下方的 AMT 也可以与集成测试共用。
pub static AMT: Lazy<AmtParams<PE>> = Lazy::new(|| {
    AmtParams::from_dir_mont("./pp", TEST_LEVEL, TEST_LEVEL, CreateMode::Neither, None)
});

/// 崩溃注入点
#[derive(Arbitrary, Clone, Copy, Debug)]
enum CrashPoint {
    /// 正常执行完整 confirmed_pending_to_history，再重启（应为成功态）
    None,
    /// 仅使 pending 前进（make_pending_db_ahead_for_test），再重启（应回滚，随后允许重试并与成功态一致）
    PendingOnly,
    /// 仅使 historical 前进（make_historical_db_ahead_for_test），再重启
    /// Unified: 直接恢复到成功态
    /// Split: from_recovery 失败（NoValidSnapshotFound），随后 from_bootstrap，自举后再补一次 commit_2，最后应与成功态一致
    HistoricalOnly,
}

/// 构造方式
#[derive(Arbitrary, Clone, Copy, Debug)]
enum FuzzConstructionMethod {
    Unified,
    Split,
}

impl From<FuzzConstructionMethod> for ConstructionMethod {
    fn from(v: FuzzConstructionMethod) -> Self {
        match v {
            FuzzConstructionMethod::Unified => ConstructionMethod::Unified,
            FuzzConstructionMethod::Split => ConstructionMethod::Split,
        }
    }
}

/// 数据规模
#[derive(Arbitrary, Clone, Copy, Debug)]
enum SizeClass {
    Small,
    Medium,
}

impl SizeClass {
    fn to_num_keys(self) -> usize {
        match self {
            SizeClass::Small => 64,
            SizeClass::Medium => 512,
        }
    }
}

/// 是否在恢复成功后再追加一次“重试确认”（主要针对 PendingOnly；其他情况下忽略）
#[derive(Arbitrary, Clone, Copy, Debug)]
enum RetryHint {
    Noop,
    RetryOnce,
}

/// fuzz 输入
#[derive(Arbitrary, Clone, Debug)]
struct CrashFuzzInput {
    method: FuzzConstructionMethod,
    crash: CrashPoint,
    size: SizeClass,
    retry_hint: RetryHint,
    /// 低概率尝试 InMemory（可扩展），当前固定 RocksDB 以覆盖真实持久化路径
    #[allow(dead_code)]
    prefer_in_memory: bool,
}

fn build_rocks(h: &PathBuf, p: &PathBuf) -> (Arc<WrappedRocksDb<HistoricalTableName>>, Arc<WrappedRocksDb<PendingTableName>>) {
    fs::create_dir_all(h).ok();
    fs::create_dir_all(p).ok();
    let historical_db = WrappedRocksDb::open(h).expect("open historical");
    let pending_db = WrappedRocksDb::open(p).expect("open pending");
    (Arc::new(historical_db), Arc::new(pending_db))
}

fn construct_db_unified(
    historical_db: Arc<WrappedRocksDb<HistoricalTableName>>,
    pending_db: Arc<WrappedRocksDb<PendingTableName>>,
) -> LvmtResult<LvmtStorage<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>> {
    LvmtStorage::new(historical_db, pending_db)
}

fn construct_db_split_empty_pending(
    historical_db: Arc<WrappedRocksDb<HistoricalTableName>>,
    pending_db: Arc<WrappedRocksDb<PendingTableName>>,
) -> LvmtResult<LvmtStorage<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>> {
    LvmtStorage::from_empty_pending_for_fuzzing(historical_db, pending_db)
}

fn reconstruct_after_crash(
    method: ConstructionMethod,
    historical_db: Arc<WrappedRocksDb<HistoricalTableName>>,
    pending_db: Arc<WrappedRocksDb<PendingTableName>>,
) -> LvmtResult<LvmtStorage<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>> {
    match method {
        ConstructionMethod::Unified => LvmtStorage::new(historical_db, pending_db),
        ConstructionMethod::Split => LvmtStorage::from_recovery_for_fuzzing(historical_db, pending_db),
    }
}

fuzz_target!(|input: CrashFuzzInput| {
    // 临时目录隔离每次 run
    let tmp = tempdir().expect("tempdir");
    let historical_path = tmp.path().join("historical");
    let pending_path = tmp.path().join("pending");

    let (historical_db, pending_db) = build_rocks(&historical_path, &pending_path);

    // 选择构造方式
    let method: ConstructionMethod = input.method.into();

    // 生成/获取测试数据集（重用 Lazy 缓存的大规模 setup）
    let num_keys = input.size.to_num_keys();
    let setup = get_setup(num_keys);

    // 预置阶段：在崩溃前构造状态，执行 phase 1
    let mut db = match method {
        ConstructionMethod::Unified => construct_db_unified(historical_db.clone(), pending_db.clone()).unwrap(),
        ConstructionMethod::Split => construct_db_split_empty_pending(historical_db.clone(), pending_db.clone()).unwrap(),
    };
    // 执行非分叉与分叉提交
    run_phase_1(&mut db, &setup);

    // 注入崩溃点：对 confirmed_pending_to_history 的不同阶段进行“半步落盘”
    match input.crash {
        CrashPoint::None => {
            // 正常完成确认：把 commit_2 作为新根推进
            db.confirmed_pending_to_history_with_commit_id(setup.commit_2).ok();
            // 模拟关机
            drop(db);

            // 重启与恢复
            let mut db = reconstruct_after_crash(method, historical_db.clone(), pending_db.clone()).unwrap();
            // 恢复后应处于“成功态”
            run_verification_after_successful_promotion(&mut db, &setup);
        }

        CrashPoint::PendingOnly => {
            // 仅 pending 前进，historical 未前进
            db.make_pending_db_ahead_for_test(setup.commit_2).ok();
            drop(db);

            // 重启与恢复
            let mut db = reconstruct_after_crash(method, historical_db.clone(), pending_db.clone()).unwrap();

            // 恢复后应为“失败态”（已回滚），且 commit_2_1 仍存在
            run_verification_after_failed_promotion(&mut db, &setup);

            // 可选：立即重试一次（逻辑内部会完成并验证）
            if let RetryHint::RetryOnce = input.retry_hint {
                // run_verification_after_failed_promotion 内部已经调用了一次
                // confirmed_pending_to_history_with_commit_id(setup.commit_2) 并随后
                // run_verification_after_successful_promotion(&mut db, &setup)
                // 因此这里无需重复操作
            }
        }

        CrashPoint::HistoricalOnly => {
            // 仅 historical 前进，pending 未前进
            db.make_historical_db_ahead_for_test(setup.commit_2).unwrap();
            drop(db);

            match method {
                ConstructionMethod::Unified => {
                    // Unified：重启后先补一次 commit_2，使 pending 形态与“成功恢复场景”一致，再做成功态验证
                    let mut db = reconstruct_after_crash(method, historical_db.clone(), pending_db.clone()).unwrap();
                    {
                        let mut lvmt = db.as_manager().unwrap();
                        lvmt.commit(
                            Some(setup.commit_1),
                            setup.commit_2,
                            H256::zero(),
                            TestSetup::changes_iter(&setup.updates_2),
                            &AMT,
                        ).unwrap();
                        lvmt.check_consistency(setup.commit_2, &AMT).unwrap();
                    }
                    run_verification_after_successful_promotion(&mut db, &setup);
                }
                ConstructionMethod::Split => {
                    // Split：严格匹配 from_recovery 的失败错误为 NoValidSnapshotFound
                    let db_res = reconstruct_after_crash(method, historical_db.clone(), pending_db.clone());
                    match db_res {
                        Ok(mut db) => {
                            // 若实现更新使得可直接恢复，则直接验证成功态
                            run_verification_after_successful_promotion(&mut db, &setup);
                        }
                        Err(e) => {
                            use cfx_storage2::StorageError;
                            use cfx_storage2::middlewares::RecoveryError;

                            match e {
                                StorageError::RecoveryError(RecoveryError::NoValidSnapshotFound) => {
                                    // 与集成测试一致：走 bootstrap → 补一次 commit_2 → 成功态验证
                                    let mut db = LvmtStorage::from_bootstrap_for_fuzzing(
                                        historical_db.clone(),
                                        pending_db.clone(),
                                    ).unwrap();

                                    {
                                        let mut lvmt = db.as_manager().unwrap();
                                        lvmt.commit(
                                            Some(setup.commit_1),
                                            setup.commit_2,
                                            H256::zero(),
                                            TestSetup::changes_iter(&setup.updates_2),
                                            &AMT,
                                        ).unwrap();
                                        lvmt.check_consistency(setup.commit_2, &AMT).unwrap();
                                    }

                                    run_verification_after_successful_promotion(&mut db, &setup);
                                }
                                other => {
                                    panic!(
                                        "Split + HistoricalOnly: unexpected recovery error: {:?}",
                                        other
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // 临时目录会自动清理
});