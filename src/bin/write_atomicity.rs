use cfx_storage2::backends::impls::kvdb_rocksdb::open_database;
use cfx_storage2::backends::{DatabaseTrait, TableName, TableRead, TableSchema, WriteSchemaTrait};
use kvdb_rocksdb::Database;
use std::borrow::Cow;
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

const DB_PATH: &str = "__test_write_atomitic";
const NUM_KEYS_PER_BATCH: usize = 100_000_000;
const FIRST_KEY: &[u8] = b"key_0";
const LAST_KEY_INDEX: usize = NUM_KEYS_PER_BATCH - 1;

fn main() {
    check_db_state();

    std::fs::create_dir_all(DB_PATH).unwrap();
    let mut db = open_database(2, DB_PATH).unwrap();
    let transaction_id_counter = Arc::new(AtomicUsize::new(0));

    println!("Starting write loop... PID: {}", process::id());
    println!(
        "In another terminal, run 'kill -9 {}' when you see the 'Starting large write...' message.",
        process::id()
    );
    println!("----------------------------------------------------------");

    loop {
        let tx_id = transaction_id_counter.fetch_add(1, Ordering::SeqCst);

        let write_schema = Database::write_schema();
        let table_op = (0..NUM_KEYS_PER_BATCH).map(|i| {
            (
                Cow::Owned(format!("key_{}", i).as_bytes().to_vec()),
                Some(Cow::Owned(
                    format!("value_tx_{}", tx_id).as_bytes().to_vec(),
                )),
            )
        });
        write_schema.write_batch::<MockTable1>(table_op);

        println!(
            "\nStarting large write for transaction ID: {}. Press kill now!",
            tx_id
        );
        let start = Instant::now();

        db.commit(write_schema).expect("Failed to write batch");

        let duration = start.elapsed();
        println!(
            "Successfully committed transaction ID: {}. Write took: {:?}",
            tx_id, duration
        );
    }
}

#[derive(Clone, Copy)]
struct MockTable1;
impl TableSchema for MockTable1 {
    const NAME: TableName = TableName::CommitID;
    type Key = [u8];
    type Value = [u8];
}

fn check_db_state() {
    println!("\n--- Checking Database State ---");

    std::fs::create_dir_all(DB_PATH).unwrap();
    let db = open_database(2, DB_PATH).unwrap();
    let table = Arc::new(db).view::<MockTable1>().unwrap();

    let last_key = format!("key_{}", LAST_KEY_INDEX);

    let first_val_opt = table.get(FIRST_KEY).expect("Failed to read first key");
    let last_val_opt = table
        .get(last_key.as_bytes())
        .expect("Failed to read last key");

    match (first_val_opt, last_val_opt) {
        (Some(first_val), Some(last_val)) => {
            let fv_str = String::from_utf8(first_val.to_vec()).unwrap();
            let lv_str = String::from_utf8(last_val.to_vec()).unwrap();

            println!("CONSISTENT STATE: Both first and last keys exist.");
            println!("  First key value: {}", fv_str);
            println!("  Last key value:  {}", lv_str);

            if fv_str == lv_str {
                let tx_id = fv_str.split('_').last().unwrap();
                println!(
                    "=> OK: Values are from the same transaction (ID={}). Atomicity holds.",
                    tx_id
                );
            } else {
                println!(
                    "=> !!! CRITICAL ERROR: Values from DIFFERENT transactions. Atomicity BROKEN."
                );
            }
        }
        (None, None) => {
            println!("CONSISTENT STATE: First and last keys do not exist.");
            println!("=> OK: The last transaction was likely killed mid-flight and rolled back. Atomicity holds.");
        }
        (Some(_), None) => {
            println!("=> !!! CRITICAL ERROR: First key exists, but last key does not. Partial write detected. Atomicity BROKEN.");
        }
        (None, Some(_)) => {
            println!(
                "=> !!! CRITICAL ERROR: Last key exists, but first key does not. Atomicity BROKEN."
            );
        }
    }

    println!("--- Check Complete ---\n");
}
