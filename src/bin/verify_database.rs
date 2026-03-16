use std::collections::HashSet;

use cfx_storage2::{
    backends::{
        impls::kvdb_rocksdb::CachedDB, serde::Encode, DatabaseTrait, TableName, TableRead,
        TableSchema,
    },
    lvmt::FlatKeyValue,
    middlewares::table_schema::HistoryChangeTable,
};

// Function specifically written for HistoryChangeTable
fn count_history_change_flatkv(view: Box<dyn '_ + TableRead<HistoryChangeTable<FlatKeyValue>>>) {
    println!("-----------------------------------------");
    println!("{:?}", HistoryChangeTable::<FlatKeyValue>::NAME);
    let mut count: u64 = 0;
    let mut total_key_size: u64 = 0;
    let mut total_value_size: u64 = 0;
    
    let mut delete_kv_count: u64 = 0;
    let mut diff_keys = HashSet::with_capacity(40_000_000);

    for item in view.iter_from_start().unwrap() {
        let (key, value) = item.unwrap();
        count += 1;
        total_key_size += Encode::encode_cow(key.clone()).len() as u64;
        total_value_size += Encode::encode_cow(value.clone()).len() as u64;

        // The `if T == ...` check is no longer needed here, as the type is already determined
        if value.into_owned().is_deletion() {
            delete_kv_count += 1;
        }
        diff_keys.insert(key.into_owned());
    }
    drop(view);
    print_stats(count, total_key_size, total_value_size);

    // Specific verification logic
    if diff_keys.len() == 40_000_000 && delete_kv_count == 0 {
        println!("Iterating through the linear history modification records, the number of distinct accounts is 40 million, and none have been deleted, indicating that the latest version view of the database has 40 million accounts.");
        println!("Verification confirmed: The database contains 40 million accounts.");
    } else {
        panic!("The program found that the verification condition of the database containing 40 million accounts is not met.");
    }
}

fn print_stats(count: u64, total_key_size: u64, total_value_size: u64) {
    println!("Database statistics:");
    println!("- Total number of key-value pairs: {}", count);
    println!("- Total key size: {} bytes (approx. {:.2} MB)", total_key_size, total_key_size as f64 / 1_048_576.0);
    println!("- Total value size: {} bytes (approx. {:.2} MB)", total_value_size, total_value_size as f64 / 1_048_576.0);
}

fn main() {
    let db_dir = "database/LVMT_4e7/";
    let backend = CachedDB::open(TableName::max_index() + 1, db_dir).unwrap();

    count_history_change_flatkv(backend.view::<HistoryChangeTable<FlatKeyValue>>().unwrap());
}