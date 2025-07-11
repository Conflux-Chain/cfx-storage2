use cfx_storage2::{backends::{impls::kvdb_rocksdb::open_database, serde::Encode, DatabaseTrait, TableName, TableRead, TableSchema}, lvmt::{AmtNodes, AuthChangeTable, FlatKeyValue, SlotAllocations}, middlewares::{table_schema::{HistoryChangeTable, HistoryIndicesTable}, CommitIDSchema, HistoryNumberSchema}};

fn count<T: TableSchema>(view: impl TableRead<T>) {
    println!("-----------------------------------------");
    println!("{:?}", T::NAME);
    let mut count: u64 = 0;
    let mut total_key_size: u64 = 0;
    let mut total_value_size: u64 = 0;
    for item in view.iter_from_start().unwrap() {
        let (key, value) = item.unwrap();
        count += 1;
        total_key_size += Encode::encode_cow(key).len() as u64;
        total_value_size += Encode::encode_cow(value).len() as u64;
    }
    drop(view);
    println!("数据库统计结果:");
    println!("- 总键值对数量: {}", count);
    println!("- 键的总大小: {} 字节 (约 {:.2} MB)", total_key_size, total_key_size as f64 / 1_048_576.0);
    println!("- 值的总大小: {} 字节 (约 {:.2} MB)", total_value_size, total_value_size as f64 / 1_048_576.0);
}

fn main() {
    let db_dir = "__lvmt_40m_size100w_warmup/LVMT_4e7/";
    let backend = open_database(TableName::max_index() + 1, db_dir).unwrap();

    count(backend.view::<CommitIDSchema>().unwrap());
    count(backend.view::<HistoryNumberSchema>().unwrap());
    count(backend.view::<AuthChangeTable>().unwrap());
    count(backend.view::<HistoryIndicesTable<FlatKeyValue>>().unwrap());
    count(backend.view::<HistoryChangeTable<FlatKeyValue>>().unwrap());
    count(backend.view::<HistoryIndicesTable<AmtNodes>>().unwrap());
    count(backend.view::<HistoryChangeTable<AmtNodes>>().unwrap());
    count(backend.view::<HistoryIndicesTable<SlotAllocations>>().unwrap());
    count(backend.view::<HistoryChangeTable<SlotAllocations>>().unwrap());
}