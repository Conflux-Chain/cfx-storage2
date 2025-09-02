pub mod impls;
pub mod serde;
mod table;
mod table_name;
mod write_schema;

use std::sync::Arc;

pub use impls::in_memory_db::WrappedInMemoryDb;
pub use table::{SeekKey, TableIter, TableKey, TableRead, TableReader, TableSchema, TableValue};
pub use table_name::{HistoricalTableName, PendingTableName, VersionedKVName};
pub use write_schema::WriteSchemaTrait;

use crate::errors::Result;

use self::table_name::TableNameTrait;

/// Trait defining the interface for a backend database, which provides multiple tables, each acting as a key-value store.
pub trait DatabaseTrait<TN: TableNameTrait>: Sized + Send + Sync {
    /// Type for identifying tables. Different databases may specify different types.
    /// For example, MDBX uses 'static str, while kvdb-rocksdb uses u32.
    // type TableID: From<TN> + Send + Sync;

    /// Type for collecting write operations.
    /// Each database can specify its own format to accommodate different key format extensions.
    /// For example, MDBX supports subkeys.
    type WriteSchema: WriteSchemaTrait<TN>;

    /// Returns a read-only view of a table.
    ///
    /// # Type Parameters
    ///
    /// * `T`: The schema of the table to be viewed.
    ///
    /// # Returns
    ///
    /// A `Result` containing an implementation of `TableReader` for the specified schema.
    fn view<T: TableSchema<TableName = TN>>(
        self: &Arc<Self>,
    ) -> Result<impl 'static + TableRead<T> + Send + Sync>;

    /// Creates a new WriteSchema instance.
    ///
    /// # Returns
    ///
    /// A new instance of the database's WriteSchema type.
    fn write_schema() -> Self::WriteSchema;

    /// Atomically commits multiple modifications to the database.
    ///
    /// # Parameters
    ///
    /// * `changes`: The WriteSchema containing the modifications to be committed.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of the commit operation.
    fn commit(&self, changes: Self::WriteSchema) -> Result<()>;
}
