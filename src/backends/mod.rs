pub mod impls;
pub mod serde;
mod table;
mod table_name;
mod write_schema;

pub use impls::in_memory_db::InMemoryDatabase;
pub use table::{TableIter, TableKey, TableRead, TableReader, TableSchema, TableValue};
pub use table_name::{TableName, VersionedKVName};
pub use write_schema::WriteSchemaTrait;

use crate::errors::Result;

/// Trait defining the interface for a backend database, which provides multiple tables, each acting as a key-value store.
pub trait DatabaseTrait: Sized + Send + Sync {
    /// Type for identifying tables. Different databases may specify different types.
    /// For example, MDBX uses 'static str, while kvdb-rocksdb uses u32.
    type TableID: From<TableName> + Send + Sync;

    /// Type for collecting write operations.
    /// Each database can specify its own format to accommodate different key format extensions.
    /// For example, MDBX supports subkeys.
    type WriteSchema: WriteSchemaTrait;

    /// Returns a read-only view of a table.
    ///
    /// # Type Parameters
    ///
    /// * `T`: The schema of the table to be viewed.
    ///
    /// # Returns
    ///
    /// A `Result` containing an implementation of `TableReader` for the specified schema.
    fn view<T: TableSchema>(&self) -> Result<impl '_ + TableRead<T>>;

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
    fn commit<'a>(&mut self, changes: Self::WriteSchema) -> Result<()>;
}
