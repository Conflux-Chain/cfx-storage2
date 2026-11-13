#[cfg(fuzzing)]
pub mod commit_id_schema;

#[cfg(not(fuzzing))]
mod commit_id_schema;
mod key_value_store_bulks;

#[cfg(fuzzing)]
pub mod versioned_flat_key_value;

#[cfg(not(fuzzing))]
mod versioned_flat_key_value;

pub use commit_id_schema::{
    history_number_to_height, CommitID, CommitIDSchema, HistoryNumber, HistoryNumberSchema,
};
pub use key_value_store_bulks::{ChangeKey, KeyValueStoreBulks};
pub use versioned_flat_key_value::{
    confirm_ids_to_history, confirm_maps_to_history, primitives_clear_pending_schema,
    primitives_gc_until_height, primitives_initialize_empty_schema, primitives_recover_schema,
    primitives_verify_no_newer_records, primitives_verify_schema_is_empty, table_schema,
    BitmapValidationError, BootstrapError, HistoryIndexKey, PendingError, PendingKeyValueConfig,
    PushError, RecoveryError, SnapshotReadError, SnapshotView, TreeWithTracker, VersionedStore,
    VersionedStoreCache,
};

#[cfg(test)]
pub use versioned_flat_key_value::{
    clear_dir, clear_dir_then_create, gen_random_commit_id, gen_updates, get_rng_for_test,
};
