mod bootstrap;
mod format;
mod recovery;
mod writer;

pub use bootstrap::BootstrapError;
pub use recovery::RecoveryError;

pub use bootstrap::primitives::{
    clear_pending_schema as primitives_clear_pending_schema,
    initialize_empty_schema as primitives_initialize_empty_schema,
    verify_schema_is_empty as primitives_verify_schema_is_empty,
};
pub use recovery::primitives::recover_schema as primitives_recover_schema;

pub(super) use writer::{
    log_add_non_root_node, log_add_root, log_change_root, log_discard, log_make_pivot,
};

use self::format::{
    ModificationId, SnapshotId, SnapshotValue, SnapshotsTable, WalKeySpecificPart, WalTable,
    WalValue,
};
use crate::{
    backends::{
        serde::{Decode, Encode, EncodeSubKey, FixedLengthEncoded},
        DatabaseTrait, PendingTableName, SeekKey, TableRead, TableSchema, WriteSchemaTrait,
    },
    middlewares::versioned_flat_key_value::pending_part::persistence::format::{
        SnapshotKey, WalKey,
    },
};

use super::{
    pending_schema::{PendingKeyValueSchema, RecoverMap, RecoverRecord},
    tree::Tree,
    tree_with_tracker::TreeWithTracker,
};

use crate::{
    errors::{DecResult, DecodeError, Result},
    types::ValueEntry,
};

use crate::subkey_not_support;

/// Tracks the sequence of state modifications to be logged for recovery.
///
/// This struct maintains the necessary counters to correctly sequence operations
/// for the write-ahead log (WAL). It ensures that each modification can be uniquely
/// identified relative to a base snapshot, which is crucial for reconstructing the
/// in-memory `tree` after a restart. It does not track completion of I/O, but rather
/// what the identity of the next submitted operation should be.
#[derive(Debug)]
pub struct PersistenceTracker {
    /// The identifier for the most recent snapshot that was created.
    /// Subsequent modifications are logged as a delta from this snapshot.
    snapshot_id: SnapshotId,
    /// The sequence ID to be assigned to the next modification record. This counter
    /// increments for each operation logged after the last snapshot.
    next_modification_id: ModificationId,
}

impl PersistenceTracker {
    /// Advances the tracker to a new snapshot.
    pub fn advance_to_next_snapshot(&mut self) {
        self.snapshot_id.0 += 1;
        self.next_modification_id = ModificationId(0);
    }

    /// Increments the modification ID counter for the next operation.
    pub fn advance_to_next_modification(&mut self) {
        self.next_modification_id.0 += 1;
    }
}
