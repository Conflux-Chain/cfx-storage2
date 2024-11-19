use super::types::{AllocationKeyInfo, AmtId, AmtNodeId, CurvePointWithVersion, LvmtValue};
use crate::define_key_value_schema;
use crate::{backends::VersionedKVName, middlewares::table_schema::VersionedKeyValueSchema};

define_key_value_schema! {
    FlatKeyValue,
    table: FlatKV,
    key: Box<[u8]>,
    value: LvmtValue,
}

define_key_value_schema! {
    AmtNodes,
    table: AmtNode,
    key: AmtId,
    value: CurvePointWithVersion,
}

define_key_value_schema! {
    SlotAllocations,
    table: SlotAllocation,
    key: AmtNodeId,
    value: AllocationKeyInfo,
}
