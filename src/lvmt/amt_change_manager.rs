use std::collections::BTreeMap;

use super::{
    crypto::G1,
    table_schema::AmtNodes,
    types::{batch_normalize, AllocatePosition, AmtId, CurvePointWithVersion, SLOT_SIZE},
};
use crate::{errors::Result, middlewares::table_schema::KeyValueSnapshotRead};

pub type AmtChange = BTreeMap<u16, [bool; SLOT_SIZE]>;

#[derive(Default)]
pub struct AmtChangeManager(BTreeMap<AmtId, AmtChange>);

impl AmtChangeManager {
    pub fn record_with_allocation(&mut self, alloc: AllocatePosition, key: &[u8]) {
        let (amt_id, node_index, slot_index) = alloc.amt_info(key);
        self.record(amt_id, node_index, slot_index);
    }

    pub fn record(&mut self, amt_id: AmtId, node_index: u16, slot_index: u8) {
        let slot = self
            .0
            .entry(amt_id)
            .or_default()
            .entry(node_index)
            .or_default()
            .get_mut(slot_index as usize)
            .unwrap();

        if *slot {
            return;
        }
        *slot = true;

        let mut amt_id = amt_id;
        let (parent_amt_id, node_index) = if let Some(node_index) = amt_id.pop() {
            (amt_id, node_index)
        } else {
            return;
        };

        self.record(amt_id, node_index, (SLOT_SIZE - 1) as u8);
    }

    pub fn compute_amt_changes(
        &self,
        db: &KeyValueSnapshotRead<'_, AmtNodes>,
    ) -> Result<Vec<(AmtId, CurvePointWithVersion)>> {
        let mut result = vec![];

        for (key, value) in self.0.iter() {
            let mut curve_point = db.get(key)?.unwrap_or_default();
            curve_point.point += commitment_diff(value);
            curve_point.version += 1;
            result.push((*key, curve_point));
        }

        let curve_point_iter_mut = result.iter_mut().map(|(_, value)| &mut value.point);
        batch_normalize(curve_point_iter_mut);

        Ok(result)
    }
}

pub fn commitment_diff(change: &AmtChange) -> G1 {
    todo!()
}
