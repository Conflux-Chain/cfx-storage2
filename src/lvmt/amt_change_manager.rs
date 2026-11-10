use std::collections::BTreeMap;

use amt::{AmtParams, SlotChanges};

use super::{
    crypto::PE,
    table_schema::AmtNodes,
    types::{AllocatePosition, AmtId, CurvePoint, CurvePointWithVersion, SLOT_SIZE},
};
use crate::{errors::Result, middlewares::table_schema::KeyValueSnapshotRead};

// Slot indices recorded here are used directly as amt's slot indices.
const _: () = assert!(SLOT_SIZE == amt::SLOT_SIZE);

pub type AmtChange = BTreeMap<u16, SlotChanges>;

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
        pp: &AmtParams<PE>,
    ) -> Result<Vec<(AmtId, CurvePointWithVersion)>> {
        let mut versions = Vec::with_capacity(self.0.len());
        let mut updates = Vec::with_capacity(self.0.len());

        for (key, change) in self.0.iter() {
            let old = db.get(key)?.unwrap_or_default();
            versions.push((*key, old.version + 1));
            let changes = change.iter().map(|(idx, slots)| (*idx as usize, slots));
            updates.push((old.point.projective(), changes));
        }

        // Adds the slot diffs to each commitment and normalizes the results
        // with one batched inversion.
        let points = pp.apply_diffs(updates);

        Ok(versions
            .into_iter()
            .zip(points)
            .map(|((key, version), point)| {
                (
                    key,
                    CurvePointWithVersion {
                        version,
                        point: CurvePoint::Affine(point),
                    },
                )
            })
            .collect())
    }
}
