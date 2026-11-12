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

    /// The new commitment and version of every AMT changed in this commit.
    ///
    /// # Invariants relied on by historical proofs
    ///
    /// Proofs at a historical version correct a checkpoint of the root leaf values
    /// by counting AMT changes (`v(Q) = v(C) ∓ #changes`), which holds because:
    ///
    /// 1. An AMT's version goes up by exactly 1 in each commit that changes it, and
    ///    only then: `self.0` is keyed by `AmtId`, so an AMT appears once per
    ///    commit.
    /// 2. Slot 5 of leaf `i` of AMT `a` changes by exactly 1 in the same commits
    ///    (`record` sets it once per commit), so it equals the version of AMT
    ///    `a ‖ i`.
    /// 3. `AmtNodes` entries are never deleted or reset (`LvmtStore::commit` only
    ///    writes `Some`), so `unwrap_or_default` below only applies to a new AMT.
    ///
    /// Checked by `test_lvmt_root_leaf_version_counting`. Anything that GCs or
    /// recycles AMT nodes must keep these, or change the checkpoint correction.
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
