mod allocation;
pub mod auth_changes;
mod curve_point;
mod lvmt_value;
mod node_id;

pub use allocation::{AllocatePosition, AllocationKeyInfo, KEY_SLOT_SIZE, SLOT_SIZE};
pub use auth_changes::{AuthChangeKey, AuthChangeNode};
pub use curve_point::{batch_normalize, CurvePointWithVersion};
pub use lvmt_value::LvmtValue;
pub use node_id::{compute_amt_node_id, AmtId, AmtNodeId};

use crate::subkey_not_support;
subkey_not_support!(AmtId, AuthChangeKey);

#[cfg(test)]
pub mod test_utils {
    use std::fmt::Debug;

    use ethereum_types::H256;
    use proptest::collection::vec;
    use proptest::prelude::*;

    use crate::backends::serde::{Decode, Encode};

    pub fn key_strategy() -> impl Strategy<Value = Box<[u8]>> {
        vec(0u8..=255, 0..128).prop_map(|x| x.into_boxed_slice())
    }

    pub fn version_strategy() -> impl Strategy<Value = u64> {
        prop_oneof![0u64..10000, 0u64..(1 << 40)]
    }

    pub fn value_strategy() -> impl Strategy<Value = Option<Box<[u8]>>> {
        prop_oneof![
            vec(0u8..=255, 0..128).prop_map(|x| Some(x.into_boxed_slice())),
            Just(None),
        ]
    }

    pub fn bytes32_strategy() -> impl Strategy<Value = H256> {
        any::<[u8; 32]>().prop_map(H256)
    }

    pub fn test_serde<T: Encode + Decode + Debug + Eq + ToOwned<Owned = T>>(data: T) {
        let encoded = data.encode();
        let decoded = T::decode(&encoded)
            .expect("Decode should succeed")
            .into_owned();
        assert_eq!(&data, &decoded);

        let encoded_again = decoded.encode();

        assert_eq!(encoded, encoded_again);
    }

    pub fn test_serde_keep_order<T: Encode + Decode + Debug + Eq + ToOwned<Owned = T> + Ord>(
        a: T,
        b: T,
    ) {
        let ord = a.cmp(&b);
        let raw_ord = a.encode().cmp(&b.encode());

        assert_eq!(ord, raw_ord);
    }
}
