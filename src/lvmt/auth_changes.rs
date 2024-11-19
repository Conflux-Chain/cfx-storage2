use std::collections::BTreeMap;

use blake2::Blake2s;
use ethereum_types::H256;

use crate::backends::serde::Encode;
use crate::backends::TableName;
use crate::backends::TableSchema;

use super::types::{
    auth_changes::{tree_height, MAX_NODE_SIZE, MAX_NODE_SIZE_LOG},
    AmtId, AuthChangeKey, AuthChangeNode, CurvePointWithVersion, LvmtValue,
};
use blake2::Digest;

#[derive(Clone, Copy)]
pub struct AuthChangeTable;
impl TableSchema for AuthChangeTable {
    const NAME: TableName = TableName::AuthNodeChange;

    type Key = AuthChangeKey;
    type Value = AuthChangeNode;
}

const KEY_VALUE_CHANGE_FLAG: u8 = 0;
const AMT_CHANGE_FLAG: u8 = 1;

pub fn amt_change_hash(amt_id: &AmtId, curve_point: &CurvePointWithVersion) -> H256 {
    let mut hasher = Blake2s::new();
    hasher.update([AMT_CHANGE_FLAG]);
    hasher.update(amt_id.encode().as_ref());
    hasher.update(curve_point.encode().as_ref());
    H256(hasher.finalize().into())
}

pub fn key_value_hash(key: &[u8], value: &LvmtValue) -> H256 {
    let mut hasher = Blake2s::new();
    hasher.update([KEY_VALUE_CHANGE_FLAG]);
    hasher.update((key.len() as u32).to_le_bytes());
    hasher.update(key.encode().as_ref());
    hasher.update(value.encode().as_ref());
    H256(hasher.finalize().into())
}

pub fn process_dump_items(mut hashes: Vec<H256>) -> BTreeMap<AuthChangeKey, AuthChangeNode> {
    hashes.sort_unstable();

    let mut map = BTreeMap::new();
    process_subtree(&hashes, AuthChangeKey::root(), &mut map);
    map
}

fn process_subtree(
    items: &[H256],
    key: AuthChangeKey,
    btree: &mut BTreeMap<AuthChangeKey, AuthChangeNode>,
) -> AuthChangeNode {
    let size = items.len();
    let height = tree_height(size);

    let layer_height = if key.is_root() {
        let top_height = (height - 1) % MAX_NODE_SIZE_LOG + 1;
        if top_height == height {
            let node = AuthChangeNode::from_leaves(items);
            btree.insert(key, node.clone());
            return node;
        }
        top_height
    } else {
        if items.len() <= MAX_NODE_SIZE {
            let node = AuthChangeNode::from_leaves(items);
            btree.insert(key, node.clone());
            return node;
        }
        MAX_NODE_SIZE_LOG
    };
    assert!(layer_height <= MAX_NODE_SIZE_LOG);

    let num_subtree = 1usize << layer_height;
    let subtree_height = height - layer_height;
    let max_subtree_size = 1usize << subtree_height;
    let min_subtree_size = 1usize << (subtree_height - 1);

    let mut items = items;
    let mut processed_nodes = vec![];
    let mut ticks = vec![];
    let mut max_shared_prefix_len = 0;

    for i in 0..num_subtree {
        let subtree_size = std::cmp::min(
            max_subtree_size,
            items.len() - min_subtree_size * (num_subtree - i - 1),
        );
        assert!(subtree_size >= min_subtree_size);

        let subtree;
        (subtree, items) = items.split_at(subtree_size);

        if !items.is_empty() {
            ticks.push(items[0]);

            let shared_prefix_len = shared_prefix_len(&subtree[subtree_size - 1].0, &items[0].0);
            if shared_prefix_len > max_shared_prefix_len {
                max_shared_prefix_len = shared_prefix_len;
            }
        }

        let node = process_subtree(subtree, key.child(i), btree);
        processed_nodes.push(node);
    }

    let node = AuthChangeNode::from_nodes(&processed_nodes, ticks, max_shared_prefix_len);

    btree.insert(key, node.clone());
    node
}

fn shared_prefix_len<T: PartialEq>(a: &[T], b: &[T]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

#[cfg(test)]
mod tests {
    use crate::lvmt::types::test_utils::bytes32_strategy;

    use super::*;
    use proptest::collection::vec;
    use proptest::prelude::*;

    fn leaf_node_size() -> impl Strategy<Value = usize> {
        (1usize..=12).prop_map(|h| 1usize << h)
    }

    fn tmp() -> impl Strategy<Value = Vec<Vec<H256>>> {
        leaf_node_size()
            .prop_flat_map(|size| {
                let non_full_node_index = 0..size;
                let non_full_node_size = 4usize..=8;
                (non_full_node_index, non_full_node_size).prop_flat_map(
                    move |(node_index, node_size)| {
                        let size_map = |i| {
                            if i < node_index {
                                8
                            } else if i > node_index {
                                4
                            } else {
                                node_size
                            }
                        };
                        (0..size)
                            .map(|i| vec(bytes32_strategy(), size_map(i)))
                            .collect::<Vec<_>>()
                    },
                )
            })
            .prop_map(|mut x| {
                let pointers: Vec<_> = x.iter_mut().flat_map(|y| y.iter_mut()).collect();
                let mut cloned_hash: Vec<H256> = pointers.iter().map(|x| **x).collect();
                cloned_hash.sort();
                pointers
                    .into_iter()
                    .zip(cloned_hash)
                    .for_each(|(pointer, value)| *pointer = value);
                x
            })
    }
}
