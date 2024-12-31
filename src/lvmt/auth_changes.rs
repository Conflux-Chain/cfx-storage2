use std::collections::BTreeMap;

use blake2::Blake2s;
use ethereum_types::H256;

use crate::backends::serde::Encode;
use crate::backends::TableName;
use crate::backends::TableSchema;
use crate::lvmt::types::auth_changes::log2_ceil;
use crate::middlewares::ChangeKey;
use crate::middlewares::CommitID;

use super::types::{
    auth_changes::{MAX_NODE_SIZE, MAX_NODE_SIZE_LOG},
    AmtId, AuthChangeKey, AuthChangeNode, CurvePointWithVersion, LvmtValue,
};
use blake2::Digest;

#[derive(Clone, Copy)]
pub struct AuthChangeTable;
impl TableSchema for AuthChangeTable {
    const NAME: TableName = TableName::AuthNodeChange;

    type Key = ChangeKey<CommitID, AuthChangeKey>;
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
    let size_log = log2_ceil(size);

    let layer_size_log = if key.is_root() {
        let top_size_log = if size_log == 0 {
            0
        } else {
            (size_log - 1) % MAX_NODE_SIZE_LOG + 1
        };
        if top_size_log == size_log {
            let node = AuthChangeNode::from_leaves(items);
            btree.insert(key, node.clone());
            return node;
        }
        top_size_log
    } else {
        assert!(size_log >= MAX_NODE_SIZE_LOG - 1);
        if items.len() <= MAX_NODE_SIZE {
            let node = AuthChangeNode::from_leaves(items);
            btree.insert(key, node.clone());
            return node;
        }
        MAX_NODE_SIZE_LOG
    };
    assert!(layer_size_log <= MAX_NODE_SIZE_LOG);

    let num_subtree = 1usize << layer_size_log;
    let subtree_size_log = size_log - layer_size_log;
    let max_subtree_size = 1usize << subtree_size_log;
    let min_subtree_size = 1usize << (subtree_size_log - 1);

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
    use std::collections::BTreeSet;

    use crate::lvmt::types::test_utils::bytes32_strategy;
    use crate::utils::hash::blake2s_tuple;

    use super::*;
    use proptest::collection::vec;
    use proptest::prelude::*;

    const MAX_TREE_SIZE: usize = 1 << MAX_NODE_SIZE_LOG;
    const MIN_TREE_SIZE: usize = MAX_TREE_SIZE / 2;

    fn leaf_node_sizes() -> impl Strategy<Value = Vec<usize>> {
        use std::iter::{once, repeat};

        (1usize..=12).prop_flat_map(|depth| {
            let size = 1usize << depth;
            let non_full_node_index = 0..size;
            let non_full_node_size = MIN_TREE_SIZE..=MAX_TREE_SIZE;
            (non_full_node_index, non_full_node_size).prop_filter_map(
                "first leaf node cannot be minimum tree",
                move |(node_index, node_size)| {
                    if node_index == 0 && node_size == MIN_TREE_SIZE {
                        return None;
                    }
                    Some(
                        repeat(MAX_TREE_SIZE)
                            .take(node_index)
                            .chain(once(node_size))
                            .chain(repeat(MIN_TREE_SIZE))
                            .take(size)
                            .collect::<Vec<_>>(),
                    )
                },
            )
        })
    }

    fn leaf_nodes() -> impl Strategy<Value = Vec<Vec<H256>>> {
        leaf_node_sizes()
            .prop_flat_map(|x| {
                x.into_iter()
                    .map(|size| vec(bytes32_strategy(), size))
                    .collect::<Vec<_>>()
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

    #[test]
    fn test_strategy_coverage() {
        use proptest::strategy::ValueTree;
        use proptest::test_runner::TestRunner;

        let strategy = leaf_node_sizes().prop_map(|x| x.into_iter().sum::<usize>());

        let mut runner = TestRunner::default();

        let map_length: BTreeSet<_> =
            std::iter::repeat_with(|| strategy.new_tree(&mut runner).unwrap().current())
                .take(1_000_000)
                .collect();

        assert!(*map_length.first().unwrap() == MAX_TREE_SIZE + 1);
        assert_eq!(
            map_length.range((MAX_TREE_SIZE + 1)..=2048).count(),
            2048 - MAX_TREE_SIZE
        );
        assert!(map_length.range(2048..).count() > ((1 << 15) - 2048) * 99 / 100);
    }

    proptest! {
        #[test]
        fn test_small_tree(mut leaves in vec(bytes32_strategy(), 1..=MAX_TREE_SIZE)) {
            leaves.sort();

            let root_node = AuthChangeNode::from_leaves(&leaves);
            let tree = process_dump_items(leaves);

            prop_assert_eq!(tree.len(), 1);
            prop_assert_eq!(&root_node, &tree[&AuthChangeKey::root()]);
        }

        #[test]
        fn test_root_node(nodes in leaf_nodes()) {
            let leaves: Vec<H256> = nodes.iter().flat_map(|x| x.iter()).cloned().collect();
            let root_hash = process_dump_items(leaves)[&AuthChangeKey::root()].hash();

            let mut level_hashes: Vec<_> = nodes.iter().map(|leaves| AuthChangeNode::from_leaves(leaves).hash()).collect();

            while level_hashes.len() > 1 {
                level_hashes = level_hashes.chunks_exact(2).map(|x| blake2s_tuple(&x[0],&x[1])).collect();
            }

            prop_assert_eq!(level_hashes[0], root_hash);
        }


        #[test]
        fn test_leaf_nodes(nodes in leaf_nodes()) {
            let leaves: Vec<H256> = nodes.iter().flat_map(|x| x.iter()).cloned().collect();
            let leaf_nodes_map_actual: BTreeMap<_, _>  = process_dump_items(leaves).into_iter().filter_map(|(_, node)| {
                node.is_leaf().then(||(node.hash(), node))
            }).collect();

            let leaf_nodes_map_expected: BTreeMap<_, _> = nodes.iter().map(|leaves| {
                let node = AuthChangeNode::from_leaves(leaves);
                let hash = node.hash();
                (hash, node)
            }).collect();

            prop_assert_eq!(leaf_nodes_map_actual, leaf_nodes_map_expected);
        }
    }
}
