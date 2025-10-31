use std::collections::BTreeSet;
use cfx_storage2::middlewares::versioned_flat_key_value::history_indices::version_range::bitmap::BITMAP_MAX_INDEX;

#[derive(Debug, Clone)]
pub struct BitmapOracle {
    indices: BTreeSet<u16>,
}

impl BitmapOracle {
    pub fn new(mut vec: Vec<u16>) -> Self {
        vec.push(0);
        Self {
            indices: vec.into_iter().filter(|&i| i <= BITMAP_MAX_INDEX).collect(),
        }
    }

    pub fn max_bit(&self) -> u64 {
        *self.indices.iter().next_back().unwrap() as u64
    }

    pub fn last_le(&self, upper_bound: u64) -> u64 {
        *self.indices
            .iter()
            .rev()
            .find(|&&i| (i as u64) <= upper_bound)
            .unwrap() as u64
    }

    pub fn collect_le(&self, upper_bound: u64) -> Vec<u64> {
        self.indices
            .iter()
            .filter(|&&i| (i as u64) <= upper_bound)
            .map(|&i| i as u64)
            .collect()
    }

    pub fn count_ones(&self) -> usize {
        self.indices.len()
    }

    pub fn set_unchecked(&mut self, index: u64) -> bool {
        if index > (BITMAP_MAX_INDEX as u64) {
            return false;
        }
        self.indices.insert(index as u16);
        true
    }

    pub fn to_vec(&self) -> Vec<u16> {
        self.indices.iter().cloned().collect()
    }
}