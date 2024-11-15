use super::super::AmtId;
use crate::utils::hash::{amt_id_hash, blake2s};
use ethereum_types::H256;
use std::cmp::Ordering;

#[cfg(test)]
use rand::distributions::{Distribution, Standard};
#[cfg(test)]
use rand::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthChangeItem {
    Flat(Vec<u8>),
    Amt(AmtId),
}

impl AuthChangeItem {
    pub fn hash(&self) -> H256 {
        use AuthChangeItem::*;
        match self {
            Flat(key) => blake2s(key),
            Amt(id) => amt_id_hash(id),
        }
    }
}

impl PartialOrd for AuthChangeItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AuthChangeItem {
    fn cmp(&self, other: &Self) -> Ordering {
        use AuthChangeItem::*;
        use Ordering::*;

        match (self, other) {
            (Flat(x), Flat(y)) => x.cmp(y),
            (Amt(x), Amt(y)) => x.cmp(y),
            (Flat(_), Amt(_)) => Less,
            (Amt(_), Flat(_)) => Greater,
        }
    }
}

#[cfg(test)]
impl Distribution<AuthChangeItem> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> AuthChangeItem {
        let flat: bool = rng.gen();
        if flat {
            let len = rng.gen_range(20..=54);
            let mut key = vec![0u8; len];
            rng.fill_bytes(&mut key);
            AuthChangeItem::Flat(key)
        } else {
            let len = rng.gen_range(0..32);
            let mut amt_id = vec![0u16; len];
            amt_id.fill_with(|| rng.gen());
            AuthChangeItem::Amt(amt_id[..].try_into().unwrap())
        }
    }
}
