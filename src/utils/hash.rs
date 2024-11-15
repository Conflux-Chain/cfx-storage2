use blake2::{Blake2s256, Digest};
use ethereum_types::H256;

pub fn blake2s(input: &[u8]) -> H256 {
    let mut hasher = Blake2s256::new();
    hasher.update(input);
    H256(hasher.finalize().into())
}

pub fn amt_id_hash(input: &[u16]) -> H256 {
    let mut hasher = Blake2s256::new();
    for item in input {
        hasher.update(item.to_be_bytes());
    }
    H256(hasher.finalize().into())
}

pub fn blake2s_tuple(a: &H256, b: &H256) -> H256 {
    let mut hasher = Blake2s256::new();
    hasher.update(&a.0);
    hasher.update(&b.0);
    H256(hasher.finalize().into())
}
