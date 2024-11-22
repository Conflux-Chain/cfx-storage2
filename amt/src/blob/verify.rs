use std::path::Path;

#[cfg(not(feature = "bls12-381"))]
use ark_bn254::Bn254;
use ark_ec::pairing::Pairing;


use crate::verify_params::AMTVerifyParams;

pub struct VerifierParams<
    PE: Pairing,
    const COSET_N: usize,
    const LOG_COL: usize,
    const LOG_ROW: usize,
> {
    pub amt_list: [AMTVerifyParams<PE>; COSET_N],
}

impl<
        PE: Pairing,
        const COSET_N: usize,
        const LOG_COL: usize,
        const LOG_ROW: usize,
    > VerifierParams<PE, COSET_N, LOG_COL, LOG_ROW>
{
    pub fn new(amt_list: [AMTVerifyParams<PE>; COSET_N]) -> Self {
        Self { amt_list }
    }

    fn from_builder<F: Fn(usize) -> AMTVerifyParams<PE>>(f: F) -> Self {
        let mut amt_list = vec![];
        for coset in 0..COSET_N {
            let amt = f(coset);
            amt_list.push(amt);
        }

        let amt_list = match amt_list.try_into() {
            Ok(x) => x,
            Err(_) => unreachable!(),
        };

        Self { amt_list }
    }

    pub fn from_dir(dir: impl AsRef<Path> + Clone) -> Self {
        Self::from_builder(|coset| {
            AMTVerifyParams::from_dir(
                dir.clone(),
                LOG_COL + LOG_ROW,
                LOG_ROW,
                coset,
            )
        })
    }
}

#[cfg(not(feature = "bls12-381"))]
impl<const COSET_N: usize, const LOG_COL: usize, const LOG_ROW: usize>
    VerifierParams<Bn254, COSET_N, LOG_COL, LOG_ROW>
{
    pub fn from_dir_mont(dir: impl AsRef<Path> + Clone) -> Self {
        Self::from_builder(|coset| {
            AMTVerifyParams::from_dir_mont(
                dir.clone(),
                LOG_COL + LOG_ROW,
                LOG_ROW,
                coset,
            )
        })
    }
}
