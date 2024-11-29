use std::{fs::File, io::BufReader, ops::Mul, path::Path};

use super::super::{
    ec_algebra::{
        k_adicity, CanonicalDeserialize, CanonicalSerialize, CurveGroup, EvaluationDomain, Fr,
        FrInt, G1Aff, G2Aff, Pairing, Radix2EvaluationDomain, Zero, G1, G2,
    },
    error,
    power_tau::PowerTau,
    prove_params::SLOT_SIZE_MINUS_1,
    utils::{amtp_file_name, index_reverse},
};
use super::AmtParams;

#[cfg(feature = "bls12-381")]
use ark_bls12_381::Bls12_381;

#[cfg(not(feature = "bls12-381"))]
use ark_bn254::Bn254;

use ark_std::cfg_iter_mut;
#[cfg(feature = "parallel")]
use rayon::prelude::*;

pub enum CreateMode {
    /// neither AMT nor PP can be created
    Neither,
    /// AMT can be created from PP, but PP cannot be created
    AmtOnly,
    /// both AMT and PP can be created
    Both,
}

impl CreateMode {
    fn amt_can_be_created(&self) -> bool {
        match self {
            CreateMode::Neither => false,
            CreateMode::AmtOnly => true,
            CreateMode::Both => true,
        }
    }

    fn pp_can_be_created(&self) -> bool {
        match self {
            CreateMode::Neither => false,
            CreateMode::AmtOnly => false,
            CreateMode::Both => true,
        }
    }
}

#[cfg(not(feature = "bls12-381"))]
impl AmtParams<Bn254> {
    #[instrument(skip_all, name = "load_amt_params", level = 2, parent = None, fields(depth=depth, prove_depth=prove_depth))]
    pub fn from_dir_mont(
        dir: impl AsRef<Path>,
        depth: usize,
        prove_depth: usize,
        create_mode: CreateMode,
        pp: Option<&PowerTau<Bn254>>,
    ) -> Self {
        debug!(
            depth = depth,
            prove_depth = prove_depth,
            "Load AMT params (mont format)"
        );
        let file_name = amtp_file_name::<Bn254>(depth, prove_depth, true);
        let path = dir.as_ref().join(file_name);

        match Self::load_cached_mont(&path) {
            Ok(loaded) => {
                return loaded;
            }
            Err(e) => {
                info!(?path, error = ?e, "Fail to load AMT params (mont format)");
            }
        }

        if !create_mode.amt_can_be_created() {
            panic!("Fail to load amt params in mont from {:?}", path);
        }

        info!("Recover from unmont format");

        let params = Self::from_dir(dir, depth, prove_depth, create_mode, pp);

        let writer = File::create(&*path).unwrap();

        info!(file = ?path, "Save generated AMT params (mont format)");
        super::fast_serde_bn254::write_amt_params(&params, writer).unwrap();

        params
    }

    fn load_cached_mont(file: impl AsRef<Path>) -> Result<Self, error::Error> {
        let buffer = BufReader::new(File::open(file)?);
        super::fast_serde_bn254::read_amt_params(buffer)
    }
}

#[cfg(feature = "bls12-381")]
impl AmtParams<Bls12_381> {
    #[instrument(skip_all, name = "load_amt_params", level = 2, parent = None, fields(depth=depth, prove_depth=prove_depth))]
    pub fn from_dir_mont(
        dir: impl AsRef<Path>,
        depth: usize,
        prove_depth: usize,
        create_mode: CreateMode,
        pp: Option<&PowerTau<Bls12_381>>,
    ) -> Self {
        debug!(
            depth = depth,
            prove_depth = prove_depth,
            "Load AMT params (mont format)"
        );
        let file_name = amtp_file_name::<Bls12_381>(depth, prove_depth, true);
        let path = dir.as_ref().join(file_name);

        match Self::load_cached_mont(&path) {
            Ok(loaded) => {
                return loaded;
            }
            Err(e) => {
                info!(?path, error = ?e, "Fail to load AMT params (mont format)");
            }
        }

        if !create_mode.amt_can_be_created() {
            panic!("Fail to load amt params in mont from {:?}", path);
        }

        info!("Recover from unmont format");

        let params = Self::from_dir(dir, depth, prove_depth, create_mode, pp);

        let writer = File::create(&*path).unwrap();

        info!(file = ?path, "Save generated AMT params (mont format)");
        super::fast_serde_bls12_381::write_amt_params(&params, writer).unwrap();

        params
    }

    fn load_cached_mont(file: impl AsRef<Path>) -> Result<Self, error::Error> {
        let buffer = BufReader::new(File::open(file)?);
        super::fast_serde_bls12_381::read_amt_params(buffer)
    }
}

impl<PE: Pairing> AmtParams<PE> {
    #[instrument(skip_all, name = "load_amt_params", level = 2, parent = None, fields(depth=depth, prove_depth=prove_depth))]
    pub fn from_dir(
        dir: impl AsRef<Path>,
        depth: usize,
        prove_depth: usize,
        create_mode: CreateMode,
        pp: Option<&PowerTau<PE>>,
    ) -> Self {
        debug!(depth, prove_depth, "Load AMT params (unmont format)");

        let file_name = amtp_file_name::<PE>(depth, prove_depth, false);
        let path = dir.as_ref().join(file_name);

        if let Ok(params) = Self::load_cached(&path) {
            return params;
        }

        info!(?path, "Fail to load AMT params (unmont format)");

        if !create_mode.amt_can_be_created() {
            panic!("Fail to load amt params from {:?}", path);
        }

        let pp = if let Some(pp) = pp {
            info!("Recover AMT parameters with specified pp");
            pp.clone()
        } else {
            info!("Recover AMT parameters by loading default pp");
            PowerTau::<PE>::from_dir(dir, depth, create_mode.pp_can_be_created())
        };

        let params = Self::from_pp(pp, prove_depth);
        let buffer = File::create(&path).unwrap();

        info!(file = ?path, "Save generated AMT params (unmont format)");
        params.serialize_uncompressed(&buffer).unwrap();

        params
    }

    fn load_cached(file: impl AsRef<Path>) -> Result<Self, error::Error> {
        let mut buffer = BufReader::new(File::open(file)?);
        Ok(CanonicalDeserialize::deserialize_uncompressed_unchecked(
            &mut buffer,
        )?)
    }

    pub fn is_empty(&self) -> bool {
        self.basis.is_empty()
    }

    pub fn len(&self) -> usize {
        self.basis.len()
    }

    fn enact<T: CurveGroup>(input: Vec<T>) -> Vec<<T as CurveGroup>::Affine> {
        let mut affine = CurveGroup::normalize_batch(input.as_slice());
        index_reverse(&mut affine);
        affine
    }

    pub(super) fn gen_basis_power_by_basis(
        basis: &[G1Aff<PE>],
    ) -> Vec<[G1Aff<PE>; SLOT_SIZE_MINUS_1]> {
        let mut vec_fr = vec![];
        let mut fr_int = FrInt::<PE>::from(1u64);
        for _ in 0..SLOT_SIZE_MINUS_1 {
            fr_int <<= 40;
            vec_fr.push(Fr::<PE>::from(fr_int));
        }

        // todo: mul_bigint
        // PE = Bn254, affine.into_group(), group.mul_bigint() is Ok
        // But how about PE: Pairing?
        let basis_power: Vec<G1<PE>> = basis
            .iter()
            .flat_map(|b| {
                let point: G1<PE> = (*b).into();
                let vec: Vec<G1<PE>> = vec_fr.iter().map(|fr| point.mul(fr)).collect();
                vec
            })
            .collect();
        let basis_power = CurveGroup::normalize_batch(basis_power.as_slice());
        let basis_power = basis_power
            .chunks_exact(SLOT_SIZE_MINUS_1)
            .map(|slice| {
                slice
                    .try_into()
                    .unwrap_or_else(|_| panic!("Slice length must be {}", SLOT_SIZE_MINUS_1))
            })
            .collect();
        basis_power
    }

    pub fn from_pp(pp: PowerTau<PE>, prove_depth: usize) -> Self {
        info!("Generate AMT params from powers of tau");

        let (g1pp, g2pp) = pp.into_projective();

        assert_eq!(g1pp.len(), g2pp.len());
        assert!(g1pp.len().is_power_of_two());
        let length = g1pp.len();
        assert!(length >= 1 << prove_depth);

        let fft_domain = Radix2EvaluationDomain::<Fr<PE>>::new(length).unwrap();

        let basis: Vec<G1Aff<PE>> = Self::enact(Self::gen_basis(&g1pp[..], &fft_domain));
        let quotients: Vec<Vec<G1Aff<PE>>> = (1..=prove_depth)
            .map(|d| Self::enact(Self::gen_quotients(&g1pp[..], &fft_domain, d)))
            .collect();
        let vanishes: Vec<Vec<G2Aff<PE>>> = (1..=prove_depth)
            .map(|d| Self::enact(Self::gen_vanishes(&g2pp[..], d)))
            .collect();

        let basis_power = Self::gen_basis_power_by_basis(&basis);

        Self::new(basis, quotients, vanishes, g2pp[0], basis_power)
    }

    fn gen_basis(g1pp: &[G1<PE>], fft_domain: &Radix2EvaluationDomain<Fr<PE>>) -> Vec<G1<PE>> {
        debug!("Generate basis");
        fft_domain.ifft(g1pp)
    }

    fn gen_quotients(
        g1pp: &[G1<PE>],
        fft_domain: &Radix2EvaluationDomain<Fr<PE>>,
        depth: usize,
    ) -> Vec<G1<PE>> {
        debug!(depth, "Generate quotients");

        assert!(g1pp.len() <= 1 << 32);

        let length = g1pp.len();
        let max_depth = k_adicity(2, length as u64) as usize;

        assert_eq!(1 << max_depth, length);
        assert!(max_depth >= depth);
        assert!(depth >= 1);

        let mut coeff = vec![G1::<PE>::zero(); length];
        let max_coeff = 1usize << (max_depth - depth);
        for i in 1..=max_coeff {
            coeff[i] = g1pp[max_coeff - i];
        }

        let mut answer = fft_domain.fft(&coeff);

        cfg_iter_mut!(answer, 1024).for_each(|val: &mut G1<PE>| *val *= fft_domain.size_inv);
        answer
    }

    fn gen_vanishes(g2pp: &[G2<PE>], depth: usize) -> Vec<G2<PE>> {
        debug!(depth, "Generate vanishes");

        assert!(g2pp.len() <= 1 << 32);

        let length = g2pp.len();
        let max_depth = k_adicity(2, length as u64) as usize;

        assert_eq!(1 << max_depth, length);
        assert!(max_depth >= depth);
        assert!(depth >= 1);

        let height = max_depth - depth;
        let step = 1 << height;
        let mut fft_domain = Radix2EvaluationDomain::<Fr<PE>>::new(1 << depth).unwrap();

        let mut coeff = vec![G2::<PE>::zero(); 1 << depth];

        coeff[0] = g2pp[length - step];
        for i in 1..length / step {
            coeff[i] = g2pp[(i - 1) * step]
        }

        std::mem::swap(&mut fft_domain.group_gen, &mut fft_domain.group_gen_inv);
        fft_domain.fft(&coeff)
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::ec_algebra::{
        EvaluationDomain, Field, Fr, One, Pairing, VariableBaseMSM, Zero, G1, G2,
    };
    use super::super::tests::{TestParams, DOMAIN, G1PP, G2PP, PE, PP, TEST_LENGTH, TEST_LEVEL, W};

    fn simple_gen_basis(index: usize) -> G1<PE> {
        let mut points = vec![Fr::<PE>::zero(); TEST_LENGTH];
        points[index] = Fr::<PE>::one();

        let coeff = DOMAIN.ifft(&points);
        G1::<PE>::msm(&PP.g1pp, &coeff[..]).unwrap()
    }

    #[test]
    #[allow(clippy::needless_range_loop)]
    fn test_gen_basis() {
        let indents = TestParams::gen_basis(&G1PP, &*DOMAIN);
        for t in 0..TEST_LENGTH {
            assert_eq!(indents[t], simple_gen_basis(t))
        }
    }

    fn simple_gen_quotinents(index: usize, depth: usize) -> G1<PE> {
        let size = TEST_LENGTH / (1 << depth);
        (0..size)
            .rev()
            .map(|j| W.pow([(index * j) as u64]))
            .zip(PP.g1pp[0..size].iter())
            .map(|(exp, base)| *base * exp)
            .sum::<G1<PE>>()
            * DOMAIN.size_inv
            * W.pow([index as u64])
    }

    #[test]
    #[allow(clippy::needless_range_loop)]
    fn test_gen_quotients() {
        for depth in (1..=TEST_LEVEL).rev() {
            let quotients = TestParams::gen_quotients(&G1PP, &DOMAIN, depth);
            for t in 0..TEST_LENGTH {
                assert_eq!(quotients[t], simple_gen_quotinents(t, depth));
            }
        }
    }

    fn simple_gen_vanishes(index: usize, depth: usize) -> G2<PE> {
        let step = TEST_LENGTH / (1 << depth);
        let size = 1 << depth;
        (0..size)
            .rev()
            .map(|j| W.pow([(index * step * j) as u64]))
            .zip(PP.g2pp.iter().step_by(step))
            .map(|(exp, base)| *base * exp)
            .sum()
    }

    #[test]
    fn test_gen_vanishes() {
        for depth in (1..=TEST_LEVEL).rev() {
            let vanishes = TestParams::gen_vanishes(&G2PP, depth);
            for t in 0..TEST_LENGTH {
                assert_eq!(vanishes[t % (1 << depth)], simple_gen_vanishes(t, depth));
            }
        }
    }

    #[test]
    fn test_simple_gen_params() {
        for depth in (1..=TEST_LEVEL).rev() {
            for t in 0..TEST_LENGTH {
                assert_eq!(
                    PE::pairing(simple_gen_basis(t), G2PP[0]),
                    PE::pairing(
                        simple_gen_quotinents(t, depth),
                        simple_gen_vanishes(t, depth)
                    )
                );
            }
        }
    }

    #[test]
    fn test_gen_params() {
        let basis = TestParams::gen_basis(&G1PP, &DOMAIN);
        for depth in (1..=TEST_LEVEL).rev() {
            let prove_data = TestParams::gen_quotients(&G1PP, &DOMAIN, depth);
            let verify_data = TestParams::gen_vanishes(&G2PP, depth);
            for t in 0..TEST_LENGTH {
                assert_eq!(
                    PE::pairing(basis[t], G2PP[0]),
                    PE::pairing(prove_data[t], verify_data[t % (1 << depth)])
                );
            }
        }
    }
}
