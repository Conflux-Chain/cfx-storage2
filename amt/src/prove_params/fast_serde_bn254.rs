use crate::{
    ec_algebra::{
        BigInt, BigInteger, CanonicalDeserialize, CanonicalSerialize, Fq, Fq2,
        G1Aff, G2Aff, PrimeField, Read, Write, G2,
    },
    error::Result,
    AMTParams, PowerTau,
};

use ark_bn254::Bn254;
use ark_std::cfg_chunks_mut;
#[cfg(feature = "parallel")]
use rayon::prelude::*;
use std::marker::PhantomData;

const HEADER: [u8; 4] = *b"bamt";
const HEADERPWT: [u8; 4] = *b"ptau";
type PE = Bn254;

pub fn write_amt_params<W: Write>(
    params: &AMTParams<PE>, mut writer: W,
) -> Result<()> {
    writer.write_all(&HEADER)?;

    let degree = ark_std::log2(params.basis.len()) as u8;
    degree.serialize_uncompressed(&mut writer)?;

    let sub_degree = params.quotients.len() as u8;
    sub_degree.serialize_uncompressed(&mut writer)?;

    params.g2.serialize_uncompressed(&mut writer)?;
    params.high_g2.serialize_uncompressed(&mut writer)?;

    for b in &params.basis {
        write_g1(b, &mut writer)?;
    }

    for layer in &params.quotients {
        for b in layer {
            write_g1(b, &mut writer)?;
        }
    }

    for layer in &params.vanishes {
        for b in layer {
            write_g2(b, &mut writer)?;
        }
    }

    for b in &params.high_basis {
        write_g1(b, &mut writer)?;
    }

    Ok(())
}

pub fn read_amt_params<R: Read>(mut reader: R) -> Result<AMTParams<PE>> {
    let header = <[u8; 4]>::deserialize_uncompressed_unchecked(&mut reader)?;
    if header != HEADER {
        return Err("Incorrect format".into());
    }

    let degree = u8::deserialize_uncompressed_unchecked(&mut reader)? as usize;
    let sub_degree =
        u8::deserialize_uncompressed_unchecked(&mut reader)? as usize;

    let g2 = G2::<PE>::deserialize_uncompressed(&mut reader)?;

    let high_g2 = G2::<PE>::deserialize_uncompressed(&mut reader)?;

    let basis = read_amt_g1_line(&mut reader, 1 << degree)?;

    let mut quotients = vec![];
    for _ in 0..sub_degree {
        quotients.push(read_amt_g1_line(&mut reader, 1 << degree)?);
    }

    let mut vanishes = vec![];
    for d in 0..sub_degree {
        vanishes.push(read_amt_g2_line(&mut reader, 1 << (d + 1))?);
    }

    let high_basis = read_amt_g1_line(&mut reader, 1 << degree)?;

    Ok(AMTParams::new(
        basis, quotients, vanishes, g2, high_basis, high_g2,
    ))
}

pub fn write_power_tau<W: Write>(
    params: &PowerTau<PE>, mut writer: W,
) -> Result<()> {
    writer.write_all(&HEADERPWT)?;

    let degree = ark_std::log2(params.g1pp.len()) as u8;
    degree.serialize_uncompressed(&mut writer)?;

    params.high_g2.serialize_uncompressed(&mut writer)?;

    for b in &params.g1pp {
        write_g1(b, &mut writer)?;
    }

    for b in &params.g2pp {
        write_g2(b, &mut writer)?;
    }

    for b in &params.high_g1pp {
        write_g1(b, &mut writer)?;
    }

    Ok(())
}

pub fn read_power_tau<R: Read>(mut reader: R) -> Result<PowerTau<PE>> {
    let header = <[u8; 4]>::deserialize_uncompressed_unchecked(&mut reader)?;
    if header != HEADERPWT {
        return Err("Incorrect format".into());
    }

    let degree = u8::deserialize_uncompressed_unchecked(&mut reader)? as usize;

    let high_g2 = G2::<PE>::deserialize_uncompressed(&mut reader)?;

    let g1pp = read_amt_g1_line(&mut reader, 1 << degree)?;

    let g2pp = read_amt_g2_line(&mut reader, 1 << degree)?;

    let high_g1pp = read_amt_g1_line(&mut reader, 1 << degree)?;

    Ok(PowerTau {
        g1pp,
        g2pp,
        high_g1pp,
        high_g2,
    })
}

#[inline]
pub fn write_g1<W: Write>(b: &G1Aff<PE>, mut writer: W) -> Result<()> {
    if b.infinity {
        return Err("Unsafe params with zero point".into());
    }
    b.x.0.serialize_uncompressed(&mut writer)?;
    b.y.0.serialize_uncompressed(&mut writer)?;
    Ok(())
}

#[inline]
pub fn write_g2<W: Write>(b: &G2Aff<PE>, mut writer: W) -> Result<()> {
    if b.infinity {
        return Err("Unsafe params with zero point".into());
    }
    b.x.c0.0.serialize_uncompressed(&mut writer)?;
    b.x.c1.0.serialize_uncompressed(&mut writer)?;
    b.y.c0.0.serialize_uncompressed(&mut writer)?;
    b.y.c1.0.serialize_uncompressed(&mut writer)?;
    Ok(())
}

const BASE_SIZE: usize =
    <<Fq<PE> as PrimeField>::BigInt as BigInteger>::NUM_LIMBS;
const BASE_BYTES: usize = BASE_SIZE * 8;
fn read_amt_g1_line<R: Read>(
    mut reader: R, length: usize,
) -> Result<Vec<G1Aff<PE>>> {
    let mut buffer = vec![0u8; BASE_BYTES * length * 2];
    reader.read_exact(&mut buffer)?;
    cfg_chunks_mut!(buffer, BASE_BYTES * 2)
        .map(|raw| -> Result<_> {
            let x = read_mont_base(&raw[0..BASE_BYTES])?;
            let y = read_mont_base(&raw[BASE_BYTES..BASE_BYTES * 2])?;
            Ok(if cfg!(test) {
                G1Aff::<PE>::new(x, y)
            } else {
                G1Aff::<PE>::new_unchecked(x, y)
            })
        })
        .collect::<Result<Vec<_>>>()
}

pub fn read_mont_base(raw: &[u8]) -> Result<Fq<PE>> {
    Ok(ark_ff::Fp(
        BigInt::<BASE_SIZE>::deserialize_uncompressed_unchecked(raw)?,
        PhantomData,
    ))
}

fn read_amt_g2_line<R: Read>(
    mut reader: R, length: usize,
) -> Result<Vec<G2Aff<PE>>> {
    let mut buffer = vec![0u8; BASE_BYTES * length * 4];
    reader.read_exact(&mut buffer)?;
    cfg_chunks_mut!(buffer, BASE_BYTES * 4)
        .map(|raw| -> Result<_> {
            let x0 = read_mont_base(&raw[0..BASE_BYTES])?;
            let x1 = read_mont_base(&raw[BASE_BYTES..BASE_BYTES * 2])?;
            let y0 = read_mont_base(&raw[BASE_BYTES * 2..BASE_BYTES * 3])?;
            let y1 = read_mont_base(&raw[BASE_BYTES * 3..])?;

            let x = Fq2::<PE>::new(x0, x1);
            let y = Fq2::<PE>::new(y0, y1);
            Ok(if cfg!(test) {
                G2Aff::<PE>::new(x, y)
            } else {
                G2Aff::<PE>::new_unchecked(x, y)
            })
        })
        .collect::<Result<Vec<_>>>()
}

#[cfg(test)]
mod tests {
    use super::{
        super::tests::{AMT, PP},
        read_amt_params, read_power_tau, write_amt_params, write_power_tau,
    };

    #[test]
    fn test_fast_serde() {
        let mut buffer = Vec::new();
        let writer: &mut Vec<u8> = &mut buffer;
        write_amt_params(&AMT, writer).unwrap();
        let another = read_amt_params(&*buffer).unwrap();
        if another != *AMT {
            panic!("serde inconsistent");
        }
    }

    #[test]
    fn test_fast_serde_power_tau() {
        let mut buffer = Vec::new();
        let writer: &mut Vec<u8> = &mut buffer;
        write_power_tau(&PP, writer).unwrap();
        let another = read_power_tau(&*buffer).unwrap();
        if another != *PP {
            panic!("serde inconsistent");
        }
    }
}
