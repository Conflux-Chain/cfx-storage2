use crate::{
    ec_algebra::{CanonicalDeserialize, CanonicalSerialize, Pairing},
    AMTParams,
};
use ark_serialize::{SerializationError, Valid, Validate};

impl<PE: Pairing> CanonicalDeserialize for AMTParams<PE> {
    fn deserialize_with_mode<R: ark_serialize::Read>(
        mut reader: R, compress: ark_serialize::Compress, validate: Validate,
    ) -> Result<Self, SerializationError> {
        let basis = CanonicalDeserialize::deserialize_with_mode(
            &mut reader,
            compress,
            validate,
        )?;
        let quotients = CanonicalDeserialize::deserialize_with_mode(
            &mut reader,
            compress,
            validate,
        )?;
        let vanishes = CanonicalDeserialize::deserialize_with_mode(
            &mut reader,
            compress,
            validate,
        )?;
        let g2 = CanonicalDeserialize::deserialize_with_mode(
            &mut reader,
            compress,
            validate,
        )?;
        let high_basis = CanonicalDeserialize::deserialize_with_mode(
            &mut reader,
            compress,
            validate,
        )?;
        let high_g2 = CanonicalDeserialize::deserialize_with_mode(
            &mut reader,
            compress,
            validate,
        )?;
        Ok(AMTParams::new(
            basis, quotients, vanishes, g2, high_basis, high_g2,
        ))
    }
}

impl<PE: Pairing> Valid for AMTParams<PE> {
    fn check(&self) -> Result<(), SerializationError> {
        Valid::check(&self.basis)?;
        Valid::check(&self.quotients)?;
        Valid::check(&self.vanishes)?;
        Valid::check(&self.g2)?;
        Valid::check(&self.high_basis)?;
        Valid::check(&self.high_g2)?;
        Ok(())
    }

    fn batch_check<'a>(
        batch: impl Iterator<Item = &'a Self> + Send,
    ) -> Result<(), SerializationError>
    where Self: 'a {
        let batch: Vec<_> = batch.collect();
        Valid::batch_check(batch.iter().map(|v| &v.basis))?;
        Valid::batch_check(batch.iter().map(|v| &v.quotients))?;
        Valid::batch_check(batch.iter().map(|v| &v.vanishes))?;
        Valid::batch_check(batch.iter().map(|v| &v.g2))?;
        Valid::batch_check(batch.iter().map(|v| &v.high_basis))?;
        Valid::batch_check(batch.iter().map(|v| &v.high_g2))?;
        Ok(())
    }
}
impl<PE: Pairing> ark_serialize::CanonicalSerialize for AMTParams<PE> {
    fn serialize_with_mode<W: ark_serialize::Write>(
        &self, mut writer: W, compress: ark_serialize::Compress,
    ) -> Result<(), SerializationError> {
        CanonicalSerialize::serialize_with_mode(
            &self.basis,
            &mut writer,
            compress,
        )?;
        CanonicalSerialize::serialize_with_mode(
            &self.quotients,
            &mut writer,
            compress,
        )?;
        CanonicalSerialize::serialize_with_mode(
            &self.vanishes,
            &mut writer,
            compress,
        )?;
        CanonicalSerialize::serialize_with_mode(
            &self.g2,
            &mut writer,
            compress,
        )?;
        CanonicalSerialize::serialize_with_mode(
            &self.high_basis,
            &mut writer,
            compress,
        )?;
        CanonicalSerialize::serialize_with_mode(
            &self.high_g2,
            &mut writer,
            compress,
        )?;
        Ok(())
    }

    fn serialized_size(&self, compress: ark_serialize::Compress) -> usize {
        let mut size = 0;
        size += CanonicalSerialize::serialized_size(&self.basis, compress);
        size += CanonicalSerialize::serialized_size(&self.quotients, compress);
        size += CanonicalSerialize::serialized_size(&self.vanishes, compress);
        size += CanonicalSerialize::serialized_size(&self.g2, compress);
        size += CanonicalSerialize::serialized_size(&self.high_basis, compress);
        size += CanonicalSerialize::serialized_size(&self.high_g2, compress);
        size
    }
}
