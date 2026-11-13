use arbitrary::{Arbitrary, Result, Unstructured};
use cfx_storage2::backends::serde::{Decode, Encode};
use cfx_storage2::errors::DecResult;
use std::borrow::Cow;

#[derive(Debug, Clone, PartialEq, Hash, PartialOrd, Eq, Ord)]
pub struct BoundedVec(pub Vec<u8>);

impl<'a> Arbitrary<'a> for BoundedVec {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        let len = u.int_in_range(0..=32)?;
        let bytes = u.bytes(len)?.to_vec();
        Ok(BoundedVec(bytes))
    }
}

impl Encode for BoundedVec {
    fn encode(&self) -> Cow<[u8]> {
        self.0.encode()
    }
}

impl Decode for BoundedVec {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        Ok(Cow::Owned(BoundedVec(Vec::<u8>::decode(input)?.into_owned())))
    }
}

cfx_storage2::subkey_not_support!(BoundedVec);