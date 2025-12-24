use std::borrow::{Borrow, Cow};

use ethereum_types::H256;
use static_assertions::const_assert_eq;

use crate::errors::{DecResult, DecodeError};

/// A trait for encoding data into a byte sequence, typically for use as keys or values in a KV store.
///
/// # ⚠️ SECURITY & CORRECTNESS WARNING: KEY ENCODING
///
/// When implementing `Encode` for structures used as **Database Keys**, you must ensure the encoding is
/// **canonical** and **prefix-free** to preserve correct lexicographical ordering and avoid collisions.
///
/// ## The "Concatenation Ambiguity" Problem
///
/// If a struct contains multiple fields and the earlier fields are **variable-length** (e.g., `String`, `Vec<u8>`),
/// simple concatenation is **unsafe**.
///
/// ### ❌ Bad Example (Ambiguous)
///
/// ```rust,ignore
/// struct Key {
///     prefix: String,
///     id: u32,
/// }
/// // If we just concat(prefix, id):
/// // Case A: prefix="abc", id=1 (encoded as '1') -> "abc1"
/// // Case B: prefix="ab",  id=c1 (encoded as 'c1') -> "abc1"
/// // Result: Case A and Case B produce the exact same bytes! This causes data corruption.
/// ```
///
/// ### ❌ Bad Example (Sorting Issue)
///
/// Even if they don't collide, variable-length prefixes without length headers destroy ordering:
/// *   Key A: `[0x01, 0x02]` (vec), `0x05` (suffix) -> `01 02 05`
/// *   Key B: `[0x01]` (vec),       `0x03` (suffix) -> `01 03`
/// *   Lexicographical order: B < A.
/// *   Byte order: `01 02 05` < `01 03` (Wait, `0x02` < `0x03`, so A < B).
/// *   **Result:** The database sort order does not match the logical sort order.
///
/// ## ✅ How to Implement Correctly
///
/// 1.  **Fixed-Length Fields:** If a field has a fixed size (e.g., `u64`, `[u8; 32]`, `H256`), simple concatenation is safe.
/// 2.  **Variable-Length Fields (Middle):** If a variable-length field is **NOT** the last field, you **MUST** prepend its length or use a separator.
///     *   *Recommendation:* Prepend the length as a Big-Endian integer (e.g., `u32`).
/// 3.  **Variable-Length Fields (Last):** If the variable-length field is the **very last** component, simple concatenation is usually safe (assuming the prefix is unique).
///
/// ## Example Implementation
///
/// ```rust,ignore
/// impl Encode for MyKey {
///     fn encode(&self) -> Vec<u8> {
///         let mut out = Vec::new();
///         
///         // 1. Variable length field in the middle: MUST add length prefix
///         let name_bytes = self.name.as_bytes();
///         out.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes()); // Length header
///         out.extend_from_slice(name_bytes);
///
///         // 2. Fixed length field: Just append
///         out.extend_from_slice(&self.id.to_be_bytes());
///
///         out
///     }
/// }
/// ```
pub trait Encode: ToOwned {
    fn encode(&self) -> Cow<[u8]>;
    fn encode_owned(input: <Self as ToOwned>::Owned) -> Vec<u8> {
        Self::encode(input.borrow()).into_owned()
    }

    fn encode_cow(input: Cow<Self>) -> Cow<[u8]> {
        match input {
            Cow::Borrowed(x) => Self::encode(x),
            Cow::Owned(x) => Cow::Owned(Self::encode_owned(x)),
        }
    }
}

pub trait FixedLengthEncoded: Encode {
    const LENGTH: usize;
}

pub trait EncodeSubKey: Encode {
    const HAVE_SUBKEY: bool;
    fn encode_subkey(&self) -> (Cow<[u8]>, Cow<[u8]>);
    fn encode_subkey_owned(input: <Self as ToOwned>::Owned) -> (Vec<u8>, Vec<u8>) {
        let (x, y) = Self::encode_subkey(input.borrow());
        (x.into_owned(), y.into_owned())
    }

    fn encode_subkey_cow(input: Cow<Self>) -> (Cow<[u8]>, Cow<[u8]>) {
        match input {
            Cow::Borrowed(x) => Self::encode_subkey(x),
            Cow::Owned(x) => {
                let (a, b) = Self::encode_subkey_owned(x);
                (Cow::Owned(a), Cow::Owned(b))
            }
        }
    }
}

pub trait Decode: ToOwned {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>>;
    fn decode_owned(input: Vec<u8>) -> DecResult<Self::Owned> {
        Ok(Self::decode(input.as_slice())?.into_owned())
    }
    fn decode_cow(input: Cow<[u8]>) -> DecResult<Cow<Self>> {
        match input {
            Cow::Borrowed(x) => Self::decode(x),
            Cow::Owned(x) => Ok(Cow::Owned(Self::decode_owned(x)?)),
        }
    }
}

impl Encode for [u8] {
    fn encode(&self) -> Cow<[u8]> {
        Cow::Borrowed(self)
    }
}

impl Decode for [u8] {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        Ok(Cow::Borrowed(input))
    }

    fn decode_owned(input: Vec<u8>) -> DecResult<Self::Owned> {
        Ok(input)
    }
}

impl Encode for Box<[u8]> {
    fn encode(&self) -> Cow<[u8]> {
        Cow::Borrowed(self)
    }
}

impl Decode for Box<[u8]> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        Ok(Cow::Owned(input.to_owned().into_boxed_slice()))
    }
}

impl Encode for Vec<u8> {
    fn encode(&self) -> Cow<[u8]> {
        Cow::Borrowed(self)
    }
}

impl Decode for Vec<u8> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        Ok(Cow::Owned(input.to_owned()))
    }
}

impl Encode for H256 {
    fn encode(&self) -> Cow<[u8]> {
        Cow::Borrowed(&self.0)
    }
}

impl Decode for H256 {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.len() != H256::len_bytes() {
            return Err(DecodeError::IncorrectLength);
        }

        Ok(Cow::Owned(H256::from_slice(input)))
    }
}

impl Encode for u64 {
    fn encode(&self) -> Cow<[u8]> {
        Cow::Owned(self.to_be_bytes().to_vec())
    }
}

impl FixedLengthEncoded for u64 {
    const LENGTH: usize = std::mem::size_of::<u64>();
}

impl Decode for u64 {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        const BYTES: usize = std::mem::size_of::<u64>();
        if input.len() != (u64::BITS / 8) as usize {
            return Err(DecodeError::IncorrectLength);
        }

        Ok(Cow::Owned(u64::from_be_bytes(input.try_into().unwrap())))
    }
}

impl Encode for [H256; 4] {
    fn encode(&self) -> Cow<[u8]> {
        use std::mem::{align_of, size_of, transmute};
        const_assert_eq!(size_of::<[H256; 4]>(), size_of::<[u8; 128]>());
        const_assert_eq!(align_of::<[H256; 4]>(), align_of::<[u8; 128]>());

        let raw = unsafe { transmute::<_, &[u8; 128]>(self) };
        Cow::Borrowed(raw.as_ref())
    }
}

impl Decode for [H256; 4] {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        const N: usize = H256::len_bytes();
        if input.len() != N * 4 {
            return Err(DecodeError::IncorrectLength);
        }

        let mut raw = [0u8; N * 4];
        raw.copy_from_slice(input);

        let res = unsafe { std::mem::transmute::<[u8; N * 4], [H256; 4]>(raw) };

        Ok(Cow::Owned(res))
    }
}

impl FixedLengthEncoded for H256 {
    const LENGTH: usize = std::mem::size_of::<H256>();
}

#[macro_export]
macro_rules! subkey_not_support {
    ($($t:ty),+) => {
        $(
            impl $crate::backends::serde::EncodeSubKey for $t {
                const HAVE_SUBKEY: bool = false;

                fn encode_subkey(&self) -> (std::borrow::Cow<[u8]>, std::borrow::Cow<[u8]>) {
                    unimplemented!()
                }
            }
        )+
    };
}

subkey_not_support!([u8], H256, u64, Box<[u8]>, Vec<u8>);
