use std::fmt::{Debug, Display};

pub trait Adapter {
    type Output: Debug + PartialEq + Sized + Eq + Copy + Clone + Send + Sync + Display;
    fn adapt(self) -> Self::Output;
}
