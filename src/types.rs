#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueEntry<T> {
    Value(T),
    Deleted,
}

impl<T> From<ValueEntry<T>> for Option<T> {
    fn from(value: ValueEntry<T>) -> Self {
        value.into_option()
    }
}

impl<T> ValueEntry<T> {
    pub fn from_option(value: Option<T>) -> Self {
        match value {
            Some(v) => ValueEntry::Value(v),
            None => ValueEntry::Deleted,
        }
    }
    pub fn into_option(self) -> Option<T> {
        match self {
            ValueEntry::Value(v) => Some(v),
            ValueEntry::Deleted => None,
        }
    }

    pub fn as_opt_ref(&self) -> Option<&T> {
        match self {
            ValueEntry::Value(v) => Some(v),
            ValueEntry::Deleted => None,
        }
    }
}

impl<T: Clone> ValueEntry<T> {
    pub fn to_option(&self) -> Option<T> {
        match self {
            ValueEntry::Value(v) => Some(v.clone()),
            ValueEntry::Deleted => None,
        }
    }
}
