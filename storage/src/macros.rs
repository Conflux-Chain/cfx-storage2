#[macro_export]
macro_rules! define_key_value_schema {
    ($type:ident, table: $name:ident, key: $key:ty, value: $value:ty $(,)?) => {
        #[derive(Clone, Copy, Debug)]
        pub struct $type;

        impl VersionedKeyValueSchema for $type {
            const NAME: VersionedKVName = VersionedKVName::$name;

            type Key = $key;
            type Value = $value;
        }
    };
}
