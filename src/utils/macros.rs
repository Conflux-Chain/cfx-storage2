#[macro_export]
macro_rules! combine_traits {
    ($trait_name:ident: $($bounds:tt)+) => {
        pub trait $trait_name: $($bounds)+ {}
        impl<T: ?Sized + $($bounds)+> $trait_name for T {}
    };

    ($trait_name:ident: $($bounds:tt)+ where $($where_clause:tt)+) => {
        pub trait $trait_name: $($bounds)+ where $($where_clause)+ {}
        impl<T: ?Sized + $($bounds)+> $trait_name for T where T: $($where_clause)+ {}
    };
}
