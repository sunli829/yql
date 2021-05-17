mod array;
mod array_ext;
mod bitmap;
mod builder;
mod data_type;
mod null_array;
mod primitive_array;
mod scalar;
mod string_array;

pub mod compute;

pub use array::{Array, ArrayRef};
pub use array_ext::ArrayExt;
pub use builder::ArrayBuilder;
pub use data_type::DataType;
pub use null_array::NullArray;
pub use primitive_array::{
    BooleanType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    PrimitiveArray, PrimitiveBuilder, PrimitiveType, TimestampType,
};
pub use scalar::Scalar;
pub use string_array::{StringArray, StringBuilder};

macro_rules! impl_primitive_arrays {
    ($(($ty:ident, $native_ty:ty)),*) => {
        $(
        pub type $ty = PrimitiveArray<$native_ty>;
        )*
    };
}

impl_primitive_arrays!(
    (Int8Array, Int8Type),
    (Int16Array, Int16Type),
    (Int32Array, Int32Type),
    (Int64Array, Int64Type),
    (Float32Array, Float32Type),
    (Float64Array, Float64Type),
    (BooleanArray, BooleanType),
    (TimestampArray, TimestampType)
);

macro_rules! impl_primitive_builder {
    ($(($ty:ident, $native_ty:ty)),*) => {
        $(
        pub type $ty = PrimitiveBuilder<$native_ty>;
        )*
    };
}

impl_primitive_builder!(
    (Int8Builder, Int8Type),
    (Int16Builder, Int16Type),
    (Int32Builder, Int32Type),
    (Int64Builder, Int64Type),
    (Float32Builder, Float32Type),
    (Float64Builder, Float64Type),
    (BooleanBuilder, BooleanType),
    (TimestampBuilder, TimestampType)
);
