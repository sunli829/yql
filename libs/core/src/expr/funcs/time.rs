use std::str::FromStr;
use std::sync::Arc;

use chrono::TimeZone;
use chrono_tz::Tz;
use itertools::Either;

use crate::array::{
    ArrayExt, ArrayRef, DataType, Int64Array, StringArray, StringBuilder, TimestampArray,
    TimestampBuilder,
};
use crate::expr::func::{Function, FunctionType};
use crate::expr::signature::Signature;

#[allow(clippy::needless_lifetimes)]
fn timezone_array<'a>(
    array: Option<&'a ArrayRef>,
) -> impl Iterator<Item = Result<Tz, String>> + 'a {
    array
        .map(|array| {
            let tz_array = array.downcast_ref::<StringArray>();
            match tz_array.to_scalar() {
                Some(Some(tz_name)) => Either::Left(std::iter::repeat(Tz::from_str(tz_name))),
                Some(None) => Either::Left(std::iter::repeat(Ok(chrono_tz::UTC))),
                None => {
                    let iter = tz_array.iter_opt().map(|name| {
                        name.map(|name| Tz::from_str(name))
                            .unwrap_or_else(|| Ok(chrono_tz::UTC))
                    });
                    Either::Right(iter)
                }
            }
        })
        .unwrap_or_else(|| Either::Left(std::iter::repeat(Ok(chrono_tz::UTC))))
}

pub const PARSE_TIMESTAMP: Function = Function {
    namespace: None,
    name: "parse_timestamp",
    signature: &Signature::OneOf(&[
        Signature::Exact(&[DataType::String, DataType::String, DataType::String]),
        Signature::Exact(&[DataType::String, DataType::String]),
    ]),
    return_type: |_| DataType::Timestamp(None),
    function_type: FunctionType::Stateless(|args| {
        let array = args[0].downcast_ref::<StringArray>();
        let fmt = args[1].downcast_ref::<StringArray>();
        let tz_array = timezone_array(args.get(2));
        let mut builder = TimestampBuilder::with_capacity(args[0].len());

        for ((value, fmt), tz_res) in array.iter_opt().zip(fmt.iter_opt()).zip(tz_array) {
            let tz = tz_res.map_err(|err| anyhow::anyhow!("{}", err))?;

            if let (Some(value), Some(fmt)) = (value, fmt) {
                builder.append(tz.datetime_from_str(value, fmt)?.timestamp_millis());
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }),
};

pub const FORMAT_TIMESTAMP: Function = Function {
    namespace: None,
    name: "format_timestamp",
    signature: &Signature::OneOf(&[
        Signature::Exact(&[
            DataType::Timestamp(None),
            DataType::String,
            DataType::String,
        ]),
        Signature::Exact(&[DataType::Timestamp(None), DataType::String]),
    ]),
    return_type: |_| DataType::String,
    function_type: FunctionType::Stateless(|args| {
        let array = args[0].downcast_ref::<TimestampArray>();
        let fmt = args[1].downcast_ref::<StringArray>();
        let tz_array = timezone_array(args.get(2));
        let mut builder = StringBuilder::with_capacity(args[0].len());

        for ((value, fmt), tz_res) in array.iter_opt().zip(fmt.iter_opt()).zip(tz_array) {
            let tz = tz_res.map_err(|err| anyhow::anyhow!("{}", err))?;

            if let (Some(value), Some(fmt)) = (value, fmt) {
                builder.append(&format!("{}", tz.timestamp_millis(value).format(fmt)));
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }),
};

pub const TIMESTAMP_ADD: Function = Function {
    namespace: None,
    name: "timestamp_add",
    signature: &Signature::Exact(&[DataType::Timestamp(None), DataType::Int64]),
    return_type: |args| args[0],
    function_type: FunctionType::Stateless(|args| {
        let array = args[0].downcast_ref::<TimestampArray>();
        let n = args[1].downcast_ref::<Int64Array>();
        let mut builder = TimestampBuilder::with_capacity(args[0].len());

        for (value, n) in array.iter_opt().zip(n.iter_opt()) {
            if let (Some(value), Some(n)) = (value, n) {
                builder.append_opt(value.checked_add(n));
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }),
};

pub const TIMESTAMP_SUB: Function = Function {
    namespace: None,
    name: "timestamp_sub",
    signature: &Signature::Exact(&[DataType::Timestamp(None), DataType::Int64]),
    return_type: |args| args[0],
    function_type: FunctionType::Stateless(|args| {
        let array = args[0].downcast_ref::<TimestampArray>();
        let n = args[1].downcast_ref::<Int64Array>();
        let mut builder = TimestampBuilder::with_capacity(args[0].len());

        for (value, n) in array.iter_opt().zip(n.iter_opt()) {
            if let (Some(value), Some(n)) = (value, n) {
                builder.append_opt(value.checked_sub(n));
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }),
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{Array, TimestampArray};

    #[test]
    fn test_parse_timestamp() {
        assert_eq!(
            &*PARSE_TIMESTAMP
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("1983 Apr 13 12:09:14.274"))),
                    Arc::new(StringArray::new_scalar(1, Some("%Y %b %d %H:%M:%S%.3f"))),
                ])
                .unwrap(),
            &TimestampArray::new_scalar(1, Some(419083754274)) as &dyn Array
        );

        assert_eq!(
            &*PARSE_TIMESTAMP
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("1983 Apr 13 12:09:14.274"))),
                    Arc::new(StringArray::new_scalar(1, Some("%Y %b %d %H:%M:%S%.3f"))),
                    Arc::new(StringArray::new_scalar(1, Some("Asia/Shanghai"))),
                ])
                .unwrap(),
            &TimestampArray::new_scalar(1, Some(419054954274)) as &dyn Array
        );

        assert_eq!(
            &*PARSE_TIMESTAMP
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, None::<&str>)),
                    Arc::new(StringArray::new_scalar(1, Some("%Y %b %d %H:%M:%S%.3f"))),
                    Arc::new(StringArray::new_scalar(1, Some("Asia/Shanghai"))),
                ])
                .unwrap(),
            &TimestampArray::new_scalar(1, None) as &dyn Array
        );
    }
}
