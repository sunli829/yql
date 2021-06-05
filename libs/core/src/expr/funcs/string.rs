use std::borrow::Cow;
use std::fmt::Write;
use std::sync::Arc;

use itertools::Either;

use crate::array::{
    Array, ArrayExt, DataType, Int64Array, Int64Builder, StringArray, StringBuilder,
};
use crate::expr::func::{Function, FunctionType};
use crate::expr::signature::Signature;

pub const CHR: Function = Function {
    namespace: None,
    name: "chr",
    signature: &Signature::Exact(&[DataType::Int64]),
    return_type: |_| DataType::String,
    function_type: FunctionType::Stateless(|args| {
        let array = args[0].downcast_ref::<Int64Array>();
        let mut builder = StringBuilder::with_capacity(array.len());

        for ch in array
            .iter_opt()
            .map(|x| x.and_then(|x| char::from_u32(x as u32)))
        {
            match ch {
                Some(ch) => {
                    let mut s = String::new();
                    s.write_char(ch)?;
                    builder.append(&s);
                }
                None => {
                    builder.append_null();
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }),
};

pub const CONCAT: Function = Function {
    namespace: None,
    name: "concat",
    signature: &Signature::Variadic(&[DataType::String]),
    return_type: |_| DataType::String,
    function_type: FunctionType::Stateless(|args| {
        let len = args[0].len();
        let mut buf = Vec::new();
        let mut builder = StringBuilder::with_capacity(len);

        for row in 0..len {
            let mut has_valid_str = false;
            buf.clear();

            for col in args {
                if let Some(value) = col.downcast_ref::<StringArray>().value_opt(row) {
                    buf.push(value);
                    has_valid_str = true;
                }
            }

            if has_valid_str {
                builder.append(&buf.concat());
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }),
};

pub const CONCAT_WS: Function = Function {
    namespace: None,
    name: "concat_ws",
    signature: &Signature::Variadic(&[DataType::String]),
    return_type: |_| DataType::String,
    function_type: FunctionType::Stateless(|args| {
        let len = args[0].len();
        let mut buf = Vec::new();
        let mut builder = StringBuilder::with_capacity(len);

        for row in 0..len {
            if let Some(sep) = args[0].downcast_ref::<StringArray>().value_opt(row) {
                let mut has_valid_str = false;
                buf.clear();

                for col in &args[1..] {
                    if let Some(value) = col.downcast_ref::<StringArray>().value_opt(row) {
                        buf.push(value);
                        has_valid_str = true;
                    }
                }

                if has_valid_str {
                    builder.append(&buf.join(sep));
                } else {
                    builder.append_null();
                }
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }),
};

pub const ENCODE: Function = Function {
    namespace: None,
    name: "encode",
    signature: &Signature::Exact(&[DataType::String, DataType::String, DataType::String]),
    return_type: |_| DataType::String,
    function_type: FunctionType::Stateless(|args| {
        let array = args[0].downcast_ref::<StringArray>();
        let input_encoding = args[1].downcast_ref::<StringArray>();
        let output_encoding = args[2].downcast_ref::<StringArray>();
        let mut builder = StringBuilder::with_capacity(array.len());

        for ((value, input), output) in array
            .iter_opt()
            .zip(input_encoding.iter_opt())
            .zip(output_encoding.iter_opt())
        {
            if let (Some(value), Some(input), Some(output)) = (value, input, output) {
                let value = if input.eq_ignore_ascii_case("hex") {
                    Cow::Owned(String::from_utf8(hex::decode(value)?)?)
                } else if input.eq_ignore_ascii_case("base64") {
                    Cow::Owned(String::from_utf8(base64::decode(value)?)?)
                } else if input.eq_ignore_ascii_case("utf8") {
                    Cow::Borrowed(value)
                } else {
                    anyhow::bail!("unsupported encoding: {}", input);
                };

                let value = if output.eq_ignore_ascii_case("hex") {
                    Cow::Owned(hex::encode(&*value))
                } else if output.eq_ignore_ascii_case("base64") {
                    Cow::Owned(base64::encode(&*value))
                } else if output.eq_ignore_ascii_case("utf8") {
                    value
                } else {
                    anyhow::bail!("unsupported encoding: {}", output);
                };

                builder.append(&value);
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }),
};

pub const INSTR: Function = Function {
    namespace: None,
    name: "instr",
    signature: &Signature::OneOf(&[
        Signature::Exact(&[DataType::String, DataType::String]),
        Signature::Exact(&[DataType::String, DataType::String, DataType::Int64]),
        Signature::Exact(&[
            DataType::String,
            DataType::String,
            DataType::Int64,
            DataType::Int64,
        ]),
    ]),
    return_type: |_| DataType::Int64,
    function_type: FunctionType::Stateless(|args| {
        let string = args[0].downcast_ref::<StringArray>();
        let substring = args[1].downcast_ref::<StringArray>();
        let position = args
            .get(2)
            .map(|array| Either::Left(array.downcast_ref::<Int64Array>().iter_opt()))
            .unwrap_or_else(|| Either::Right(std::iter::repeat(None)));
        let occurrence = args
            .get(3)
            .map(|array| Either::Left(array.downcast_ref::<Int64Array>().iter_opt()))
            .unwrap_or_else(|| Either::Right(std::iter::repeat(None)));
        let mut builder = Int64Builder::with_capacity(string.len());

        for (((string, substring), position), occurrence) in string
            .iter_opt()
            .zip(substring.iter_opt())
            .zip(position)
            .zip(occurrence)
        {
            match (string, substring, position, occurrence) {
                (Some(string), Some(substring), None, None) => {
                    builder.append_opt(string.find(substring).map(|x| x as i64))
                }
                (Some(string), Some(substring), Some(position), None) => {
                    if position >= 0 {
                        if position as usize <= string.len() {
                            builder.append_opt(
                                string[position as usize..]
                                    .find(substring)
                                    .map(|x| x as i64 + position),
                            );
                        } else {
                            builder.append_null();
                        }
                    } else {
                        let position = -position as usize - 1;
                        if position <= string.len() {
                            builder.append_opt(
                                string[..string.len() - position]
                                    .rfind(substring)
                                    .map(|x| x as i64),
                            );
                        } else {
                            builder.append_null();
                        }
                    }
                }
                (Some(string), Some(substring), Some(mut position), Some(occurrence)) => {
                    if position >= 0 {
                        if position as usize <= string.len() {
                            for i in 0..occurrence {
                                match string[position as usize..]
                                    .find(substring)
                                    .map(|x| x as i64 + position)
                                {
                                    Some(idx) => {
                                        if i == occurrence - 1 {
                                            builder.append(idx);
                                        } else {
                                            position = idx + 1;
                                        }
                                    }
                                    None => {
                                        builder.append_null();
                                        break;
                                    }
                                }
                            }
                        } else {
                            builder.append_null();
                        }
                    } else {
                        let mut position = string.len() - (position.abs() as usize - 1);
                        if position <= string.len() {
                            for i in 0..occurrence {
                                match string[..position].rfind(substring).map(|x| x as i64) {
                                    Some(idx) => {
                                        if i == occurrence - 1 {
                                            builder.append(idx);
                                        } else {
                                            position = idx as usize;
                                        }
                                    }
                                    None => {
                                        builder.append_null();
                                        break;
                                    }
                                }
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                }
                _ => builder.append_null(),
            }
        }

        Ok(Arc::new(builder.finish()))
    }),
};

pub const LCASE: Function = Function {
    namespace: None,
    name: "lcase",
    signature: &Signature::Exact(&[DataType::String]),
    return_type: |_| DataType::String,
    function_type: FunctionType::Stateless(|args| {
        let array = args[0].downcast_ref::<StringArray>();
        let mut builder = StringBuilder::with_capacity(array.len());

        for value in array.iter_opt() {
            if let Some(value) = value {
                builder.append(&value.to_lowercase());
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }),
};

pub const LEN: Function = Function {
    namespace: None,
    name: "len",
    signature: &Signature::Exact(&[DataType::String]),
    return_type: |_| DataType::Int64,
    function_type: FunctionType::Stateless(|args| {
        let array = args[0].downcast_ref::<StringArray>();
        let mut builder = Int64Builder::with_capacity(array.len());

        for value in array.iter_opt() {
            if let Some(value) = value {
                builder.append(value.len() as i64);
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }),
};

pub const LPAD: Function = Function {
    namespace: None,
    name: "lpad",
    signature: &Signature::Exact(&[DataType::String, DataType::Int64, DataType::String]),
    return_type: |_| DataType::String,
    function_type: FunctionType::Stateless(|args| {
        let array = args[0].downcast_ref::<StringArray>();
        let length = args[1].downcast_ref::<Int64Array>();
        let padding = args[2].downcast_ref::<StringArray>();
        let mut builder = StringBuilder::with_capacity(array.len());

        for ((value, length), padding) in array
            .iter_opt()
            .zip(length.iter_opt())
            .zip(padding.iter_opt())
        {
            match (value, length, padding) {
                (Some(value), Some(length), Some(padding))
                    if length >= 0 && !padding.is_empty() && (length as usize) > value.len() =>
                {
                    let times_padding = (length as usize - value.len()) / padding.len();
                    let remaining_padding = (length as usize - value.len()) % padding.len();
                    let mut s = String::new();

                    for _ in 0..times_padding {
                        s.push_str(padding);
                    }
                    s.push_str(&padding[..remaining_padding]);
                    s.push_str(value);
                    builder.append(&s);
                }
                (Some(value), Some(length), _) => {
                    builder.append(&value[..length as usize]);
                }
                _ => builder.append_null(),
            }
        }

        Ok(Arc::new(builder.finish()))
    }),
};

pub const REPLACE: Function = Function {
    namespace: None,
    name: "replace",
    signature: &Signature::Exact(&[DataType::String, DataType::String, DataType::String]),
    return_type: |_| DataType::String,
    function_type: FunctionType::Stateless(|args| {
        let array = args[0].downcast_ref::<StringArray>();
        let a = args[1].downcast_ref::<StringArray>();
        let b = args[2].downcast_ref::<StringArray>();
        let mut builder = StringBuilder::with_capacity(array.len());

        for ((value, a), b) in array.iter_opt().zip(a.iter_opt()).zip(b.iter_opt()) {
            if let (Some(value), Some(a), Some(b)) = (value, a, b) {
                builder.append(&value.replace(a, b));
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }),
};

pub const RPAD: Function = Function {
    namespace: None,
    name: "rpad",
    signature: &Signature::Exact(&[DataType::String, DataType::Int64, DataType::String]),
    return_type: |_| DataType::String,
    function_type: FunctionType::Stateless(|args| {
        let array = args[0].downcast_ref::<StringArray>();
        let length = args[1].downcast_ref::<Int64Array>();
        let padding = args[2].downcast_ref::<StringArray>();
        let mut builder = StringBuilder::with_capacity(array.len());

        for ((value, length), padding) in array
            .iter_opt()
            .zip(length.iter_opt())
            .zip(padding.iter_opt())
        {
            match (value, length, padding) {
                (Some(value), Some(length), Some(padding))
                    if length >= 0 && !padding.is_empty() && (length as usize) > value.len() =>
                {
                    let times_padding = (length as usize - value.len()) / padding.len();
                    let remaining_padding = (length as usize - value.len()) % padding.len();
                    let mut s = String::new();

                    for _ in 0..times_padding {
                        s.push_str(padding);
                    }
                    s.push_str(value);
                    s.push_str(&padding[..remaining_padding]);
                    builder.append(&s);
                }
                (Some(value), Some(length), _) => {
                    builder.append(&value[..length as usize]);
                }
                _ => builder.append_null(),
            }
        }

        Ok(Arc::new(builder.finish()))
    }),
};

pub const SUBSTRING: Function = Function {
    namespace: None,
    name: "substring",
    signature: &Signature::OneOf(&[
        Signature::Exact(&[DataType::String, DataType::Int64]),
        Signature::Exact(&[DataType::String, DataType::Int64, DataType::Int64]),
    ]),
    return_type: |_| DataType::String,
    function_type: FunctionType::Stateless(|args| {
        let array = args[0].downcast_ref::<StringArray>();
        let pos = args[1].downcast_ref::<Int64Array>();
        let length = args
            .get(2)
            .map(|array| Either::Left(array.downcast_ref::<Int64Array>().iter_opt()))
            .unwrap_or_else(|| Either::Right(std::iter::repeat(None)));
        let mut builder = StringBuilder::with_capacity(array.len());

        for ((value, pos), length) in array.iter_opt().zip(pos.iter_opt()).zip(length) {
            match (value, pos, length) {
                (Some(value), Some(pos), Some(length)) => {
                    if pos < 0 || length < 0 {
                        builder.append_null();
                        continue;
                    }
                    if (pos as usize) <= value.len() && ((pos + length) as usize) <= value.len() {
                        builder.append(&value[pos as usize..(pos + length) as usize]);
                    } else {
                        builder.append_null();
                    }
                }
                (Some(value), Some(pos), None) => {
                    if pos < 0 {
                        builder.append_null();
                        continue;
                    }
                    if (pos as usize) <= value.len() {
                        builder.append(&value[pos as usize..]);
                    } else {
                        builder.append_null();
                    }
                }
                _ => builder.append_null(),
            }
        }

        Ok(Arc::new(builder.finish()))
    }),
};

pub const TRIM: Function = Function {
    namespace: None,
    name: "trim",
    signature: &Signature::Exact(&[DataType::String]),
    return_type: |_| DataType::String,
    function_type: FunctionType::Stateless(|args| {
        let array = args[0].downcast_ref::<StringArray>();
        let mut builder = StringBuilder::with_capacity(array.len());

        for value in array.iter_opt() {
            if let Some(value) = value {
                builder.append(value.trim());
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }),
};

pub const UCASE: Function = Function {
    namespace: None,
    name: "ucase",
    signature: &Signature::Exact(&[DataType::String]),
    return_type: |_| DataType::String,
    function_type: FunctionType::Stateless(|args| {
        let array = args[0].downcast_ref::<StringArray>();
        let mut builder = StringBuilder::with_capacity(array.len());

        for value in array.iter_opt() {
            if let Some(value) = value {
                builder.append(&value.to_uppercase());
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

    #[test]
    fn test_chr() {
        assert_eq!(
            &*CHR
                .function_type
                .call_stateless_fun(&[Arc::new(Int64Array::new_scalar(1, Some(75)))])
                .unwrap(),
            &StringArray::new_scalar(1, Some("K")) as &dyn Array
        );

        assert_eq!(
            &*CHR
                .function_type
                .call_stateless_fun(&[Arc::new(Int64Array::new_scalar(1, Some(22909)))])
                .unwrap(),
            &StringArray::new_scalar(1, Some("好")) as &dyn Array
        );

        assert_eq!(
            &*CHR
                .function_type
                .call_stateless_fun(&[Arc::new(Int64Array::new_scalar(1, None))])
                .unwrap(),
            &StringArray::new_scalar(1, None::<&str>) as &dyn Array
        );
    }

    #[test]
    fn test_concat() {
        assert_eq!(
            &*CONCAT
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("a"))),
                    Arc::new(StringArray::new_scalar(1, Some("bc"))),
                    Arc::new(StringArray::new_scalar(1, Some("def")))
                ])
                .unwrap(),
            &StringArray::new_scalar(1, Some("abcdef")) as &dyn Array
        );

        assert_eq!(
            &*CONCAT
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("a"))),
                    Arc::new(StringArray::new_scalar(1, None::<&str>)),
                    Arc::new(StringArray::new_scalar(1, Some("def")))
                ])
                .unwrap(),
            &StringArray::new_scalar(1, Some("adef")) as &dyn Array
        );

        assert_eq!(
            &*CONCAT
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, None::<&str>)),
                    Arc::new(StringArray::new_scalar(1, None::<&str>)),
                ])
                .unwrap(),
            &StringArray::new_scalar(1, None::<&str>) as &dyn Array
        );
    }

    #[test]
    fn test_concat_ws() {
        assert_eq!(
            &*CONCAT_WS
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some(","))),
                    Arc::new(StringArray::new_scalar(1, Some("a"))),
                    Arc::new(StringArray::new_scalar(1, Some("bc"))),
                    Arc::new(StringArray::new_scalar(1, Some("def"))),
                ])
                .unwrap(),
            &StringArray::new_scalar(1, Some("a,bc,def")) as &dyn Array
        );

        assert_eq!(
            &*CONCAT_WS
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some(","))),
                    Arc::new(StringArray::new_scalar(1, Some("a"))),
                    Arc::new(StringArray::new_scalar(1, None::<&str>)),
                    Arc::new(StringArray::new_scalar(1, Some("def"))),
                ])
                .unwrap(),
            &StringArray::new_scalar(1, Some("a,def")) as &dyn Array
        );

        assert_eq!(
            &*CONCAT_WS
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some(","))),
                    Arc::new(StringArray::new_scalar(1, None::<&str>)),
                    Arc::new(StringArray::new_scalar(1, None::<&str>)),
                ])
                .unwrap(),
            &StringArray::new_scalar(1, None::<&str>) as &dyn Array
        );
    }

    #[test]
    fn test_encode() {
        assert_eq!(
            &*ENCODE
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("abc"))),
                    Arc::new(StringArray::new_scalar(1, Some("utf8"))),
                    Arc::new(StringArray::new_scalar(1, Some("hex"))),
                ])
                .unwrap(),
            &StringArray::new_scalar(1, Some("616263")) as &dyn Array
        );

        assert_eq!(
            &*ENCODE
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("616263"))),
                    Arc::new(StringArray::new_scalar(1, Some("hex"))),
                    Arc::new(StringArray::new_scalar(1, Some("utf8"))),
                ])
                .unwrap(),
            &StringArray::new_scalar(1, Some("abc")) as &dyn Array
        );

        assert_eq!(
            &*ENCODE
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("abc"))),
                    Arc::new(StringArray::new_scalar(1, Some("utf8"))),
                    Arc::new(StringArray::new_scalar(1, Some("base64"))),
                ])
                .unwrap(),
            &StringArray::new_scalar(1, Some("YWJj")) as &dyn Array
        );

        assert_eq!(
            &*ENCODE
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("YWJj"))),
                    Arc::new(StringArray::new_scalar(1, Some("base64"))),
                    Arc::new(StringArray::new_scalar(1, Some("utf8"))),
                ])
                .unwrap(),
            &StringArray::new_scalar(1, Some("abc")) as &dyn Array
        );

        assert_eq!(
            &*ENCODE
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("YWJj"))),
                    Arc::new(StringArray::new_scalar(1, Some("base64"))),
                    Arc::new(StringArray::new_scalar(1, Some("hex"))),
                ])
                .unwrap(),
            &StringArray::new_scalar(1, Some("616263")) as &dyn Array
        );

        assert_eq!(
            &*ENCODE
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("616263"))),
                    Arc::new(StringArray::new_scalar(1, Some("hex"))),
                    Arc::new(StringArray::new_scalar(1, Some("base64"))),
                ])
                .unwrap(),
            &StringArray::new_scalar(1, Some("YWJj")) as &dyn Array
        );

        assert_eq!(
            &*ENCODE
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, None::<&str>)),
                    Arc::new(StringArray::new_scalar(1, Some("utf8"))),
                    Arc::new(StringArray::new_scalar(1, Some("hex"))),
                ])
                .unwrap(),
            &StringArray::new_scalar(1, None::<&str>) as &dyn Array
        );
    }

    #[test]
    fn test_instr() {
        assert_eq!(
            &*INSTR
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("abcabc"))),
                    Arc::new(StringArray::new_scalar(1, Some("ca"))),
                ])
                .unwrap(),
            &Int64Array::new_scalar(1, Some(2)) as &dyn Array
        );

        assert_eq!(
            &*INSTR
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("abcabc"))),
                    Arc::new(StringArray::new_scalar(1, Some("de"))),
                ])
                .unwrap(),
            &Int64Array::new_scalar(1, None) as &dyn Array
        );

        assert_eq!(
            &*INSTR
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("abcabc"))),
                    Arc::new(StringArray::new_scalar(1, Some("bc"))),
                    Arc::new(Int64Array::new_scalar(1, Some(3))),
                ])
                .unwrap(),
            &Int64Array::new_scalar(1, Some(4)) as &dyn Array
        );

        assert_eq!(
            &*INSTR
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("abcabc"))),
                    Arc::new(StringArray::new_scalar(1, Some("ab"))),
                    Arc::new(Int64Array::new_scalar(1, Some(0))),
                ])
                .unwrap(),
            &Int64Array::new_scalar(1, Some(0)) as &dyn Array
        );

        assert_eq!(
            &*INSTR
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("abcabc"))),
                    Arc::new(StringArray::new_scalar(1, Some("bc"))),
                    Arc::new(Int64Array::new_scalar(1, Some(3))),
                ])
                .unwrap(),
            &Int64Array::new_scalar(1, Some(4)) as &dyn Array
        );

        assert_eq!(
            &*INSTR
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("abcabc"))),
                    Arc::new(StringArray::new_scalar(1, Some("bcd"))),
                    Arc::new(Int64Array::new_scalar(1, Some(3))),
                ])
                .unwrap(),
            &Int64Array::new_scalar(1, None) as &dyn Array
        );

        assert_eq!(
            &*INSTR
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("abcabc"))),
                    Arc::new(StringArray::new_scalar(1, Some("a"))),
                    Arc::new(Int64Array::new_scalar(1, Some(-3))),
                ])
                .unwrap(),
            &Int64Array::new_scalar(1, Some(3)) as &dyn Array
        );

        assert_eq!(
            &*INSTR
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("abcabc"))),
                    Arc::new(StringArray::new_scalar(1, Some("c"))),
                    Arc::new(Int64Array::new_scalar(1, Some(-2))),
                ])
                .unwrap(),
            &Int64Array::new_scalar(1, Some(2)) as &dyn Array
        );

        assert_eq!(
            &*INSTR
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("abcabc"))),
                    Arc::new(StringArray::new_scalar(1, Some("c"))),
                    Arc::new(Int64Array::new_scalar(1, Some(-5))),
                ])
                .unwrap(),
            &Int64Array::new_scalar(1, None) as &dyn Array
        );

        assert_eq!(
            &*INSTR
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("abcabc"))),
                    Arc::new(StringArray::new_scalar(1, Some("a"))),
                    Arc::new(Int64Array::new_scalar(1, Some(0))),
                    Arc::new(Int64Array::new_scalar(1, Some(2))),
                ])
                .unwrap(),
            &Int64Array::new_scalar(1, Some(3)) as &dyn Array
        );

        assert_eq!(
            &*INSTR
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("abcabc"))),
                    Arc::new(StringArray::new_scalar(1, Some("a"))),
                    Arc::new(Int64Array::new_scalar(1, Some(2))),
                    Arc::new(Int64Array::new_scalar(1, Some(2))),
                ])
                .unwrap(),
            &Int64Array::new_scalar(1, None) as &dyn Array
        );

        assert_eq!(
            &*INSTR
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("abcabc"))),
                    Arc::new(StringArray::new_scalar(1, Some("c"))),
                    Arc::new(Int64Array::new_scalar(1, Some(-1))),
                    Arc::new(Int64Array::new_scalar(1, Some(2))),
                ])
                .unwrap(),
            &Int64Array::new_scalar(1, Some(2)) as &dyn Array
        );

        assert_eq!(
            &*INSTR
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("abcabc"))),
                    Arc::new(StringArray::new_scalar(1, Some("c"))),
                    Arc::new(Int64Array::new_scalar(1, Some(-1))),
                    Arc::new(Int64Array::new_scalar(1, Some(1))),
                ])
                .unwrap(),
            &Int64Array::new_scalar(1, Some(5)) as &dyn Array
        );

        assert_eq!(
            &*INSTR
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, Some("abcabc"))),
                    Arc::new(StringArray::new_scalar(1, Some("c"))),
                    Arc::new(Int64Array::new_scalar(1, Some(-1))),
                    Arc::new(Int64Array::new_scalar(1, Some(3))),
                ])
                .unwrap(),
            &Int64Array::new_scalar(1, None) as &dyn Array
        );

        assert_eq!(
            &*INSTR
                .function_type
                .call_stateless_fun(&[
                    Arc::new(StringArray::new_scalar(1, None::<&str>)),
                    Arc::new(StringArray::new_scalar(1, Some("ca"))),
                ])
                .unwrap(),
            &Int64Array::new_scalar(1, None) as &dyn Array
        );
    }

    #[test]
    fn test_lcase() {
        assert_eq!(
            &*LCASE
                .function_type
                .call_stateless_fun(&[Arc::new(StringArray::new_scalar(1, Some("aBc")))])
                .unwrap(),
            &StringArray::new_scalar(1, Some("abc")) as &dyn Array
        );

        assert_eq!(
            &*LCASE
                .function_type
                .call_stateless_fun(&[Arc::new(StringArray::new_scalar(1, None::<&str>))])
                .unwrap(),
            &StringArray::new_scalar(1, None::<&str>) as &dyn Array
        );
    }

    #[test]
    fn test_ucase() {
        assert_eq!(
            &*UCASE
                .function_type
                .call_stateless_fun(&[Arc::new(StringArray::new_scalar(1, Some("aBc")))])
                .unwrap(),
            &StringArray::new_scalar(1, Some("ABC")) as &dyn Array
        );

        assert_eq!(
            &*UCASE
                .function_type
                .call_stateless_fun(&[Arc::new(StringArray::new_scalar(1, None::<&str>))])
                .unwrap(),
            &StringArray::new_scalar(1, None::<&str>) as &dyn Array
        );
    }

    #[test]
    fn test_len() {
        assert_eq!(
            &*LEN
                .function_type
                .call_stateless_fun(&[Arc::new(StringArray::new_scalar(1, Some("aBc")))])
                .unwrap(),
            &Int64Array::new_scalar(1, Some(3)) as &dyn Array
        );

        assert_eq!(
            &*LEN
                .function_type
                .call_stateless_fun(&[Arc::new(StringArray::new_scalar(1, None::<&str>))])
                .unwrap(),
            &Int64Array::new_scalar(1, None) as &dyn Array
        );
    }
}
