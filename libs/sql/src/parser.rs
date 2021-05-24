use std::str::FromStr;

use chrono_tz::Tz;
use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, tag_no_case, take};
use nom::character::complete::{alpha1, alphanumeric1, char, digit1, one_of};
use nom::combinator::{cut, map, map_res, opt, recognize, value};
use nom::error::context;
use nom::multi::{fold_many0, many0, separated_list0, separated_list1};
use nom::sequence::{delimited, pair, preceded, separated_pair, tuple};
use nom::IResult;
use yql_array::DataType;
use yql_dataset::Field;
use yql_expr::{BinaryOperator, Expr, Literal, UnaryOperator};
use yql_planner::Window;

use crate::ast::{
    GroupBy, OutputFormat, Select, Source, SourceFrom, Stmt, StmtCreateSink, StmtCreateSource,
    StmtCreateStream, StmtDeleteSink, StmtDeleteSource, StmtDeleteStream,
};

fn sp(input: &str) -> IResult<&str, ()> {
    fold_many0(value((), one_of(" \t\n\r")), (), |_, _| ())(input)
}

fn ident(input: &str) -> IResult<&str, &str> {
    context(
        "ident",
        recognize(pair(
            alt((alpha1, tag("_"))),
            many0(alt((alphanumeric1, tag("_")))),
        )),
    )(input)
}

fn boolean(input: &str) -> IResult<&str, bool> {
    context(
        "boolean",
        alt((
            map(tag_no_case("true"), |_| true),
            map(tag_no_case("false"), |_| false),
        )),
    )(input)
}

fn integer(input: &str) -> IResult<&str, i64> {
    context(
        "integer",
        map(recognize(tuple((opt(char('-')), digit1))), |s| {
            i64::from_str(s).unwrap()
        }),
    )(input)
}

fn float(input: &str) -> IResult<&str, f64> {
    context(
        "float",
        map(
            recognize(tuple((
                opt(char('-')),
                alt((
                    map(tuple((digit1, pair(char('.'), opt(digit1)))), |_| ()),
                    map(tuple((char('.'), digit1)), |_| ()),
                )),
                opt(tuple((
                    alt((char('e'), char('E'))),
                    opt(alt((char('+'), char('-')))),
                    cut(digit1),
                ))),
            ))),
            |s| f64::from_str(s).unwrap(),
        ),
    )(input)
}

fn raw_string_quoted(input: &str, is_single_quote: bool) -> IResult<&str, String> {
    let quote_str = if is_single_quote { "\'" } else { "\"" };
    let double_quote_str = if is_single_quote { "\'\'" } else { "\"\"" };
    let backslash_quote = if is_single_quote { "\\\'" } else { "\\\"" };
    delimited(
        tag(quote_str),
        fold_many0(
            alt((
                is_not(backslash_quote),
                map(tag(double_quote_str), |_| -> &str {
                    if is_single_quote {
                        "\'"
                    } else {
                        "\""
                    }
                }),
                map(tag("\\\\"), |_| "\\"),
                map(tag("\\b"), |_| "\x7f"),
                map(tag("\\r"), |_| "\r"),
                map(tag("\\n"), |_| "\n"),
                map(tag("\\t"), |_| "\t"),
                map(tag("\\0"), |_| "\0"),
                map(tag("\\Z"), |_| "\x1A"),
                preceded(tag("\\"), take(1usize)),
            )),
            String::new(),
            |mut acc: String, s: &str| {
                acc.push_str(s);
                acc
            },
        ),
        tag(quote_str),
    )(input)
}

fn raw_string_single_quoted(input: &str) -> IResult<&str, String> {
    raw_string_quoted(input, true)
}

fn raw_string_double_quoted(input: &str) -> IResult<&str, String> {
    raw_string_quoted(input, false)
}

fn string(input: &str) -> IResult<&str, String> {
    context(
        "string",
        alt((raw_string_single_quoted, raw_string_double_quoted)),
    )(input)
}

fn literal(input: &str) -> IResult<&str, Literal> {
    context(
        "literal",
        alt((
            map(boolean, Literal::Boolean),
            map(float, Literal::Float),
            map(integer, Literal::Int),
            map(string, Literal::String),
        )),
    )(input)
}

fn name(input: &str) -> IResult<&str, String> {
    context("name", alt((string, map(ident, ToString::to_string))))(input)
}

fn column(input: &str) -> IResult<&str, Expr> {
    context(
        "input",
        alt((
            map(
                separated_pair(name, char('.'), name),
                |(qualifier, name)| Expr::Column {
                    qualifier: Some(qualifier),
                    name,
                },
            ),
            map(name, |name| Expr::Column {
                qualifier: None,
                name,
            }),
        )),
    )(input)
}

fn expr(input: &str) -> IResult<&str, Expr> {
    context("expr", expr_a)(input)
}

fn expr_call(input: &str) -> IResult<&str, Expr> {
    let arguments = separated_list0(char(','), delimited(sp, expr, sp));
    context(
        "expr_call",
        map(
            tuple((ident, sp, char('('), sp, arguments, sp, char(')'))),
            |(name, _, _, _, args, _, _)| Expr::Call {
                name: name.to_string(),
                args,
            },
        ),
    )(input)
}

fn expr_primitive(input: &str) -> IResult<&str, Expr> {
    let parens = map(
        tuple((char('('), sp, expr, sp, char(')'))),
        |(_, _, expr, _, _)| expr,
    );
    let p = alt((
        parens,
        expr_unary,
        expr_call,
        map(literal, Expr::Literal),
        column,
    ));
    context("expr_primitive", delimited(sp, p, sp))(input)
}

fn expr_unary(input: &str) -> IResult<&str, Expr> {
    let op = alt((
        value(UnaryOperator::Not, tag_no_case("not")),
        value(UnaryOperator::Neg, char('-')),
    ));
    map(separated_pair(op, sp, expr), |(op, expr)| Expr::Unary {
        op,
        expr: Box::new(expr),
    })(input)
}

fn expr_a(input: &str) -> IResult<&str, Expr> {
    let (input, lhs) = expr_b(input)?;
    let (input, exprs) = many0(tuple((
        value(BinaryOperator::Or, tag_no_case("or")),
        expr_b,
    )))(input)?;
    Ok((input, parse_expr(lhs, exprs)))
}

fn expr_b(input: &str) -> IResult<&str, Expr> {
    let (input, lhs) = expr_c(input)?;
    let (input, exprs) = many0(tuple((
        value(BinaryOperator::Or, tag_no_case("and")),
        expr_c,
    )))(input)?;
    Ok((input, parse_expr(lhs, exprs)))
}

fn expr_c(input: &str) -> IResult<&str, Expr> {
    let (input, lhs) = expr_d(input)?;
    let (input, exprs) = many0(tuple((
        alt((
            value(BinaryOperator::Eq, tag("=")),
            value(BinaryOperator::NotEq, tag("!=")),
            value(BinaryOperator::NotEq, tag("<>")),
            value(BinaryOperator::Lt, tag("<")),
            value(BinaryOperator::LtEq, tag("<")),
            value(BinaryOperator::Gt, tag(">")),
            value(BinaryOperator::GtEq, tag(">=")),
        )),
        expr_d,
    )))(input)?;
    Ok((input, parse_expr(lhs, exprs)))
}

fn expr_d(input: &str) -> IResult<&str, Expr> {
    let (input, lhs) = expr_e(input)?;
    let (input, exprs) = many0(tuple((
        alt((
            value(BinaryOperator::Plus, char('+')),
            value(BinaryOperator::Minus, char('-')),
        )),
        expr_e,
    )))(input)?;
    Ok((input, parse_expr(lhs, exprs)))
}

fn expr_e(input: &str) -> IResult<&str, Expr> {
    let (input, lhs) = expr_primitive(input)?;
    let (input, exprs) = many0(tuple((
        alt((
            value(BinaryOperator::Multiply, char('*')),
            value(BinaryOperator::Divide, char('/')),
        )),
        expr_primitive,
    )))(input)?;
    Ok((input, parse_expr(lhs, exprs)))
}

fn parse_expr(expr: Expr, rem: Vec<(BinaryOperator, Expr)>) -> Expr {
    rem.into_iter().fold(expr, |lhs, (op, rhs)| Expr::Binary {
        op,
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
    })
}

fn projection_field(input: &str) -> IResult<&str, Expr> {
    context(
        "projection_field",
        alt((
            map(
                tuple((expr, sp, tag_no_case("as"), sp, name)),
                |(expr, _, _, _, alias)| expr.alias(alias),
            ),
            expr,
        )),
    )(input)
}

fn source_from(input: &str) -> IResult<&str, SourceFrom> {
    context(
        "source_from",
        alt((
            map(
                tuple((char('('), sp, select, sp, char(')'))),
                |(_, _, sub_query, _, _)| SourceFrom::SubQuery(Box::new(sub_query)),
            ),
            map(name, SourceFrom::Named),
        )),
    )(input)
}

fn source(input: &str) -> IResult<&str, Source> {
    context(
        "source",
        alt((
            map(
                tuple((source_from, sp, tag_no_case("as"), sp, name)),
                |(from, _, _, _, alias)| Source {
                    from,
                    alias: Some(alias),
                },
            ),
            map(source_from, |from| Source { from, alias: None }),
        )),
    )(input)
}

fn group_by(input: &str) -> IResult<&str, GroupBy> {
    context(
        "group_by",
        map(
            tuple((
                tag_no_case("group"),
                sp,
                tag_no_case("by"),
                sp,
                separated_list1(char(','), delimited(sp, expr, sp)),
            )),
            |(_, _, _, _, exprs)| GroupBy { exprs },
        ),
    )(input)
}

fn duration(input: &str) -> IResult<&str, i64> {
    let seconds = map(pair(integer, tag_no_case("s")), |(n, _)| n * 1000);
    let milliseconds = map(pair(integer, tag_no_case("ms")), |(n, _)| n);
    let minutes = map(pair(integer, tag_no_case("m")), |(n, _)| n * 1000 * 60);
    context("duration", alt((seconds, milliseconds, minutes)))(input)
}

fn window(input: &str) -> IResult<&str, Window> {
    let fixed_window = map(
        tuple((
            tag_no_case("window"),
            sp,
            tag_no_case("fixed"),
            sp,
            char('('),
            sp,
            duration,
            sp,
            char(')'),
        )),
        |(_, _, _, _, _, _, length, _, _)| Window::Fixed { length },
    );
    let sliding_window = map(
        tuple((
            tag_no_case("window"),
            sp,
            tag_no_case("sliding"),
            sp,
            char('('),
            sp,
            duration,
            sp,
            char(','),
            sp,
            duration,
            sp,
            char(')'),
        )),
        |(_, _, _, _, _, _, length, _, _, _, interval, _, _)| Window::Sliding { length, interval },
    );

    context("window", alt((fixed_window, sliding_window)))(input)
}

fn select(input: &str) -> IResult<&str, Select> {
    let projection = separated_list1(char(','), delimited(sp, projection_field, sp));
    let where_clause = map(tuple((tag_no_case("where"), sp, expr)), |(_, _, expr)| expr);
    let having_clause = map(tuple((tag_no_case("having"), sp, expr)), |(_, _, expr)| {
        expr
    });

    context(
        "select",
        map(
            tuple((
                tag_no_case("select"),
                delimited(sp, projection, sp),
                tag_no_case("from"),
                delimited(sp, source, sp),
                opt(delimited(sp, where_clause, sp)),
                opt(delimited(sp, group_by, sp)),
                opt(delimited(sp, having_clause, sp)),
                opt(delimited(sp, window, sp)),
            )),
            |(_, projection, _, source, where_clause, group_by, having_clause, window)| Select {
                projection,
                source,
                where_clause,
                having_clause,
                group_clause: group_by,
                window,
            },
        ),
    )(input)
}

fn timezone(input: &str) -> IResult<&str, Tz> {
    cut(map_res(string, |name| name.parse::<Tz>()))(input)
}

fn data_type(input: &str) -> IResult<&str, DataType> {
    let timezone = map(
        tuple((tag_no_case("timezone"), sp, timezone)),
        |(_, _, tz)| tz,
    );

    context(
        "data_type",
        alt((
            value(DataType::Int8, tag_no_case("int8")),
            value(DataType::Int16, tag_no_case("int16")),
            value(DataType::Int32, tag_no_case("int32")),
            value(DataType::Int64, tag_no_case("int64")),
            value(DataType::Float32, tag_no_case("float32")),
            value(DataType::Float64, tag_no_case("float64")),
            value(DataType::Boolean, tag_no_case("boolean")),
            map(
                tuple((
                    tag_no_case("timestamp"),
                    sp,
                    opt(delimited(sp, timezone, sp)),
                )),
                |(_, _, tz)| DataType::Timestamp(tz),
            ),
        )),
    )(input)
}

fn stmt_create_source(input: &str) -> IResult<&str, StmtCreateSource> {
    let field = map(tuple((name, sp, data_type)), |(name, _, data_type)| Field {
        qualifier: None,
        name,
        data_type,
    });
    let time_by = map(
        tuple((tag_no_case("time"), sp, tag_no_case("by"), sp, expr)),
        |(_, _, _, _, expr)| expr,
    );
    let watermark_by = map(
        tuple((tag_no_case("watermark"), sp, tag_no_case("by"), sp, expr)),
        |(_, _, _, _, expr)| expr,
    );

    context(
        "stmt_create_source",
        map(
            tuple((
                tag_no_case("create"),
                sp,
                tag_no_case("source"),
                sp,
                name,
                sp,
                delimited(
                    char('('),
                    separated_list0(char(','), delimited(sp, field, sp)),
                    char(')'),
                ),
                sp,
                tag_no_case("with"),
                sp,
                string,
                opt(delimited(sp, time_by, sp)),
                opt(delimited(sp, watermark_by, sp)),
            )),
            |(_, _, _, _, name, _, fields, _, _, _, uri, time_by, watermark_by)| StmtCreateSource {
                name,
                uri,
                fields,
                time: time_by,
                watermark: watermark_by,
            },
        ),
    )(input)
}

fn stmt_create_stream(input: &str) -> IResult<&str, StmtCreateStream> {
    context(
        "stmt_create_stream",
        map(
            tuple((
                tag_no_case("create"),
                sp,
                tag_no_case("stream"),
                sp,
                name,
                sp,
                tag_no_case("with"),
                sp,
                select,
                sp,
                tag_no_case("to"),
                sp,
                name,
            )),
            |(_, _, _, _, name, _, _, _, select, _, _, _, to)| StmtCreateStream {
                name,
                select,
                to,
            },
        ),
    )(input)
}

fn output_format(input: &str) -> IResult<&str, OutputFormat> {
    context(
        "output_format",
        value(OutputFormat::Json, tag_no_case("json")),
    )(input)
}

fn stmt_create_sink(input: &str) -> IResult<&str, StmtCreateSink> {
    let format = map(
        tuple((tag_no_case("format"), sp, output_format)),
        |(_, _, format)| format,
    );

    context(
        "stmt_create_sink",
        map(
            tuple((
                tag_no_case("create"),
                sp,
                tag_no_case("sink"),
                sp,
                name,
                sp,
                tag_no_case("with"),
                sp,
                string,
                sp,
                opt(format),
            )),
            |(_, _, _, _, name, _, _, _, uri, _, format)| StmtCreateSink {
                name,
                uri,
                format: format.unwrap_or_default(),
            },
        ),
    )(input)
}

fn stmt_delete_source(input: &str) -> IResult<&str, StmtDeleteSource> {
    context(
        "stmt_delete_source",
        map(
            tuple((tag_no_case("delete"), sp, tag_no_case("source"), sp, name)),
            |(_, _, _, _, name)| StmtDeleteSource { name },
        ),
    )(input)
}

fn stmt_delete_stream(input: &str) -> IResult<&str, StmtDeleteStream> {
    context(
        "stmt_delete_stream",
        map(
            tuple((tag_no_case("delete"), sp, tag_no_case("stream"), sp, name)),
            |(_, _, _, _, name)| StmtDeleteStream { name },
        ),
    )(input)
}

fn stmt_delete_sink(input: &str) -> IResult<&str, StmtDeleteSink> {
    context(
        "stmt_delete_sink",
        map(
            tuple((tag_no_case("delete"), sp, tag_no_case("sink"), sp, name)),
            |(_, _, _, _, name)| StmtDeleteSink { name },
        ),
    )(input)
}

pub fn stmt(input: &str) -> IResult<&str, Stmt> {
    context(
        "stmt",
        alt((
            map(delimited(sp, stmt_create_source, sp), Stmt::CreateSource),
            map(delimited(sp, stmt_create_stream, sp), Stmt::CreateStream),
            map(delimited(sp, stmt_create_sink, sp), Stmt::CreateSink),
            map(delimited(sp, stmt_delete_source, sp), Stmt::DeleteSource),
            map(delimited(sp, stmt_delete_stream, sp), Stmt::DeleteStream),
            map(delimited(sp, stmt_delete_sink, sp), Stmt::DeleteSink),
        )),
    )(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sp() {
        assert_eq!(sp(" \t\r\n"), Ok(("", ())));
    }

    #[test]
    fn test_ident() {
        assert_eq!(ident("a"), Ok(("", "a")));
        assert_eq!(ident("abc"), Ok(("", "abc")));
        assert_eq!(ident("ABC"), Ok(("", "ABC")));
        assert_eq!(ident("a1"), Ok(("", "a1")));
        assert_eq!(ident("A1"), Ok(("", "A1")));
        assert_eq!(ident("a_b"), Ok(("", "a_b")));
        assert_eq!(ident("_ab"), Ok(("", "_ab")));
    }

    #[test]
    fn test_bool() {
        assert_eq!(boolean("true"), Ok(("", true)));
        assert_eq!(boolean("false"), Ok(("", false)));
        assert_eq!(boolean("True"), Ok(("", true)));
        assert_eq!(boolean("False"), Ok(("", false)));
        assert_eq!(boolean("TRUE"), Ok(("", true)));
        assert_eq!(boolean("FALSE"), Ok(("", false)));
    }

    #[test]
    fn test_integer() {
        assert_eq!(integer("123"), Ok(("", 123)));
        assert_eq!(integer("0123"), Ok(("", 123)));
        assert_eq!(integer("230"), Ok(("", 230)));
    }

    #[test]
    fn test_float() {
        assert_eq!(float("123.12"), Ok(("", 123.12)));
        assert_eq!(float("0123.45"), Ok(("", 123.45)));
        assert_eq!(float("12.0e+2"), Ok(("", 1200.0)));
        assert_eq!(float("12.0e-2"), Ok(("", 0.12)));
    }

    #[test]
    fn test_string() {
        assert_eq!(string(r#""abc""#), Ok(("", "abc".to_string())));
        assert_eq!(string(r#"'abc'"#), Ok(("", "abc".to_string())));
        assert_eq!(string(r#"'\nab\rc'"#), Ok(("", "\nab\rc".to_string())));
    }

    #[test]
    fn test_literal() {
        assert_eq!(literal(r#"true"#), Ok(("", Literal::Boolean(true))));
        assert_eq!(literal(r#"0"#), Ok(("", Literal::Int(0))));
        assert_eq!(literal(r#"127"#), Ok(("", Literal::Int(127))));
        assert_eq!(literal(r#"-128"#), Ok(("", Literal::Int(-128))));
        assert_eq!(
            literal(r#""abc""#),
            Ok(("", Literal::String("abc".to_string())))
        );
    }

    #[test]
    fn test_name() {
        assert_eq!(name(r#""abc""#), Ok(("", "abc".to_string())));
        assert_eq!(name(r#"abc"#), Ok(("", "abc".to_string())));
    }

    #[test]
    fn test_column() {
        assert_eq!(
            column(r#""abc".a"#),
            Ok((
                "",
                Expr::Column {
                    qualifier: Some("abc".to_string()),
                    name: "a".to_string()
                }
            ))
        );

        assert_eq!(
            column(r#"abc.'123'"#),
            Ok((
                "",
                Expr::Column {
                    qualifier: Some("abc".to_string()),
                    name: "123".to_string()
                }
            ))
        );

        assert_eq!(
            column(r#"abc"#),
            Ok((
                "",
                Expr::Column {
                    qualifier: None,
                    name: "abc".to_string()
                }
            ))
        );

        assert_eq!(
            column(r#"'123'"#),
            Ok((
                "",
                Expr::Column {
                    qualifier: None,
                    name: "123".to_string()
                }
            ))
        );
    }

    #[test]
    fn test_expr() {
        assert_eq!(
            expr(r#"2000+4/2"#),
            Ok((
                "",
                Expr::Literal(Literal::Int(2000))
                    + Expr::Literal(Literal::Int(4)) / Expr::Literal(Literal::Int(2))
            ))
        );

        assert_eq!(
            expr(r#"(2000+4)/2"#),
            Ok((
                "",
                (Expr::Literal(Literal::Int(2000)) + Expr::Literal(Literal::Int(4)))
                    / Expr::Literal(Literal::Int(2))
            ))
        );
    }

    #[test]
    fn test_expr_call() {
        assert_eq!(
            expr_call(r#"sum(a)"#),
            Ok((
                "",
                Expr::Call {
                    name: "sum".to_string(),
                    args: vec![Expr::Column {
                        qualifier: None,
                        name: "a".to_string()
                    }]
                }
            ))
        );

        assert_eq!(
            expr_call(r#"c(a, 1, b, 2)"#),
            Ok((
                "",
                Expr::Call {
                    name: "c".to_string(),
                    args: vec![
                        Expr::Column {
                            qualifier: None,
                            name: "a".to_string()
                        },
                        Expr::Literal(Literal::Int(1)),
                        Expr::Column {
                            qualifier: None,
                            name: "b".to_string()
                        },
                        Expr::Literal(Literal::Int(2)),
                    ]
                }
            ))
        );
    }

    #[test]
    fn test_source() {
        assert_eq!(
            source(r#"abc"#),
            Ok((
                "",
                Source {
                    from: SourceFrom::Named("abc".to_string()),
                    alias: None
                }
            ))
        );
    }

    #[test]
    fn test_window() {
        assert_eq!(
            window(r#"window fixed(5m)"#),
            Ok((
                "",
                Window::Fixed {
                    length: 1000 * 5 * 60
                },
            ))
        );

        assert_eq!(
            window(r#"window sliding(5m, 1m)"#),
            Ok((
                "",
                Window::Sliding {
                    length: 1000 * 5 * 60,
                    interval: 1000 * 60,
                },
            ))
        );
    }

    #[test]
    fn test_select() {
        assert_eq!(
            select(r#"select a, b, a+b, sum(a) from t"#),
            Ok((
                "",
                Select {
                    projection: vec![
                        Expr::Column {
                            qualifier: None,
                            name: "a".to_string()
                        },
                        Expr::Column {
                            qualifier: None,
                            name: "b".to_string()
                        },
                        Expr::Column {
                            qualifier: None,
                            name: "a".to_string()
                        } + Expr::Column {
                            qualifier: None,
                            name: "b".to_string()
                        },
                        Expr::Call {
                            name: "sum".to_string(),
                            args: vec![Expr::Column {
                                qualifier: None,
                                name: "a".to_string()
                            }]
                        }
                    ],
                    source: Source {
                        from: SourceFrom::Named("t".to_string()),
                        alias: None
                    },
                    where_clause: None,
                    having_clause: None,
                    group_clause: None,
                    window: None,
                },
            )),
        );

        assert_eq!(
            select(r#"select a, b from t where a>10"#),
            Ok((
                "",
                Select {
                    projection: vec![
                        Expr::Column {
                            qualifier: None,
                            name: "a".to_string()
                        },
                        Expr::Column {
                            qualifier: None,
                            name: "b".to_string()
                        },
                    ],
                    source: Source {
                        from: SourceFrom::Named("t".to_string()),
                        alias: None
                    },
                    where_clause: Some(
                        Expr::Column {
                            qualifier: None,
                            name: "a".to_string()
                        }
                        .gt(Expr::Literal(Literal::Int(10)))
                    ),
                    having_clause: None,
                    group_clause: None,
                    window: None
                },
            )),
        );

        assert_eq!(
            select(r#"select a, b from t where a>10 group by b window fixed(5m)"#),
            Ok((
                "",
                Select {
                    projection: vec![
                        Expr::Column {
                            qualifier: None,
                            name: "a".to_string()
                        },
                        Expr::Column {
                            qualifier: None,
                            name: "b".to_string()
                        },
                    ],
                    source: Source {
                        from: SourceFrom::Named("t".to_string()),
                        alias: None
                    },
                    where_clause: Some(
                        Expr::Column {
                            qualifier: None,
                            name: "a".to_string()
                        }
                        .gt(Expr::Literal(Literal::Int(10)))
                    ),
                    having_clause: None,
                    group_clause: Some(GroupBy {
                        exprs: vec![Expr::Column {
                            qualifier: None,
                            name: "b".to_string()
                        }]
                    }),
                    window: Some(Window::Fixed {
                        length: 5 * 1000 * 60
                    })
                },
            )),
        );
    }

    #[test]
    fn test_time_zone() {
        assert_eq!(timezone(r#""UTC""#), Ok(("", chrono_tz::UTC)));
        assert_eq!(
            timezone(r#""Asia/Shanghai""#),
            Ok(("", chrono_tz::Asia::Shanghai))
        );
    }

    #[test]
    fn test_data_type() {
        assert_eq!(data_type("int8"), Ok(("", DataType::Int8)));
        assert_eq!(data_type("int16"), Ok(("", DataType::Int16)));
        assert_eq!(data_type("int32"), Ok(("", DataType::Int32)));
        assert_eq!(data_type("int64"), Ok(("", DataType::Int64)));
        assert_eq!(data_type("float32"), Ok(("", DataType::Float32)));
        assert_eq!(data_type("float64"), Ok(("", DataType::Float64)));
        assert_eq!(data_type("boolean"), Ok(("", DataType::Boolean)));
        assert_eq!(data_type("timestamp"), Ok(("", DataType::Timestamp(None))));

        assert_eq!(
            data_type("timestamp timezone \"UTC\""),
            Ok(("", DataType::Timestamp(Some(chrono_tz::UTC))))
        );
    }

    #[test]
    fn test_create_source() {
        assert_eq!(
            stmt_create_source(
                r#"create source a (
            a int8,
            b int16
        ) with "csv:///test""#
            ),
            Ok((
                "",
                StmtCreateSource {
                    name: "a".to_string(),
                    uri: "csv:///test".to_string(),
                    fields: vec![
                        Field::new("a", DataType::Int8),
                        Field::new("b", DataType::Int16),
                    ],
                    time: None,
                    watermark: None
                }
            ))
        );

        assert_eq!(
            stmt_create_source(
                r#"create source a (
            a int8,
            b int16,
            t timestamp,
            t2 timestamp
        ) with "csv:///test"
        time by t
        watermark by t2"#
            ),
            Ok((
                "",
                StmtCreateSource {
                    name: "a".to_string(),
                    uri: "csv:///test".to_string(),
                    fields: vec![
                        Field::new("a", DataType::Int8),
                        Field::new("b", DataType::Int16),
                        Field::new("t", DataType::Timestamp(None)),
                        Field::new("t2", DataType::Timestamp(None)),
                    ],
                    time: Some(Expr::Column {
                        qualifier: None,
                        name: "t".to_string()
                    }),
                    watermark: Some(Expr::Column {
                        qualifier: None,
                        name: "t2".to_string()
                    }),
                }
            ))
        )
    }

    #[test]
    fn test_create_stream() {
        assert_eq!(
            stmt_create_stream(r#"create stream a with select a, b from abc to d"#),
            Ok((
                "",
                StmtCreateStream {
                    name: "a".to_string(),
                    select: Select {
                        projection: vec![
                            Expr::Column {
                                qualifier: None,
                                name: "a".to_string()
                            },
                            Expr::Column {
                                qualifier: None,
                                name: "b".to_string()
                            }
                        ],
                        source: Source {
                            from: SourceFrom::Named("abc".to_string()),
                            alias: None
                        },
                        where_clause: None,
                        having_clause: None,
                        group_clause: None,
                        window: None
                    },
                    to: "d".to_string()
                }
            ))
        );

        assert_eq!(
            stmt_create_stream(r#"create stream a with select a.a, a.b from abc as a to d"#),
            Ok((
                "",
                StmtCreateStream {
                    name: "a".to_string(),
                    select: Select {
                        projection: vec![
                            Expr::Column {
                                qualifier: Some("a".to_string()),
                                name: "a".to_string()
                            },
                            Expr::Column {
                                qualifier: Some("a".to_string()),
                                name: "b".to_string()
                            }
                        ],
                        source: Source {
                            from: SourceFrom::Named("abc".to_string()),
                            alias: Some("a".to_string())
                        },
                        where_clause: None,
                        having_clause: None,
                        group_clause: None,
                        window: None
                    },
                    to: "d".to_string()
                }
            ))
        );
    }

    #[test]
    fn test_create_sink() {
        assert_eq!(
            stmt_create_sink(r#"create sink a with "http://test""#),
            Ok((
                "",
                StmtCreateSink {
                    name: "a".to_string(),
                    uri: "http://test".to_string(),
                    format: OutputFormat::Json,
                }
            ))
        );

        assert_eq!(
            stmt_create_sink(r#"create sink a with "http://test" format json"#),
            Ok((
                "",
                StmtCreateSink {
                    name: "a".to_string(),
                    uri: "http://test".to_string(),
                    format: OutputFormat::Json,
                }
            ))
        );
    }

    #[test]
    fn test_delete_source() {
        assert_eq!(
            stmt_delete_source(r#"delete source a"#),
            Ok((
                "",
                StmtDeleteSource {
                    name: "a".to_string(),
                }
            ))
        );
    }

    #[test]
    fn test_delete_stream() {
        assert_eq!(
            stmt_delete_stream(r#"delete stream a"#),
            Ok((
                "",
                StmtDeleteStream {
                    name: "a".to_string(),
                }
            ))
        );
    }

    #[test]
    fn test_delete_sink() {
        assert_eq!(
            stmt_delete_sink(r#"delete sink a"#),
            Ok((
                "",
                StmtDeleteSink {
                    name: "a".to_string(),
                }
            ))
        );
    }
}
