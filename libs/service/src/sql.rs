use chrono_tz::Tz;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::char;
use nom::combinator::{cut, eof, map, map_res, opt, value};
use nom::error::context;
use nom::multi::separated_list0;
use nom::sequence::{delimited, terminated, tuple};
use nom::IResult;
use yql_core::array::DataType;
use yql_core::dataset::Field;
use yql_core::expr::Expr;
use yql_core::sql::ast::Select;
use yql_core::sql::parser::{expr, name, select, sp, string};

#[derive(Debug, PartialEq)]
pub struct StmtCreateStream {
    pub name: String,
    pub select: Select,
    pub to: String,
}

#[derive(Debug, PartialEq)]
pub struct StmtCreateSource {
    pub name: String,
    pub uri: String,
    pub fields: Vec<Field>,
    pub time: Option<Expr>,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum OutputFormat {
    Json,
}

impl Default for OutputFormat {
    fn default() -> Self {
        OutputFormat::Json
    }
}

#[derive(Debug, PartialEq)]
pub struct StmtCreateSink {
    pub name: String,
    pub uri: String,
    pub format: OutputFormat,
}

#[derive(Debug, PartialEq)]
pub struct StmtDeleteSource {
    pub name: String,
}

#[derive(Debug, PartialEq)]
pub struct StmtDeleteStream {
    pub name: String,
}

#[derive(Debug, PartialEq)]
pub struct StmtDeleteSink {
    pub name: String,
}

#[derive(Debug, PartialEq)]
pub struct StmtStartStream {
    pub name: String,
    pub restart: bool,
}

#[derive(Debug, PartialEq)]
pub struct StmtStopStream {
    pub name: String,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum ShowType {
    Sources,
    Streams,
    Sinks,
}

#[derive(Debug, PartialEq)]
pub struct StmtShow {
    pub show_type: ShowType,
}

#[derive(Debug, PartialEq)]
pub struct StmtSelect {
    pub select: Select,
}

#[derive(Debug, PartialEq)]
pub enum Stmt {
    CreateSource(Box<StmtCreateSource>),
    CreateStream(Box<StmtCreateStream>),
    CreateSink(Box<StmtCreateSink>),
    DeleteSource(Box<StmtDeleteSource>),
    DeleteStream(Box<StmtDeleteStream>),
    DeleteSink(Box<StmtDeleteSink>),
    StartStream(Box<StmtStartStream>),
    StopStream(Box<StmtStopStream>),
    Show(Box<StmtShow>),
    Select(Box<StmtSelect>),
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
            value(DataType::String, tag_no_case("string")),
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
            )),
            |(_, _, _, _, name, _, fields, _, _, _, uri, time_by)| StmtCreateSource {
                name,
                uri,
                fields,
                time: time_by,
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

fn stmt_start_stream(input: &str) -> IResult<&str, StmtStartStream> {
    context(
        "stmt_start_stream",
        map(
            tuple((
                alt((
                    value(false, tag_no_case("start")),
                    value(true, tag_no_case("restart")),
                )),
                sp,
                tag_no_case("stream"),
                sp,
                name,
            )),
            |(restart, _, _, _, name)| StmtStartStream { name, restart },
        ),
    )(input)
}

fn stmt_stop_stream(input: &str) -> IResult<&str, StmtStopStream> {
    context(
        "stmt_stop_stream",
        map(
            tuple((tag_no_case("stop"), sp, tag_no_case("stream"), sp, name)),
            |(_, _, _, _, name)| StmtStopStream { name },
        ),
    )(input)
}

fn stmt_show_stream(input: &str) -> IResult<&str, StmtShow> {
    let show_type = alt((
        value(ShowType::Sources, tag_no_case("sources")),
        value(ShowType::Streams, tag_no_case("streams")),
        value(ShowType::Sinks, tag_no_case("sinks")),
    ));

    context(
        "stmt_show_stream",
        map(
            tuple((tag_no_case("show"), sp, show_type)),
            |(_, _, show_type)| StmtShow { show_type },
        ),
    )(input)
}

pub fn stmt(input: &str) -> IResult<&str, Stmt> {
    context(
        "stmt",
        terminated(
            alt((
                map(delimited(sp, stmt_create_source, sp), |stmt| {
                    Stmt::CreateSource(Box::new(stmt))
                }),
                map(delimited(sp, stmt_create_stream, sp), |stmt| {
                    Stmt::CreateStream(Box::new(stmt))
                }),
                map(delimited(sp, stmt_create_sink, sp), |stmt| {
                    Stmt::CreateSink(Box::new(stmt))
                }),
                map(delimited(sp, stmt_delete_source, sp), |stmt| {
                    Stmt::DeleteSource(Box::new(stmt))
                }),
                map(delimited(sp, stmt_delete_stream, sp), |stmt| {
                    Stmt::DeleteStream(Box::new(stmt))
                }),
                map(delimited(sp, stmt_delete_sink, sp), |stmt| {
                    Stmt::DeleteSink(Box::new(stmt))
                }),
                map(delimited(sp, stmt_start_stream, sp), |stmt| {
                    Stmt::StartStream(Box::new(stmt))
                }),
                map(delimited(sp, stmt_stop_stream, sp), |stmt| {
                    Stmt::StopStream(Box::new(stmt))
                }),
                map(delimited(sp, stmt_show_stream, sp), |stmt| {
                    Stmt::Show(Box::new(stmt))
                }),
                map(delimited(sp, select, sp), |select| {
                    Stmt::Select(Box::new(StmtSelect { select }))
                }),
            )),
            eof,
        ),
    )(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use yql_core::sql::ast::{Source, SourceFrom};

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
        "#
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
                        window: None,
                        watermark: None
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
                        window: None,
                        watermark: None
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
