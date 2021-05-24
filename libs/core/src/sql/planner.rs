use anyhow::Result;
use nom::combinator::{eof, map};
use nom::sequence::tuple;

use crate::sql::ast::{Select, Source, SourceFrom};
use crate::sql::parser::sp;
use crate::sql::SqlContext;
use crate::DataFrame;

pub fn create_data_frame_with_sql(ctx: &dyn SqlContext, sql: &str) -> Result<DataFrame> {
    let (_, select) = map(
        tuple((sp, crate::sql::parser::select, sp, eof)),
        |(_, select, _, _)| select,
    )(sql)
    .map_err(|err| anyhow::anyhow!("{}", err))?;
    create_data_frame(ctx, select)
}

pub fn create_data_frame(ctx: &dyn SqlContext, select: Select) -> Result<DataFrame> {
    let mut df = create_source(ctx, select.source)?;
    if let Some(condition) = select.where_clause {
        df = df.filter(condition);
    }

    match (select.group_clause, select.window) {
        (Some(group_by), Some(window)) => {
            df = df.aggregate(group_by.exprs, select.projection, window);
        }
        (None, Some(window)) => {
            df = df.aggregate(vec![], select.projection, window);
        }
        (Some(_), None) => {
            anyhow::bail!("the window clause is missing.");
        }
        (None, None) => {
            df = df.select(select.projection);
        }
    }

    if let Some(condition) = select.having_clause {
        df = df.filter(condition);
    }

    Ok(df)
}

fn create_source(ctx: &dyn SqlContext, source: Source) -> Result<DataFrame> {
    match source.from {
        SourceFrom::Named(name) => {
            let provider = ctx
                .create_source_provider(&name)
                .ok_or_else(|| anyhow::anyhow!("source '{}' not found.", name))?;
            Ok(DataFrame::new(
                provider.source_provider,
                source.alias,
                provider.time_expr,
                provider.watermark_expr,
            ))
        }
        SourceFrom::SubQuery(select) => create_data_frame(ctx, *select),
    }
}
