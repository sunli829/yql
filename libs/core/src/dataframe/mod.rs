pub mod dsl;

use anyhow::Result;

use crate::execution::stream::DataStream;
use crate::expr::Expr;
use crate::planner::logical_plan::{
    LogicalAggregatePlan, LogicalFilterPlan, LogicalPlan, LogicalProjectionPlan, LogicalSourcePlan,
};
use crate::sql::ast::Select;
use crate::sql::SqlContext;
use crate::{SourceProvider, Window};

pub struct DataFrame(LogicalPlan);

impl DataFrame {
    pub fn new(
        source_provider: SourceProvider,
        qualifier: Option<String>,
        time_expr: Option<Expr>,
        watermark_expr: Option<Expr>,
    ) -> Self {
        Self(LogicalPlan::Source(LogicalSourcePlan {
            qualifier,
            source_provider,
            time_expr,
            watermark_expr,
        }))
    }

    pub fn from_sql(ctx: &dyn SqlContext, sql: &str) -> Result<Self> {
        crate::sql::planner::create_data_frame_with_sql(ctx, sql)
    }

    pub fn from_sql_select(ctx: &dyn SqlContext, select: Select) -> Result<Self> {
        crate::sql::planner::create_data_frame(ctx, select)
    }

    pub fn select(self, exprs: Vec<Expr>) -> Self {
        Self(LogicalPlan::Projection(LogicalProjectionPlan {
            exprs,
            input: Box::new(self.0),
        }))
    }

    pub fn filter(self, expr: Expr) -> Self {
        Self(LogicalPlan::Filter(LogicalFilterPlan {
            expr,
            input: Box::new(self.0),
        }))
    }

    pub fn aggregate(self, group_exprs: Vec<Expr>, aggr_exprs: Vec<Expr>, window: Window) -> Self {
        Self(LogicalPlan::Aggregate(LogicalAggregatePlan {
            group_exprs,
            aggr_exprs,
            window,
            input: Box::new(self.0),
        }))
    }

    pub fn into_stream(self, state: Option<Vec<u8>>) -> Result<DataStream> {
        DataStream::new(self.0, state)
    }
}
