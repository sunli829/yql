pub mod dsl;

use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use yql_dataset::dataset::DataSet;

use crate::execution::stream::create_data_stream;
use crate::expr::Expr;
use crate::planner::logical_plan::{
    LogicalAggregatePlan, LogicalFilterPlan, LogicalPlan, LogicalProjectionPlan, LogicalSourcePlan,
};
use crate::sql::ast::Select;
use crate::sql::SqlContext;
use crate::{ExecutionContext, SinkProvider, SourceProvider, Window};

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

    pub fn into_stream(self, ctx: Arc<ExecutionContext>) -> BoxStream<'static, Result<DataSet>> {
        self.into_stream_with_graceful_shutdown(
            ctx,
            Option::<futures_util::future::Pending<()>>::None,
        )
    }

    pub fn into_stream_with_graceful_shutdown(
        self,
        ctx: Arc<ExecutionContext>,
        signal: Option<impl Future<Output = ()> + Send + 'static>,
    ) -> BoxStream<'static, Result<DataSet>> {
        create_data_stream(ctx, self.0, signal)
    }

    pub fn into_task(
        self,
        ctx: Arc<ExecutionContext>,
        sink_provider: impl SinkProvider,
    ) -> BoxFuture<'static, Result<()>> {
        self.into_task_with_graceful_shutdown(
            ctx,
            sink_provider,
            Option::<futures_util::future::Pending<()>>::None,
        )
    }

    pub fn into_task_with_graceful_shutdown(
        self,
        ctx: Arc<ExecutionContext>,
        sink_provider: impl SinkProvider,
        signal: Option<impl Future<Output = ()> + Send + 'static>,
    ) -> BoxFuture<'static, Result<()>> {
        let mut stream = self.into_stream_with_graceful_shutdown(ctx, signal);
        Box::pin(async move {
            let mut sink = sink_provider.create()?;
            while let Some(res) = stream.next().await {
                let dataset = res?;
                sink.send(dataset).await?;
            }
            Ok(())
        })
    }
}
