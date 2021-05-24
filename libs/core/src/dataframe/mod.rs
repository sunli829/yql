pub mod dsl;

use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;

use crate::dataset::DataSet;
use crate::execution::stream::DataStream;
use crate::expr::Expr;
use crate::planner::logical_plan::{
    LogicalAggregatePlan, LogicalFilterPlan, LogicalPlan, LogicalProjectionPlan, LogicalSourcePlan,
};
use crate::source_provider::SourceProviderWrapper;
use crate::{ExecutionContext, GenericSourceProvider, SinkProvider, Window};

pub struct DataFrame(LogicalPlan);

impl DataFrame {
    pub fn new<T: GenericSourceProvider>(
        name: impl Into<String>,
        source_provider: SourceProviderWrapper<T>,
        qualifier: Option<String>,
        time_expr: Option<Expr>,
        watermark_expr: Option<Expr>,
    ) -> Self {
        Self(LogicalPlan::Source(LogicalSourcePlan {
            name: name.into(),
            qualifier,
            provider: Arc::new(source_provider),
            time_expr,
            watermark_expr,
        }))
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

    pub fn into_stream(self, ctx: ExecutionContext) -> Result<BoxStream<'static, Result<DataSet>>> {
        self.into_stream_with_graceful_shutdown(
            ctx,
            Option::<futures_util::future::Pending<()>>::None,
        )
    }

    pub fn into_stream_with_graceful_shutdown(
        self,
        ctx: ExecutionContext,
        signal: Option<impl Future<Output = ()> + Send + 'static>,
    ) -> Result<BoxStream<'static, Result<DataSet>>> {
        Ok(Box::pin(DataStream::try_new(ctx, self.0, signal)?))
    }

    pub fn into_task(
        self,
        ctx: ExecutionContext,
        sink_provider: impl SinkProvider,
    ) -> Result<BoxFuture<'static, Result<()>>> {
        self.into_task_graceful_shutdown(
            ctx,
            sink_provider,
            Option::<futures_util::future::Pending<()>>::None,
        )
    }

    pub fn into_task_graceful_shutdown(
        self,
        ctx: ExecutionContext,
        sink_provider: impl SinkProvider,
        signal: Option<impl Future<Output = ()> + Send + 'static>,
    ) -> Result<BoxFuture<'static, Result<()>>> {
        let mut stream = self.into_stream_with_graceful_shutdown(ctx, signal)?;
        let mut sink = sink_provider.create()?;
        Ok(Box::pin(async move {
            while let Some(res) = stream.next().await {
                let dataset = res?;
                sink.send(dataset).await?;
            }
            Ok(())
        }))
    }
}
