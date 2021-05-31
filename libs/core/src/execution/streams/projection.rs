use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::Result;
use futures_util::{Stream, StreamExt};
use itertools::Itertools;

use crate::dataset::{DataSet, SchemaRef};
use crate::execution::stream::{
    BoxDataSetStream, CreateStreamContext, DataSetStream, DataSetWithWatermark,
};
use crate::execution::streams::create_stream;
use crate::expr::physical_expr::PhysicalExpr;
use crate::planner::physical_plan::PhysicalProjectionNode;

pub fn create_projection_stream(
    create_ctx: &mut CreateStreamContext,
    node: PhysicalProjectionNode,
) -> Result<BoxDataSetStream> {
    let PhysicalProjectionNode {
        id,
        schema,
        exprs,
        input,
    } = node;

    let mut stream = ProjectionStream {
        id,
        schema,
        exprs,
        input: create_stream(create_ctx, *input)?,
    };
    if let Some(data) = create_ctx.prev_state.remove(&id) {
        stream.load_state(data)?;
    }

    Ok(Box::pin(stream))
}

struct ProjectionStream {
    id: usize,
    schema: SchemaRef,
    exprs: Vec<PhysicalExpr>,
    input: BoxDataSetStream,
}

impl ProjectionStream {
    fn load_state(&mut self, data: Vec<u8>) -> Result<()> {
        let state: Vec<Vec<u8>> = bincode::deserialize(&data)?;
        for (expr, state_data) in self.exprs.iter_mut().zip(state) {
            expr.load_state(state_data)?;
        }
        Ok(())
    }

    fn process_dataset(&mut self, dataset: &DataSet) -> Result<DataSet> {
        let mut columns = Vec::with_capacity(self.exprs.len());
        for expr in &mut self.exprs {
            columns.push(expr.eval(&dataset)?);
        }
        DataSet::try_new(self.schema.clone(), columns)
    }
}

impl DataSetStream for ProjectionStream {
    fn save_state(&self, state: &mut HashMap<usize, Vec<u8>>) -> Result<()> {
        let exprs_state = self
            .exprs
            .iter()
            .map(|expr| expr.save_state())
            .try_collect::<_, Vec<_>, _>()?;
        state.insert(self.id, bincode::serialize(&exprs_state)?);
        Ok(())
    }
}

impl Stream for ProjectionStream {
    type Item = Result<DataSetWithWatermark>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(DataSetWithWatermark { watermark, dataset }))) => {
                match self.process_dataset(&dataset) {
                    Ok(new_dataset) => Poll::Ready(Some(Ok(DataSetWithWatermark {
                        watermark,
                        dataset: new_dataset,
                    }))),
                    Err(err) => Poll::Ready(Some(Err(err))),
                }
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
