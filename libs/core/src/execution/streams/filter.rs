use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::Result;
use futures_util::Stream;
use futures_util::StreamExt;

use crate::array::{ArrayExt, BooleanArray};
use crate::dataset::DataSet;
use crate::execution::stream::{
    BoxDataSetStream, CreateStreamContext, DataSetStream, DataSetWithWatermark,
};
use crate::execution::streams::create_stream;
use crate::expr::physical_expr::PhysicalExpr;
use crate::planner::physical_plan::PhysicalFilterNode;

pub fn create_filter_stream(
    create_ctx: &mut CreateStreamContext,
    node: PhysicalFilterNode,
) -> Result<BoxDataSetStream> {
    let PhysicalFilterNode {
        id, expr, input, ..
    } = node;

    let mut stream = FilterStream {
        id,
        expr,
        input: create_stream(create_ctx, *input)?,
    };
    if let Some(data) = create_ctx.prev_state.remove(&id) {
        stream.load_state(data)?;
    }

    Ok(Box::pin(stream))
}

struct FilterStream {
    id: usize,
    expr: PhysicalExpr,
    input: BoxDataSetStream,
}

impl FilterStream {
    fn load_state(&mut self, data: Vec<u8>) -> Result<()> {
        self.expr.load_state(data)
    }

    fn process_dataset(&mut self, dataset: &DataSet) -> Result<DataSet> {
        let array = self.expr.eval(&dataset)?;
        dataset.filter(array.downcast_ref::<BooleanArray>())
    }
}

impl DataSetStream for FilterStream {
    fn save_state(&self, state: &mut HashMap<usize, Vec<u8>>) -> Result<()> {
        self.input.save_state(state)?;

        state.insert(self.id, self.expr.save_state()?);
        Ok(())
    }
}

impl Stream for FilterStream {
    type Item = Result<DataSetWithWatermark>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(DataSetWithWatermark { watermark, dataset }))) => {
                    match self.process_dataset(&dataset) {
                        Ok(new_dataset) if !new_dataset.is_empty() => {
                            return Poll::Ready(Some(Ok(DataSetWithWatermark {
                                watermark,
                                dataset: new_dataset,
                            })));
                        }
                        Ok(_) => {}
                        Err(err) => return Poll::Ready(Some(Err(err))),
                    }
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
