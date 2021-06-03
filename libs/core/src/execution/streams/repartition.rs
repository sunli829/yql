use anyhow::Result;
use tokio::sync::mpsc::UnboundedSender;

use crate::dataset::{DataSet, SchemaRef};
use crate::execution::dataset::{DataSetExt, GroupedKey};
use crate::execution::stream::{BoxDataSetStream, CreateStreamContext, DataSetWithWatermark};
use crate::expr::physical_expr::PhysicalExpr;
use crate::planner::physical_plan::{PhysicalPartitioning, PhysicalRepartitionNode};
use ahash::AHashMap;

pub fn create_repartition_stream(
    create_ctx: &mut CreateStreamContext,
    node: PhysicalRepartitionNode,
) -> Result<BoxDataSetStream> {
    todo!()
}

enum Partitioning {
    RoundRobin {
        current: usize,
        tx: Vec<UnboundedSender<DataSetWithWatermark>>,
    },
    Hash {
        exprs: Vec<PhysicalExpr>,
        tx: Vec<UnboundedSender<DataSetWithWatermark>>,
    },
    Group {
        exprs: Vec<PhysicalExpr>,
        tx: AHashMap<GroupedKey, UnboundedSender<DataSetWithWatermark>>,
    },
}

struct RepartitionStream {
    id: usize,
    schema: SchemaRef,
    partitioning: PhysicalPartitioning,
    exprs: Vec<PhysicalExpr>,
    input: BoxDataSetStream,
}

impl RepartitionStream {
    fn load_state(&mut self, data: Vec<u8>) -> Result<()> {
        match &mut self.partitioning {
            Partitioning::RoundRobin { current, .. } => {
                *current = bincode::deserialize(&data)?;
            }
            Partitioning::Hash { exprs, .. } | Partitioning::Group { exprs, .. } => {
                let state: Vec<Vec<u8>> = bincode::deserialize(&data)?;
                for (expr, state_data) in exprs.iter_mut().zip(state) {
                    expr.load_state(state_data)?;
                }
            }
        }
        Ok(())
    }

    fn process_dataset(&mut self, dataset: &DataSet) -> Result<DataSet> {
        match &mut self.partitioning {
            Partitioning::RoundRobin { current, tx } => {}
            Partitioning::Hash { exprs, tx } => {}
            Partitioning::Group { exprs, tx } => {}
        }
        todo!()
    }
}
