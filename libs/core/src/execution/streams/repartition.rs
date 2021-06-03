use ahash::AHashMap;
use anyhow::Result;
use tokio::sync::mpsc::UnboundedSender;

use crate::dataset::{DataSet, SchemaRef};
use crate::execution::dataset::{DataSetExt, GroupedKey};
use crate::execution::stream::{BoxDataSetStream, CreateStreamContext};
use crate::expr::physical_expr::PhysicalExpr;
use crate::planner::physical_plan::PhysicalRepartitionNode;

pub fn create_repartition_stream(
    create_ctx: &mut CreateStreamContext,
    node: PhysicalRepartitionNode,
) -> Result<BoxDataSetStream> {
    todo!()
}

enum Partitioning {
    RoundRobin {
        current: usize,
        count: usize,
        children: Vec<UnboundedSender<DataSet>>,
    },
    Hash {
        exprs: Vec<PhysicalExpr>,
        count: usize,
        children: Vec<Option<UnboundedSender<DataSet>>>,
    },
    Group {
        exprs: Vec<PhysicalExpr>,
        children: AHashMap<GroupedKey, UnboundedSender<DataSet>>,
    },
}

struct RepartitionStream {
    id: usize,
    schema: SchemaRef,
    partitioning: Partitioning,
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

    fn spawn_process(&mut self) -> UnboundedSender<DataSet> {
        todo!()
    }

    fn process_dataset(&mut self, dataset: DataSet) -> Result<()> {
        match &mut self.partitioning {
            Partitioning::RoundRobin {
                current,
                count,
                children,
            } => {
                if *current < *count - 1 {
                    let tx = self.spawn_process();
                    children.push(tx.clone());
                }
                children[*current].send(dataset)?;
                *current += 1;
                if *current >= *count {
                    *current = 0;
                }
            }
            Partitioning::Hash {
                exprs,
                count,
                children,
            } => {
                for res in dataset.hash_group_by_exprs(exprs)? {
                    let (hash, dataset) = res?;
                    match children.get_mut(hash as usize % *count) {
                        Some(Some(tx)) => {
                            tx.send(dataset)?;
                        }
                        Some(child @ None) => {
                            let tx = self.spawn_process();
                            *child = Some(tx.clone());
                            tx.send(dataset)?;
                        }
                        None => unreachable!(),
                    }
                }
            }
            Partitioning::Group { exprs, children } => {
                for res in dataset.group_by_exprs(exprs)? {
                    let (grouped_key, dataset) = res?;
                    match children.get_mut(&grouped_key) {
                        Some(tx) => tx.send(dataset)?,
                        None => {
                            let tx = self.spawn_process();
                            tx.send(dataset)?;
                            children.insert(grouped_key, tx);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
