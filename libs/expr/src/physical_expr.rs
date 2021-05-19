use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use yql_array::{ArrayRef, DataType};
use yql_dataset::DataSet;

use crate::func::StatefulFunction;
use crate::{cast, BinaryOperator, Literal, UnaryOperator};
use std::collections::HashMap;

#[derive(Clone)]
pub enum PhysicalFunction {
    Stateless(fn(&[ArrayRef]) -> Result<ArrayRef>),
    Stateful { id: usize },
}

#[derive(Clone)]
pub enum PhysicalNode {
    Literal(Literal),
    Column {
        index: usize,
    },
    Binary {
        op: BinaryOperator,
        lhs: Box<PhysicalNode>,
        rhs: Box<PhysicalNode>,
    },
    Unary {
        op: UnaryOperator,
        expr: Box<PhysicalNode>,
    },
    Call {
        input_data_types: Vec<DataType>,
        func: PhysicalFunction,
        args: Vec<PhysicalNode>,
    },
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ExprState {
    data: Vec<u8>,
}

#[derive(Clone)]
pub struct PhysicalExpr {
    pub(crate) root: PhysicalNode,
    pub(crate) data_type: DataType,
    pub(crate) stateful_funcs: Vec<Box<dyn StatefulFunction>>,
}

impl PhysicalExpr {
    #[inline]
    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    pub fn eval(&mut self, dataset: &DataSet) -> Result<ArrayRef> {
        internal_eval(&mut self.root, &mut self.stateful_funcs, dataset)
    }

    pub fn save_state(&self) -> Result<ExprState> {
        let mut func_state = HashMap::new();
        for (id, func) in self.stateful_funcs.iter().enumerate() {
            let data = func.save_state()?;
            func_state.insert(id, data);
        }
        let data = bincode::serialize(&func_state)?;
        Ok(ExprState { data })
    }

    pub fn load_state(&mut self, state: ExprState) -> Result<()> {
        let func_state: HashMap<usize, Vec<u8>> = bincode::deserialize(&state.data)?;
        for (id, data) in func_state {
            let func = self
                .stateful_funcs
                .get_mut(id)
                .ok_or_else(|| anyhow::anyhow!("invalid state"))?;
            func.load_state(data)?;
        }
        Ok(())
    }
}

#[inline]
fn internal_eval(
    op: &mut PhysicalNode,
    stateful_funcs: &mut [Box<dyn StatefulFunction>],
    dataset: &DataSet,
) -> Result<ArrayRef> {
    match op {
        PhysicalNode::Literal(literal) => Ok(literal.to_array(dataset.len())),
        PhysicalNode::Column { index } => Ok(dataset.column(*index).context("internal error")?),
        PhysicalNode::Binary { op, lhs, rhs } => {
            let left = internal_eval(lhs, stateful_funcs, dataset)?;
            let right = internal_eval(rhs, stateful_funcs, dataset)?;
            op.eval_array(&*left, &*right)
        }
        PhysicalNode::Unary { op, expr } => {
            let array = internal_eval(expr, stateful_funcs, dataset)?;
            op.eval_array(&*array)
        }
        PhysicalNode::Call {
            input_data_types,
            func,
            args,
        } => {
            let mut arg_values = Vec::with_capacity(args.len());
            for (expr, data_type) in args.iter_mut().zip(input_data_types) {
                arg_values.push(cast::array_cast_to(
                    internal_eval(expr, stateful_funcs, dataset)?,
                    *data_type,
                )?);
            }
            match func {
                PhysicalFunction::Stateless(func) => func(&arg_values),
                PhysicalFunction::Stateful { id } => {
                    let func = &mut stateful_funcs[*id];
                    func.call(&arg_values)
                }
            }
        }
    }
}
