use crate::{Expr, PhysicalExpr};

use anyhow::Error;
use yql_array::DataType;
use yql_dataset::SchemaRef;

use crate::func::{FunctionType, StatefulFunction};
use crate::funcs::FUNCS;
use crate::physical_expr::{PhysicalFunction, PhysicalOp};

pub type Result<T, E = Error> = std::result::Result<(T, DataType), E>;

struct Context {
    schema: SchemaRef,
    stateful_funcs: Vec<Box<dyn StatefulFunction>>,
}

fn to_physical(ctx: &mut Context, expr: Expr) -> Result<PhysicalOp> {
    match expr {
        Expr::Literal(literal) => {
            let data_type = literal.data_type();
            Ok((PhysicalOp::Literal(literal), data_type))
        }
        Expr::Column { qualifier, name } => match ctx.schema.field(qualifier.as_deref(), &name) {
            Some((index, field)) => Ok((PhysicalOp::Column { index }, field.data_type)),
            None => match qualifier {
                Some(qualifier) => anyhow::bail!("not such column: '{}.{}'", qualifier, name),
                None => anyhow::bail!("not such column: '{}'", name),
            },
        },
        Expr::Binary { op, lhs, rhs } => {
            let (lhs, lhs_data_type) = to_physical(ctx, *lhs)?;
            let (rhs, rhs_data_type) = to_physical(ctx, *rhs)?;
            let data_type = op.data_type(lhs_data_type, rhs_data_type)?;
            Ok((
                PhysicalOp::Binary {
                    op,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                },
                data_type,
            ))
        }
        Expr::Unary { op, expr } => {
            let (expr, data_type) = to_physical(ctx, *expr)?;
            let data_type = op.data_type(data_type)?;
            Ok((
                PhysicalOp::Unary {
                    op,
                    expr: Box::new(expr),
                },
                data_type,
            ))
        }
        Expr::Call {
            name,
            args: arguments,
        } => {
            let func = match FUNCS
                .iter()
                .find(|func| func.name.eq_ignore_ascii_case(&name))
            {
                Some(func) => func,
                None => anyhow::bail!("no such function: '{}'", name),
            };

            let mut arg_exprs = Vec::new();
            let mut arg_data_types = Vec::new();
            for arg in arguments {
                let (expr, data_type) = to_physical(ctx, arg)?;
                arg_exprs.push(expr);
                arg_data_types.push(data_type);
            }

            let input_data_types = func
                .signature
                .data_types(&arg_data_types)
                .map_err(|_| anyhow::anyhow!("misuse function: {}", func.name))?;
            let return_data_type = (func.return_type)(&input_data_types);

            let call = PhysicalOp::Call {
                input_data_types,
                func: match &func.function_type {
                    FunctionType::Stateless(f) => PhysicalFunction::Stateless(*f),
                    FunctionType::Stateful(f) => {
                        let id = ctx.stateful_funcs.len() as usize;
                        ctx.stateful_funcs.push(f());
                        PhysicalFunction::Stateful { id }
                    }
                },
                args: arg_exprs,
            };
            Ok((call, return_data_type))
        }
        Expr::Alias(expr, _) => to_physical(ctx, *expr),
        Expr::Wildcard { .. } => anyhow::bail!("invalid wildcard position"),
    }
}

impl PhysicalExpr {
    pub fn try_new(schema: SchemaRef, expr: Expr) -> anyhow::Result<Self> {
        let mut ctx = Context {
            schema,
            stateful_funcs: Vec::new(),
        };
        let (root, data_type) = to_physical(&mut ctx, expr)?;
        Ok(Self {
            root,
            data_type,
            stateful_funcs: ctx.stateful_funcs,
        })
    }
}
