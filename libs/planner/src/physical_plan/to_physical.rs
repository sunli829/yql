use std::sync::Arc;

use anyhow::Result;
use itertools::Itertools;
use yql_array::DataType;
use yql_dataset::{Field, Schema, SchemaRef};
use yql_expr::{Expr, PhysicalExpr};

use crate::logical_plan::{
    LogicalAggregatePlan, LogicalFilterPlan, LogicalPlan, LogicalProjectionPlan, LogicalSourcePlan,
};
use crate::physical_plan::{
    PhysicalAggregateNode, PhysicalFilterNode, PhysicalNode, PhysicalPlan, PhysicalProjectionNode,
    PhysicalSourceNode,
};

struct Context {
    id: usize,
    node_count: usize,
    source_count: usize,
}

impl Context {
    #[inline]
    fn take_id(&mut self) -> usize {
        let id = self.id;
        self.id += 1;
        id
    }
}

fn to_physical(ctx: &mut Context, plan: LogicalPlan) -> Result<PhysicalNode> {
    ctx.node_count += 1;
    match plan {
        LogicalPlan::Source(source) => {
            ctx.source_count += 1;
            source_to_physical(ctx, source)
        }
        LogicalPlan::Projection(projection) => projection_to_physical(ctx, projection),
        LogicalPlan::Filter(filter) => filter_to_physical(ctx, filter),
        LogicalPlan::Aggregate(aggregate) => aggregate_to_physical(ctx, aggregate),
    }
}

fn source_to_physical(ctx: &mut Context, source: LogicalSourcePlan) -> Result<PhysicalNode> {
    let schema = Arc::new(Schema::try_new(
        source
            .provider
            .schema()?
            .fields()
            .to_vec()
            .into_iter()
            .map(|mut field| {
                field.qualifier = source.qualifier.clone();
                field
            })
            .collect(),
    )?);
    Ok(PhysicalNode::Source(PhysicalSourceNode {
        id: ctx.take_id(),
        schema,
        provider: source.provider.clone(),
    }))
}

fn projection_to_physical(
    ctx: &mut Context,
    projection: LogicalProjectionPlan,
) -> Result<PhysicalNode> {
    let input = to_physical(ctx, *projection.input)?;
    let (exprs, schema) = select_expr(projection.exprs, input.schema())?;
    Ok(PhysicalNode::Projection(PhysicalProjectionNode {
        id: ctx.take_id(),
        schema,
        exprs,
        input: Box::new(input),
    }))
}

fn filter_to_physical(ctx: &mut Context, filter: LogicalFilterPlan) -> Result<PhysicalNode> {
    let input = to_physical(ctx, *filter.input)?;
    let expr = PhysicalExpr::try_new(input.schema(), filter.expr)?;

    anyhow::ensure!(
        expr.data_type() == DataType::Boolean,
        "filter expression must return a boolean type."
    );
    Ok(PhysicalNode::Filter(PhysicalFilterNode {
        id: ctx.take_id(),
        schema: input.schema(),
        expr,
        input: Box::new(input),
    }))
}

fn aggregate_to_physical(
    ctx: &mut Context,
    aggregate: LogicalAggregatePlan,
) -> Result<PhysicalNode> {
    let input = to_physical(ctx, *aggregate.input)?;
    let input_schema = input.schema();

    let group_exprs = aggregate
        .group_exprs
        .into_iter()
        .map(|expr| PhysicalExpr::try_new(input.schema(), expr))
        .try_collect()?;

    let (aggr_exprs, schema) = select_expr(aggregate.aggr_exprs, input.schema())?;

    let time_expr = match aggregate.time_expr {
        Some(expr) => {
            let expr = PhysicalExpr::try_new(input_schema.clone(), expr)?;
            anyhow::ensure!(
                expr.data_type().is_timestamp(),
                "window time requires a timestamp type."
            );
            Some(expr)
        }
        None => None,
    };

    let watermark_expr = match aggregate.watermark_expr {
        Some(expr) => {
            let expr = PhysicalExpr::try_new(input_schema, expr)?;
            anyhow::ensure!(
                expr.data_type().is_timestamp(),
                "watermark requires a timestamp type."
            );
            Some(expr)
        }
        None => None,
    };

    Ok(PhysicalNode::Aggregate(PhysicalAggregateNode {
        id: ctx.take_id(),
        schema,
        group_exprs,
        aggr_exprs,
        window: aggregate.window,
        time_expr,
        watermark_expr,
        input: Box::new(input),
    }))
}

fn select_expr(exprs: Vec<Expr>, schema: SchemaRef) -> Result<(Vec<PhysicalExpr>, SchemaRef)> {
    let mut fields = Vec::new();
    let mut physical_exprs = Vec::new();

    for expr in exprs {
        match expr {
            Expr::Wildcard { qualifier } => {
                let select_fields = match qualifier.clone() {
                    Some(qualifier) => schema
                        .fields()
                        .to_vec()
                        .into_iter()
                        .filter(|field| field.qualifier.as_ref() == Some(&qualifier))
                        .collect::<Vec<_>>(),
                    None => schema.fields().to_vec().into_iter().collect(),
                };
                for field in &select_fields {
                    let expr = PhysicalExpr::try_new(
                        schema.clone(),
                        Expr::Column {
                            qualifier: qualifier.clone(),
                            name: field.name.clone(),
                        },
                    )?;
                    physical_exprs.push(expr);
                }
                fields.extend(select_fields.into_iter());
            }
            _ => {
                let field_name = expr.create_name();
                let physical_expr = PhysicalExpr::try_new(schema.clone(), expr)?;
                fields.push(Field {
                    qualifier: None,
                    name: field_name,
                    data_type: physical_expr.data_type(),
                });
            }
        }
    }

    let new_schema = Arc::new(Schema::try_new(fields)?);
    Ok((physical_exprs, new_schema))
}

impl PhysicalPlan {
    pub fn try_new(plan: LogicalPlan) -> Result<PhysicalPlan> {
        let mut ctx = Context {
            id: 0,
            node_count: 0,
            source_count: 0,
        };
        let root = to_physical(&mut ctx, plan)?;
        Ok(PhysicalPlan {
            root,
            source_count: ctx.source_count,
            node_count: ctx.node_count,
        })
    }
}
