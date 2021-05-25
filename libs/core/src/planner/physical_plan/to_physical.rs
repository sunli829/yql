use std::sync::Arc;

use anyhow::Result;
use itertools::Itertools;
use yql_dataset::array::DataType;
use yql_dataset::dataset::{Field, Schema, SchemaRef};

use crate::expr::physical_expr::PhysicalExpr;
use crate::expr::Expr;
use crate::planner::logical_plan::{
    LogicalAggregatePlan, LogicalFilterPlan, LogicalPlan, LogicalProjectionPlan, LogicalSourcePlan,
};
use crate::planner::physical_plan::{
    PhysicalAggregateNode, PhysicalFilterNode, PhysicalNode, PhysicalPlan, PhysicalProjectionNode,
    PhysicalSourceNode, FIELD_TIME,
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
    let source_schema = source.source_provider.schema()?;
    let schema = Arc::new(Schema::try_new(
        source_schema
            .fields()
            .to_vec()
            .into_iter()
            .chain(std::iter::once(Field {
                qualifier: None,
                name: FIELD_TIME.to_string(),
                data_type: DataType::Null,
            }))
            .map(|mut field| {
                field.qualifier = source.qualifier.clone();
                field
            })
            .collect(),
    )?);
    Ok(PhysicalNode::Source(PhysicalSourceNode {
        id: ctx.take_id(),
        schema,
        source_provider: source.source_provider,
        time_expr: match source.time_expr {
            Some(expr) => Some(expr.into_physical(source_schema.clone())?),
            None => None,
        },
        watermark_expr: match source.watermark_expr {
            Some(expr) => Some(expr.into_physical(source_schema)?),
            None => None,
        },
    }))
}

fn projection_to_physical(
    ctx: &mut Context,
    projection: LogicalProjectionPlan,
) -> Result<PhysicalNode> {
    let input = to_physical(ctx, *projection.input)?;
    let (exprs, schema) = select_expr(projection.exprs, input.schema(), vec![])?;
    Ok(PhysicalNode::Projection(PhysicalProjectionNode {
        id: ctx.take_id(),
        schema,
        exprs,
        input: Box::new(input),
    }))
}

fn filter_to_physical(ctx: &mut Context, filter: LogicalFilterPlan) -> Result<PhysicalNode> {
    let input = to_physical(ctx, *filter.input)?;
    let expr = filter.expr.into_physical(input.schema())?;

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
    let (time_idx, timezone) = match input.schema().field(None, FIELD_TIME) {
        Some((idx, Field { data_type:DataType::Timestamp(timezone), .. })) => {
            (idx, *timezone)
        },
        _ => anyhow::bail!("A column whose name is '@time' and type is 'timestamp' is required to perform aggregation operations."),
    };

    let group_exprs = aggregate
        .group_exprs
        .into_iter()
        .map(|expr| expr.into_physical(input.schema()))
        .try_collect()?;
    let (aggr_exprs, schema) = select_expr(
        aggregate.aggr_exprs,
        input.schema(),
        vec![Field::new(FIELD_TIME, DataType::Timestamp(timezone))],
    )?;

    Ok(PhysicalNode::Aggregate(PhysicalAggregateNode {
        id: ctx.take_id(),
        schema,
        group_exprs,
        aggr_exprs,
        window: aggregate.window,
        time_idx,
        input: Box::new(input),
    }))
}

fn select_expr(
    exprs: Vec<Expr>,
    schema: SchemaRef,
    extra_fields: Vec<Field>,
) -> Result<(Vec<PhysicalExpr>, SchemaRef)> {
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
                    let expr = Expr::Column {
                        qualifier: qualifier.clone(),
                        name: field.name.clone(),
                    }
                    .into_physical(schema.clone())?;
                    physical_exprs.push(expr);
                }
                fields.extend(select_fields.into_iter());
            }
            _ => {
                let field_name = expr.create_name();
                let physical_expr = expr.into_physical(schema.clone())?;
                fields.push(Field {
                    qualifier: None,
                    name: field_name,
                    data_type: physical_expr.data_type(),
                });
            }
        }
    }

    fields.extend(extra_fields);
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
