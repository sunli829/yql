use std::sync::Arc;

use ahash::AHashMap;
use anyhow::Result;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::time::{SystemTime, UNIX_EPOCH};
use yql_array::{
    Array, ArrayExt, ArrayRef, BooleanArray, BooleanType, DataType, Float32Type, Float64Type,
    Int16Type, Int32Type, Int64Type, Int8Type, NullArray, PrimitiveArray, PrimitiveBuilder,
    StringArray, StringBuilder, TimestampArray, TimestampType,
};
use yql_dataset::DataSet;
use yql_expr::PhysicalExpr;
use yql_planner::Window;

macro_rules! fill_integer_key {
    ($record_keys:expr, $array:expr, $ty:ty, $num_columns:expr, $column:expr) => {{
        let array = $array.downcast_ref::<PrimitiveArray<$ty>>();
        for row in 0..array.len() {
            if let Some(value) = array.value_opt(row) {
                $record_keys[row * $num_columns + $column] = Key::Int(value as i64);
            }
        }
    }};
}

macro_rules! fill_float_key {
    ($record_keys:expr, $array:expr, $ty:ty, $num_columns:expr, $column:expr) => {{
        let array = $array.downcast_ref::<PrimitiveArray<$ty>>();
        for row in 0..array.len() {
            if let Some(value) = array.value_opt(row) {
                $record_keys[row * $num_columns + $column] = Key::Float(OrderedFloat(value as f64));
            }
        }
    }};
}

macro_rules! copy_grouped_primitive_values {
    ($array:expr, $indexes:expr, $ty:ty) => {{
        let array = $array.downcast_ref::<PrimitiveArray<$ty>>();
        let mut builder = PrimitiveBuilder::<$ty>::with_capacity($indexes.len());
        for index in $indexes {
            builder.append_opt(array.value_opt(*index));
        }
        Arc::new(builder.finish()) as ArrayRef
    }};
}

#[derive(Debug, Hash, Eq, PartialEq, Clone, Serialize, Deserialize)]
enum Key {
    Null,
    Boolean(bool),
    Int(i64),
    Float(OrderedFloat<f64>),
    String(String),
}

#[derive(Default, Eq, PartialEq, Hash, Clone, Serialize, Deserialize)]
pub struct GroupedKey(SmallVec<[Key; 4]>);

pub type GroupByExprsIter<'a> = Box<dyn Iterator<Item = Result<(GroupedKey, DataSet)>> + 'a>;

pub fn group_by_exprs<'a>(
    dataset: &'a DataSet,
    exprs: &mut [PhysicalExpr],
) -> Result<GroupByExprsIter<'a>> {
    let num_group_exprs = exprs.len();
    let keys = exprs
        .iter_mut()
        .map(|expr| expr.eval(dataset))
        .try_collect::<_, Vec<_>, _>()?;
    let mut record_keys = vec![Key::Null; keys.len() * dataset.len()];

    for (column, array) in keys.into_iter().enumerate() {
        match array.data_type() {
            DataType::Null => {}
            DataType::Int8 => {
                fill_integer_key!(record_keys, array, Int8Type, num_group_exprs, column)
            }
            DataType::Int16 => {
                fill_integer_key!(record_keys, array, Int16Type, num_group_exprs, column)
            }
            DataType::Int32 => {
                fill_integer_key!(record_keys, array, Int32Type, num_group_exprs, column)
            }
            DataType::Int64 => {
                fill_integer_key!(record_keys, array, Int64Type, num_group_exprs, column)
            }
            DataType::Float32 => {
                fill_float_key!(record_keys, array, Float32Type, num_group_exprs, column)
            }
            DataType::Float64 => {
                fill_float_key!(record_keys, array, Float64Type, num_group_exprs, column)
            }
            DataType::Boolean => {
                let array = array.downcast_ref::<BooleanArray>();
                for row in 0..array.len() {
                    if let Some(value) = array.value_opt(row) {
                        record_keys[row * num_group_exprs + column] = Key::Boolean(value);
                    }
                }
            }
            DataType::Timestamp(_) => {
                fill_integer_key!(record_keys, array, TimestampType, num_group_exprs, column)
            }
            DataType::String => {
                let array = array.downcast_ref::<StringArray>();
                for row in 0..array.len() {
                    if let Some(value) = array.value_opt(row) {
                        record_keys[row * num_group_exprs + column] =
                            Key::String(value.to_string());
                    }
                }
            }
        }
    }

    let mut keys_map: AHashMap<_, Vec<usize>> = AHashMap::new();
    for row in 0..dataset.len() {
        let mut grouped_key = GroupedKey::default();
        for value in record_keys[row * num_group_exprs..(row + 1) * num_group_exprs].iter_mut() {
            grouped_key.0.push(std::mem::replace(value, Key::Null));
        }
        keys_map.entry(grouped_key).or_default().push(row);
    }

    Ok(Box::new(keys_map.into_iter().map(move |(key, indexes)| {
        create_dataset(dataset, &indexes).map(|dataset| (key, dataset))
    })))
}

pub type GroupByWindowIter<'a> = Box<dyn Iterator<Item = Result<(i64, i64, DataSet)>> + 'a>;

pub fn group_by_window<'a>(
    dataset: &'a DataSet,
    time_expr: Option<&mut PhysicalExpr>,
    watermark_expr: Option<&mut PhysicalExpr>,
    current_watermark: &mut Option<i64>,
    window: &Window,
) -> Result<GroupByWindowIter<'a>> {
    let mut windows: AHashMap<_, (i64, Vec<usize>)> = AHashMap::new();

    let times = match time_expr {
        Some(expr) => expr.eval(dataset)?,
        None => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            Arc::new(TimestampArray::new_scalar(dataset.len(), Some(now)))
        }
    };
    let watermarks = match watermark_expr {
        Some(expr) => expr.eval(dataset)?,
        None => times.clone(),
    };

    let times = times.downcast_ref::<TimestampArray>();
    let watermarks = watermarks.downcast_ref::<TimestampArray>();

    for (idx, timestamp, watermark) in times
        .iter_opt()
        .zip(watermarks.iter_opt())
        .enumerate()
        .filter_map(|(idx, (timestamp, watermark))| {
            timestamp.map(|timestamp| (idx, timestamp, watermark))
        })
    {
        let watermark = watermark.unwrap_or(timestamp);

        // update watermark
        match current_watermark {
            Some(current_watermark) => {
                if watermark < *current_watermark {
                    continue;
                }
                *current_watermark = watermark;
            }
            None => {
                *current_watermark = Some(watermark);
            }
        }

        for (start, end) in window.windows(timestamp) {
            let window = windows.entry(start).or_default();
            window.0 = end;
            window.1.push(idx);
        }
    }

    Ok(Box::new(windows.into_iter().map(
        move |(start, (end, indexes))| {
            create_dataset(dataset, &indexes).map(|dataset| (start, end, dataset))
        },
    )))
}

fn create_dataset(dataset: &DataSet, indexes: &[usize]) -> Result<DataSet> {
    let mut columns = Vec::with_capacity(dataset.schema().fields().len());
    for array in dataset.columns() {
        let new_array = match array.data_type() {
            DataType::Null => Arc::new(NullArray::new(indexes.len())) as ArrayRef,
            DataType::Int8 => copy_grouped_primitive_values!(array, indexes, Int8Type),
            DataType::Int16 => copy_grouped_primitive_values!(array, indexes, Int16Type),
            DataType::Int32 => copy_grouped_primitive_values!(array, indexes, Int32Type),
            DataType::Int64 => copy_grouped_primitive_values!(array, indexes, Int64Type),
            DataType::Float32 => copy_grouped_primitive_values!(array, indexes, Float32Type),
            DataType::Float64 => copy_grouped_primitive_values!(array, indexes, Float64Type),
            DataType::Boolean => copy_grouped_primitive_values!(array, indexes, BooleanType),
            DataType::Timestamp(_) => {
                copy_grouped_primitive_values!(array, indexes, TimestampType)
            }
            DataType::String => {
                let array = array.downcast_ref::<StringArray>();
                let mut builder = StringBuilder::with_capacity(indexes.len());
                for index in indexes {
                    builder.append_opt(array.value_opt(*index));
                }
                Arc::new(builder.finish()) as ArrayRef
            }
        };
        columns.push(new_array);
    }
    DataSet::try_new(dataset.schema(), columns)
}
