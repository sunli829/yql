use std::fmt::{self, Display, Formatter};

use chrono::TimeZone;
use comfy_table::presets::UTF8_HORIZONTAL_BORDERS_ONLY;
use comfy_table::{Cell, ContentArrangement, Row, Table, TableComponent};

use crate::array::{
    ArrayExt, BooleanArray, DataType, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray, TimestampArray,
};
use crate::dataset::DataSet;

macro_rules! add_table_cell {
    ($table_row:expr, $dataset:expr, $row:expr, $column:expr, $ty:ty) => {
        $table_row.add_cell(Cell::new(
            $dataset.columns()[$column]
                .downcast_ref::<$ty>()
                .value($row),
        ))
    };
}

pub struct DataSetDisplay<'a> {
    dataset: &'a DataSet,
    no_header: bool,
}

impl DataSet {
    pub fn display(&self) -> DataSetDisplay<'_> {
        DataSetDisplay {
            dataset: self,
            no_header: false,
        }
    }

    pub fn display_no_header(&self) -> DataSetDisplay<'_> {
        DataSetDisplay {
            dataset: self,
            no_header: true,
        }
    }
}

impl<'a> Display for DataSetDisplay<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut table = Table::new();
        table.set_content_arrangement(ContentArrangement::DynamicFullWidth);
        table.load_preset(UTF8_HORIZONTAL_BORDERS_ONLY);

        if !self.no_header {
            table.set_header(
                self.dataset
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| &field.name),
            );
        } else {
            table.remove_style(TableComponent::TopBorder);
            table.remove_style(TableComponent::TopBorderIntersections);
        }

        if self.dataset.is_empty() {
            table.add_row(Row::from(vec!["No data!"]));
        } else {
            for row in 0..self.dataset.len() {
                let mut table_row = Row::new();

                for (column, field) in self.dataset.schema().fields().iter().enumerate() {
                    let _ = match field.data_type {
                        DataType::Null => table_row.add_cell(Cell::new("null")),
                        DataType::Int8 => {
                            add_table_cell!(table_row, self.dataset, row, column, Int8Array)
                        }
                        DataType::Int16 => {
                            add_table_cell!(table_row, self.dataset, row, column, Int16Array)
                        }
                        DataType::Int32 => {
                            add_table_cell!(table_row, self.dataset, row, column, Int32Array)
                        }
                        DataType::Int64 => {
                            add_table_cell!(table_row, self.dataset, row, column, Int64Array)
                        }
                        DataType::Float32 => {
                            add_table_cell!(table_row, self.dataset, row, column, Float32Array)
                        }
                        DataType::Float64 => {
                            add_table_cell!(table_row, self.dataset, row, column, Float64Array)
                        }
                        DataType::Boolean => {
                            add_table_cell!(table_row, self.dataset, row, column, BooleanArray)
                        }
                        DataType::String => {
                            add_table_cell!(table_row, self.dataset, row, column, StringArray)
                        }
                        DataType::Timestamp(Some(tz)) => table_row.add_cell(Cell::new(
                            tz.timestamp_millis(
                                self.dataset.columns()[column]
                                    .as_any()
                                    .downcast_ref::<TimestampArray>()
                                    .unwrap()
                                    .value(row),
                            ),
                        )),
                        DataType::Timestamp(None) => table_row.add_cell(Cell::new(
                            chrono::Local.timestamp_millis(
                                self.dataset.columns()[column]
                                    .as_any()
                                    .downcast_ref::<TimestampArray>()
                                    .unwrap()
                                    .value(row),
                            ),
                        )),
                    };
                }

                table.add_row(table_row);
            }
        }

        table.fmt(f)
    }
}
