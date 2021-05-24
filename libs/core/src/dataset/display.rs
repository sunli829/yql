use std::fmt::{self, Display, Formatter};

use chrono::TimeZone;
use comfy_table::presets::UTF8_HORIZONTAL_BORDERS_ONLY;
use comfy_table::{Cell, Row, Table};

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

impl Display for DataSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut table = Table::new();
        table.load_preset(UTF8_HORIZONTAL_BORDERS_ONLY);

        table.set_header(self.schema().fields().iter().map(|field| &field.name));

        for row in 0..self.len() {
            let mut table_row = Row::new();

            for (column, field) in self.schema().fields().iter().enumerate() {
                let _ = match field.data_type {
                    DataType::Null => table_row.add_cell(Cell::new("null")),
                    DataType::Int8 => add_table_cell!(table_row, self, row, column, Int8Array),
                    DataType::Int16 => add_table_cell!(table_row, self, row, column, Int16Array),
                    DataType::Int32 => add_table_cell!(table_row, self, row, column, Int32Array),
                    DataType::Int64 => add_table_cell!(table_row, self, row, column, Int64Array),
                    DataType::Float32 => {
                        add_table_cell!(table_row, self, row, column, Float32Array)
                    }
                    DataType::Float64 => {
                        add_table_cell!(table_row, self, row, column, Float64Array)
                    }
                    DataType::Boolean => {
                        add_table_cell!(table_row, self, row, column, BooleanArray)
                    }
                    DataType::String => add_table_cell!(table_row, self, row, column, StringArray),
                    DataType::Timestamp(Some(tz)) => table_row.add_cell(Cell::new(
                        tz.timestamp_millis(
                            self.columns()[column]
                                .as_any()
                                .downcast_ref::<TimestampArray>()
                                .unwrap()
                                .value(row),
                        ),
                    )),
                    DataType::Timestamp(None) => table_row.add_cell(Cell::new(
                        chrono::Local.timestamp_millis(
                            self.columns()[column]
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
        table.fmt(f)
    }
}
