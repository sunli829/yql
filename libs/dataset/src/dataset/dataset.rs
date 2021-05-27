use std::fs::File;
use std::io::{Cursor, Read};
use std::path::Path;

use anyhow::Result;

use crate::array::{compute, ArrayRef, BooleanArray};
use crate::dataset::{CsvOptions, SchemaRef};

#[derive(Debug, Clone)]
pub struct DataSet {
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
}

impl DataSet {
    pub fn try_new(schema: SchemaRef, columns: Vec<ArrayRef>) -> Result<Self> {
        anyhow::ensure!(
            schema.fields().len() == columns.len() && !columns.is_empty(),
            "invalid dataset: expect {} fields, actual {} fields.",
            schema.fields().len(),
            columns.len()
        );

        let size = columns[0].len();
        for (idx, column) in columns[1..].iter().enumerate() {
            anyhow::ensure!(
                column.len() == size,
                "invalid dataset: expect column '{}' length is {}, actual length is {}.",
                schema.fields()[idx + 1].name,
                size,
                column.len()
            );
        }

        for (column, field) in columns.iter().zip(schema.fields()) {
            anyhow::ensure!(
                column.data_type() == field.data_type,
                "invalid dataset: expect column '{}' datatype is {}, actual datatype is {}.",
                field.name,
                field.data_type,
                column.data_type()
            );
        }

        Ok(Self { schema, columns })
    }

    pub fn from_csv<R: Read>(schema: SchemaRef, options: CsvOptions, rdr: R) -> Result<DataSet> {
        let mut reader = options.open(schema, rdr);
        reader.read_batch(None)
    }

    pub fn from_csv_file(
        schema: SchemaRef,
        options: CsvOptions,
        path: impl AsRef<Path>,
    ) -> Result<DataSet> {
        Self::from_csv(schema, options, File::open(path)?)
    }

    pub fn from_csv_slice(schema: SchemaRef, options: CsvOptions, data: &[u8]) -> Result<DataSet> {
        Self::from_csv(schema, options, Cursor::new(data))
    }

    #[inline]
    pub fn column(&self, index: usize) -> Option<ArrayRef> {
        self.columns.get(index).cloned()
    }

    #[inline]
    pub fn columns(&self) -> &[ArrayRef] {
        &self.columns
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.columns[0].is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.columns[0].len()
    }

    #[inline]
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn slice(&self, offset: usize, length: usize) -> DataSet {
        DataSet {
            schema: self.schema.clone(),
            columns: self
                .columns
                .iter()
                .map(|column| column.slice(offset, length))
                .collect(),
        }
    }

    pub fn filter(&self, flags: &BooleanArray) -> Result<DataSet> {
        DataSet::try_new(
            self.schema.clone(),
            self.columns
                .iter()
                .cloned()
                .map(|array| compute::filter(array, flags))
                .collect(),
        )
    }
}

impl PartialEq for DataSet {
    fn eq(&self, other: &Self) -> bool {
        if self.schema != other.schema {
            return false;
        }
        if self.columns.len() != other.columns.len() {
            return false;
        }
        for (a, b) in self.columns.iter().zip(&other.columns) {
            if !a.as_ref().eq(b.as_ref()) {
                return false;
            }
        }
        true
    }
}
