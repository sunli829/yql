use std::any::Any;
use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use csv::StringRecord;
use once_cell::sync::Lazy;
use regex::{Regex, RegexBuilder};
use yql_array::{
    BooleanBuilder, BooleanType, DataType, Float32Builder, Float32Type, Float64Builder,
    Float64Type, Int16Builder, Int16Type, Int32Builder, Int32Type, Int64Builder, Int64Type,
    Int8Builder, Int8Type, NullArray, PrimitiveBuilder, PrimitiveType, StringBuilder,
    TimestampBuilder, TimestampType,
};

use crate::{DataSet, Field, Schema, SchemaRef};

pub struct CsvOptions {
    pub delimiter: u8,
    pub has_header: bool,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: b',',
            has_header: false,
        }
    }
}

impl CsvOptions {
    pub fn open_path(self, schema: SchemaRef, path: impl AsRef<Path>) -> Result<CsvReader<File>> {
        Ok(self.open(schema, File::open(path)?))
    }

    pub fn open<R: Read>(self, schema: SchemaRef, rdr: R) -> CsvReader<R> {
        let reader = csv::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .has_headers(self.has_header)
            .from_reader(rdr);
        CsvReader {
            reader,
            schema,
            options: self,
        }
    }

    pub fn infer_schema_from_path(&self, path: impl AsRef<Path>) -> Result<SchemaRef> {
        self.infer_schema(File::open(path)?)
    }

    pub fn infer_schema<R: Read>(&self, rdr: R) -> Result<SchemaRef> {
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .has_headers(self.has_header)
            .from_reader(rdr);

        let headers: Vec<String> = if self.has_header {
            let headers = &reader.headers()?.clone();
            headers.iter().map(|s| s.to_string()).collect()
        } else {
            let first_record_count = &reader.headers()?.len();
            (0..*first_record_count)
                .map(|i| format!("c{}", i + 1))
                .collect()
        };

        let header_length = headers.len();
        let mut column_types: Vec<HashSet<DataType>> = vec![HashSet::new(); header_length];
        let mut fields = Vec::new();
        let mut record = StringRecord::new();

        loop {
            if !reader.read_record(&mut record)? {
                break;
            }

            for (i, column_type) in column_types.iter_mut().enumerate().take(header_length) {
                if let Some(string) = record.get(i) {
                    column_type.insert(infer_field_schema(string));
                }
            }
        }

        for i in 0..header_length {
            let possibilities = &column_types[i];
            let field_name = &headers[i];

            match possibilities.len() {
                1 => {
                    for data_type in possibilities.iter() {
                        fields.push(Field::new(field_name, *data_type));
                    }
                }
                2 => {
                    if possibilities.contains(&DataType::Int64)
                        && possibilities.contains(&DataType::Float64)
                    {
                        fields.push(Field::new(field_name, DataType::Float64));
                    } else {
                        fields.push(Field::new(field_name, DataType::String));
                    }
                }
                _ => fields.push(Field::new(field_name, DataType::String)),
            }
        }

        Ok(Arc::new(Schema::try_new(fields)?))
    }
}

pub struct CsvReader<R> {
    reader: csv::Reader<R>,
    schema: SchemaRef,
    options: CsvOptions,
}

impl<R: Read> CsvReader<R> {
    pub fn read_batch(&mut self, batch_size: Option<usize>) -> Result<DataSet> {
        let mut count = batch_size.unwrap_or(usize::MAX);
        let mut batch_records = vec![StringRecord::new(); 100];
        let mut builders = self
            .schema
            .fields()
            .iter()
            .map(|field| match field.data_type {
                DataType::Null => Box::new(0usize) as Box<dyn Any>,
                DataType::Int8 => Box::new(Int8Builder::default()) as Box<dyn Any>,
                DataType::Int16 => Box::new(Int16Builder::default()) as Box<dyn Any>,
                DataType::Int32 => Box::new(Int32Builder::default()) as Box<dyn Any>,
                DataType::Int64 => Box::new(Int64Builder::default()) as Box<dyn Any>,
                DataType::Float32 => Box::new(Float32Builder::default()) as Box<dyn Any>,
                DataType::Float64 => Box::new(Float64Builder::default()) as Box<dyn Any>,
                DataType::Boolean => Box::new(BooleanBuilder::default()) as Box<dyn Any>,
                DataType::Timestamp(_) => Box::new(TimestampBuilder::default()) as Box<dyn Any>,
                DataType::String => Box::new(StringBuilder::default()) as Box<dyn Any>,
            })
            .collect::<Vec<_>>();

        while count > 0 {
            let read_count = batch_records.len().min(count);
            let count = self.read_batch_records(&mut batch_records[..read_count])?;
            if count == 0 {
                break;
            }
            append_data(&self.schema, &mut builders, &batch_records[..count])?;
        }

        todo!()
        //DataSet::try_new(self.schema.clone(), columns)
    }

    fn read_batch_records(&mut self, records: &mut [StringRecord]) -> Result<usize> {
        let mut num_records = 0;

        while num_records < records.len() {
            if !self.reader.read_record(&mut records[num_records])? {
                break;
            }
            num_records += 1;
        }

        Ok(num_records)
    }
}

fn infer_field_schema(string: &str) -> DataType {
    static DECIMAL_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^-?(\d+\.\d+)$").unwrap());
    static INTEGER_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^-?(\d+)$").unwrap());
    static BOOLEAN_RE: Lazy<Regex> = Lazy::new(|| {
        RegexBuilder::new(r"^(true)$|^(false)$")
            .case_insensitive(true)
            .build()
            .unwrap()
    });

    if string.starts_with('"') {
        return DataType::String;
    }
    if BOOLEAN_RE.is_match(string) {
        DataType::Boolean
    } else if DECIMAL_RE.is_match(string) {
        DataType::Float64
    } else if INTEGER_RE.is_match(string) {
        DataType::Int64
    } else {
        DataType::String
    }
}

macro_rules! append_value {
    ($builder:expr, $records:expr, $idx:expr, $ty:ty) => {{
        let builder = $builder.downcast_mut::<PrimitiveBuilder<$ty>>().unwrap();
        for record in $records {
            match record.get($idx) {
                Some(value) => {
                    let value = <$ty as PrimitiveType>::Native::from_str(value)?;
                    builder.append(value);
                }
                None => builder.append_null(),
            }
        }
    }};
}

fn append_data(
    schema: &Schema,
    builders: &mut Vec<Box<dyn Any>>,
    records: &[StringRecord],
) -> Result<()> {
    for (idx, field) in schema.fields().iter().enumerate() {
        match field.data_type {
            DataType::Null => *builders[idx].downcast_mut::<usize>().unwrap() += records.len(),
            DataType::Int8 => append_value!(builders[idx], records, idx, Int8Type),
            DataType::Int16 => append_value!(builders[idx], records, idx, Int16Type),
            DataType::Int32 => append_value!(builders[idx], records, idx, Int32Type),
            DataType::Int64 => append_value!(builders[idx], records, idx, Int64Type),
            DataType::Float32 => append_value!(builders[idx], records, idx, Float32Type),
            DataType::Float64 => append_value!(builders[idx], records, idx, Float64Type),
            DataType::Boolean => append_value!(builders[idx], records, idx, BooleanType),
            DataType::Timestamp(_) => append_value!(builders[idx], records, idx, TimestampType),
            DataType::String => {
                let builder = builders[idx].downcast_mut::<StringBuilder>().unwrap();
                for record in records {
                    builder.append_opt(record.get(idx));
                }
            }
        }
    }

    Ok(())
}
