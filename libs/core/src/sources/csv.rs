use std::path::{Path, PathBuf};

use anyhow::Result;
use futures_util::stream::BoxStream;
use serde::{Deserialize, Serialize};

use crate::dataset::{CsvOptions, SchemaRef};
use crate::{GenericSourceDataSet, GenericSourceProvider};
use std::fs::File;
use std::io::{Cursor, Read};

const DEFAULT_BATCH_SIZE: usize = 10000;

#[derive(Serialize, Deserialize)]
pub struct Options {
    #[serde(default = "default_delimiter")]
    pub delimiter: u8,
    #[serde(default)]
    pub has_header: bool,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_delimiter() -> u8 {
    b','
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

enum DataFrom {
    Path(PathBuf),
    Data(Vec<u8>),
}

pub struct Provider {
    options: Options,
    schema: SchemaRef,
    from: DataFrom,
}

impl Provider {
    pub fn new(options: Options, schema: SchemaRef, path: impl AsRef<Path>) -> Self {
        Self {
            options,
            schema,
            from: DataFrom::Path(path.as_ref().to_path_buf()),
        }
    }

    pub fn new_from_memory(options: Options, schema: SchemaRef, data: impl Into<Vec<u8>>) -> Self {
        Self {
            options,
            schema,
            from: DataFrom::Data(data.into()),
        }
    }
}

impl GenericSourceProvider for Provider {
    type State = usize;

    fn provider_name(&self) -> &'static str {
        "csv"
    }

    fn schema(&self) -> Result<SchemaRef> {
        Ok(self.schema.clone())
    }

    fn create_stream(
        &self,
        position: Option<Self::State>,
    ) -> Result<BoxStream<'static, Result<GenericSourceDataSet<Self::State>>>> {
        let options = CsvOptions {
            delimiter: self.options.delimiter,
            has_header: self.options.has_header,
        };
        let mut reader = match &self.from {
            DataFrom::Path(path) => options.open(
                self.schema.clone(),
                Box::new(File::open(&path)?) as Box<dyn Read + Send>,
            ),
            DataFrom::Data(data) => options.open(
                self.schema.clone(),
                Box::new(Cursor::new(data.clone())) as Box<dyn Read + Send>,
            ),
        };
        let mut position = if let Some(position) = position {
            reader.skip(position)?;
            position
        } else {
            0
        };

        let batch_size = self.options.batch_size;
        Ok(Box::pin(async_stream::try_stream! {
            loop {
                let dataset = reader.read_batch(Some(batch_size))?;
                if dataset.is_empty() {
                    break;
                }
                let count = dataset.len();
                yield GenericSourceDataSet {
                    state: position + count,
                    dataset,
                };
                position += count;
            }
        }))
    }
}
