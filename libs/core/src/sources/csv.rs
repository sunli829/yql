use std::path::{Path, PathBuf};

use anyhow::Result;
use futures_util::stream::BoxStream;
use serde::{Deserialize, Serialize};

use crate::dataset::{CsvOptions, SchemaRef};
use crate::{GenericSourceDataSet, GenericSourceProvider};

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

pub struct Provider {
    options: Options,
    schema: SchemaRef,
    path: PathBuf,
}

impl Provider {
    pub fn new(options: Options, schema: SchemaRef, path: impl AsRef<Path>) -> Self {
        Self {
            options,
            schema,
            path: path.as_ref().to_path_buf(),
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
        let mut reader = CsvOptions {
            delimiter: self.options.delimiter,
            has_header: self.options.has_header,
        }
        .open_path(self.schema.clone(), &self.path)?;
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
                    state: position,
                    dataset,
                };
                position += count;
            }
        }))
    }
}
