use std::path::{Path, PathBuf};
use std::pin::Pin;

use anyhow::Result;
use tokio_stream::Stream;
use yql_dataset::{CsvOptions, SchemaRef};
use yql_planner::{GenericSourceDataSet, GenericSourceProvider};

const DEFAULT_BATCH_SIZE: usize = 1000;

pub struct SourceCsv {
    options: CsvOptions,
    schema: SchemaRef,
    batch_size: usize,
    path: PathBuf,
}

impl SourceCsv {
    pub fn new(
        options: CsvOptions,
        schema: Option<SchemaRef>,
        path: impl AsRef<Path>,
    ) -> Result<Self> {
        let schema = match schema {
            Some(schema) => schema,
            None => options.infer_schema_from_path(path.as_ref())?,
        };
        Ok(Self {
            options,
            batch_size: DEFAULT_BATCH_SIZE,
            schema,
            path: path.as_ref().to_path_buf(),
        })
    }

    pub fn with_batch_size(self, batch_size: usize) -> Self {
        assert!(batch_size > 0);
        Self { batch_size, ..self }
    }
}

#[allow(clippy::type_complexity)]
impl GenericSourceProvider for SourceCsv {
    type State = usize;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<GenericSourceDataSet<Self::State>>> + Send + 'static>>;

    fn provider_name(&self) -> &'static str {
        "csv"
    }

    fn schema(&self) -> Result<SchemaRef> {
        Ok(self.schema.clone())
    }

    fn create_stream(&self, position: Option<Self::State>) -> Result<Self::Stream> {
        let mut reader = self.options.open_path(self.schema.clone(), &self.path)?;
        let mut position = if let Some(position) = position {
            reader.skip(position)?;
            position
        } else {
            0
        };

        let batch_size = self.batch_size;
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
