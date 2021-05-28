use anyhow::Result;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use itertools::Itertools;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use serde::{Deserialize, Serialize};

use crate::dataset::{DataFormat, SchemaRef};
use crate::{GenericSourceDataSet, GenericSourceProvider};

#[derive(Serialize, Deserialize)]
pub struct Options {
    servers: String,
    group_id: String,
    topics: Vec<String>,
    #[serde(default)]
    format: DataFormat,
}

pub struct Provider {
    options: Options,
    schema: SchemaRef,
}

impl Provider {
    pub fn new(options: Options, schema: SchemaRef) -> Self {
        Self { options, schema }
    }
}

impl GenericSourceProvider for Provider {
    type State = ();

    fn provider_name(&self) -> &'static str {
        "kafka"
    }

    fn schema(&self) -> Result<SchemaRef> {
        Ok(self.schema.clone())
    }

    fn create_stream(
        &self,
        _: Option<Self::State>,
    ) -> Result<BoxStream<'static, Result<GenericSourceDataSet<Self::State>>>> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", &self.options.group_id)
            .set("bootstrap.servers", &self.options.servers)
            .set("enable.auto.commit", "false")
            .create()?;

        consumer.subscribe(&self.options.topics.iter().map(|x| x.as_str()).collect_vec())?;

        let schema = self.schema.clone();
        let format = self.options.format;
        Ok(Box::pin(async_stream::try_stream! {
            let mut input = consumer.stream();
            while let Some(message) = input.next().await.transpose()? {
                if let Some(payload) = message.payload() {
                    let dataset = format.parse(schema.clone(), payload)?;
                    yield GenericSourceDataSet {
                        state: (),
                        dataset,
                    };
                }
            }
        }))
    }
}
