use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures_util::stream::{BoxStream, StreamExt};
use itertools::Itertools;
use once_cell::sync::Lazy;
use tokio::sync::Mutex;
use yql_core::array::{ArrayRef, BooleanBuilder, DataType, StringArray, StringBuilder};
use yql_core::dataset::{DataSet, Field, Schema, SchemaRef};
use yql_core::sql::SqlSourceProvider;
use yql_core::{DataFrame, ExecutionMetrics, SinkProvider};

use crate::registry::Registry;
use crate::sink_provider::create_sink_provider;
use crate::source_provider::create_source_provider;
use crate::sql::{
    ShowType, Stmt, StmtCreateSink, StmtCreateSource, StmtCreateStream, StmtDeleteSink,
    StmtDeleteSource, StmtDeleteStream, StmtSelect, StmtShow, StmtStartStream, StmtStopStream,
};
use crate::storage::{Definition, SourceDefinition, Storage};
use crate::task::start_task;
use crate::{SinkDefinition, StreamDefinition};

static ACTION_RESULT_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    let fields = vec![
        Field::new("action", DataType::String),
        Field::new("success", DataType::Boolean),
    ];
    Arc::new(Schema::try_new(fields).unwrap())
});

static SHOW_SOURCES_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    let fields = vec![
        Field::new("name", DataType::String),
        Field::new("uri", DataType::String),
    ];
    Arc::new(Schema::try_new(fields).unwrap())
});

static SHOW_STREAMS_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    let fields = vec![
        Field::new("name", DataType::String),
        Field::new("to", DataType::String),
        Field::new("status", DataType::String),
    ];
    Arc::new(Schema::try_new(fields).unwrap())
});

static SHOW_SINKS_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    let fields = vec![
        Field::new("name", DataType::String),
        Field::new("uri", DataType::String),
    ];
    Arc::new(Schema::try_new(fields).unwrap())
});

fn create_action_result_dataset(action: &str, success: bool) -> Result<DataSet> {
    let columns = vec![
        {
            let mut builder = StringBuilder::default();
            builder.append(action);
            Arc::new(builder.finish()) as ArrayRef
        },
        {
            let mut builder = BooleanBuilder::default();
            builder.append(success);
            Arc::new(builder.finish()) as ArrayRef
        },
    ];
    DataSet::try_new(ACTION_RESULT_SCHEMA.clone(), columns)
}

struct SqlContext<'a>(&'a ServiceInner);

impl<'a> yql_core::sql::SqlContext for SqlContext<'a> {
    fn create_source_provider(&self, name: &str) -> Result<Option<SqlSourceProvider>> {
        let definition =
            self.0
                .storage
                .get_definition(name)?
                .and_then(|definition| match definition {
                    Definition::Source(source_definition) => Some(source_definition),
                    _ => None,
                });
        match definition {
            Some(definition) => Ok(Some(create_source_provider(&definition)?)),
            None => Ok(None),
        }
    }
}

pub enum ExecuteStreamItem {
    DataSet(DataSet),
    Metrics(ExecutionMetrics),
}

pub enum ExecuteResult {
    DataSet(DataSet),
    ExecStream(BoxStream<'static, Result<ExecuteStreamItem>>),
}

pub struct ServiceInner {
    pub(crate) storage: Storage,
    pub(crate) registry: Registry,
}

impl ServiceInner {
    fn create_sink_provider(&self, name: &str) -> Result<Box<dyn SinkProvider>> {
        let definition =
            self.storage
                .get_definition(name)?
                .and_then(|definition| match definition {
                    Definition::Sink(sink_definition) => Some(sink_definition),
                    _ => None,
                });
        match definition {
            Some(definition) => create_sink_provider(&definition),
            None => anyhow::bail!("sink '{}' not defined"),
        }
    }
}

#[derive(Clone)]
pub struct Service {
    inner: Arc<Mutex<ServiceInner>>,
}

impl Service {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let storage = Storage::open(path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(ServiceInner {
                storage,
                registry: Registry::default(),
            })),
        })
    }

    pub async fn execute(&self, sql: &str) -> Result<ExecuteResult> {
        let (_, stmt) = crate::sql::stmt(sql).map_err(|err| anyhow::anyhow!("{}", err))?;

        match stmt {
            Stmt::CreateSource(stmt) => Ok(ExecuteResult::DataSet(
                self.execute_create_source(stmt).await?,
            )),
            Stmt::CreateStream(stmt) => Ok(ExecuteResult::DataSet(
                self.execute_create_stream(stmt).await?,
            )),
            Stmt::CreateSink(stmt) => Ok(ExecuteResult::DataSet(
                self.execute_create_sink(stmt).await?,
            )),
            Stmt::DeleteSource(stmt) => Ok(ExecuteResult::DataSet(
                self.execute_delete_source(stmt).await?,
            )),
            Stmt::DeleteStream(stmt) => Ok(ExecuteResult::DataSet(
                self.execute_delete_stream(stmt).await?,
            )),
            Stmt::DeleteSink(stmt) => Ok(ExecuteResult::DataSet(
                self.execute_delete_sink(stmt).await?,
            )),
            Stmt::StartStream(stmt) => Ok(ExecuteResult::DataSet(
                self.execute_start_stream(stmt).await?,
            )),
            Stmt::StopStream(stmt) => Ok(ExecuteResult::DataSet(
                self.execute_stop_stream(stmt).await?,
            )),
            Stmt::Show(stmt) => Ok(ExecuteResult::DataSet(self.execute_show(stmt).await?)),
            Stmt::Select(stmt) => Ok(ExecuteResult::ExecStream(self.execute_select(stmt).await?)),
        }
    }

    async fn execute_create_source(&self, stmt: StmtCreateSource) -> Result<DataSet> {
        let inner = self.inner.lock().await;
        anyhow::ensure!(
            !inner.storage.definition_exists(&stmt.name)?,
            "already exists"
        );

        inner
            .storage
            .create_definition(Definition::Source(SourceDefinition {
                name: stmt.name,
                schema: Arc::new(Schema::try_new(stmt.fields)?),
                uri: stmt.uri,
                time_expr: stmt.time,
                watermark_expr: stmt.watermark,
            }))?;

        create_action_result_dataset("Create Source", true)
    }

    async fn execute_create_stream(&self, stmt: StmtCreateStream) -> Result<DataSet> {
        let inner = self.inner.lock().await;
        anyhow::ensure!(
            !inner.storage.definition_exists(&stmt.name)?,
            "already exists"
        );

        inner
            .storage
            .create_definition(Definition::Stream(StreamDefinition {
                name: stmt.name,
                select: stmt.select,
                to: stmt.to,
            }))?;

        create_action_result_dataset("Create Stream", true)
    }

    async fn execute_create_sink(&self, stmt: StmtCreateSink) -> Result<DataSet> {
        let inner = self.inner.lock().await;
        anyhow::ensure!(
            !inner.storage.definition_exists(&stmt.name)?,
            "already exists"
        );

        inner
            .storage
            .create_definition(Definition::Sink(SinkDefinition {
                name: stmt.name,
                uri: stmt.uri,
            }))?;

        create_action_result_dataset("Create Sink", true)
    }

    async fn execute_delete_source(&self, stmt: StmtDeleteSource) -> Result<DataSet> {
        let inner = self.inner.lock().await;
        anyhow::ensure!(
            inner
                .storage
                .get_definition(&stmt.name)?
                .map(|definition| matches!(definition, Definition::Source(_)))
                .unwrap_or_default(),
            "not exists"
        );
        inner.storage.delete_definition(&stmt.name)?;
        create_action_result_dataset("Delete Source", true)
    }

    async fn execute_delete_stream(&self, stmt: StmtDeleteStream) -> Result<DataSet> {
        let mut inner = self.inner.lock().await;
        anyhow::ensure!(
            inner
                .storage
                .get_definition(&stmt.name)?
                .map(|definition| matches!(definition, Definition::Stream(_)))
                .unwrap_or_default(),
            "not exists"
        );
        inner.storage.delete_definition(&stmt.name)?;
        inner.storage.delete_stream_state(&stmt.name)?;
        inner.storage.delete_stream_state_data(&stmt.name)?;
        inner.registry.stop(&stmt.name);
        create_action_result_dataset("Delete Stream", true)
    }

    async fn execute_delete_sink(&self, stmt: StmtDeleteSink) -> Result<DataSet> {
        let inner = self.inner.lock().await;
        anyhow::ensure!(
            inner
                .storage
                .get_definition(&stmt.name)?
                .map(|definition| matches!(definition, Definition::Sink(_)))
                .unwrap_or_default(),
            "not exists"
        );
        inner.storage.delete_definition(&stmt.name)?;
        create_action_result_dataset("Delete Sink", true)
    }

    async fn execute_start_stream(&self, stmt: StmtStartStream) -> Result<DataSet> {
        let service_inner = self.inner.clone();

        let inner = self.inner.lock().await;
        anyhow::ensure!(!inner.registry.is_running(&stmt.name), "already running");

        let definition = inner
            .storage
            .get_definition(&stmt.name)?
            .ok_or_else(|| anyhow::anyhow!("not found"))?;
        let stream_definition = match definition {
            Definition::Stream(stream_definition) => stream_definition,
            _ => anyhow::bail!("not stream"),
        };
        let sink = inner
            .create_sink_provider(&stream_definition.to)?
            .create()?;
        let df = DataFrame::from_sql_select(&SqlContext(&*inner), stream_definition.select)?;

        let stream = if stmt.restart {
            inner.storage.delete_stream_state_data(&stmt.name)?;
            df.into_stream(None)?
        } else {
            df.into_stream(inner.storage.get_stream_state_data(&stmt.name)?)?
        };
        let interval = tokio::time::interval(Duration::from_secs(5 * 60));

        tokio::spawn(start_task(
            service_inner,
            stmt.name.clone(),
            interval,
            stream,
            sink,
        ));

        create_action_result_dataset("Start Stream", true)
    }

    async fn execute_stop_stream(&self, stmt: StmtStopStream) -> Result<DataSet> {
        let mut inner = self.inner.lock().await;
        anyhow::ensure!(inner.registry.is_running(&stmt.name), "not running");
        inner.registry.stop(&stmt.name);
        create_action_result_dataset("Stop Stream", true)
    }

    async fn execute_show(&self, stmt: StmtShow) -> Result<DataSet> {
        let inner = self.inner.lock().await;

        match stmt.show_type {
            ShowType::Sources => {
                let sources = inner
                    .storage
                    .definition_list()?
                    .into_iter()
                    .filter_map(|definition| match definition {
                        Definition::Source(source_definition) => Some(source_definition),
                        _ => None,
                    })
                    .collect_vec();
                DataSet::try_new(
                    SHOW_SOURCES_SCHEMA.clone(),
                    vec![
                        Arc::new(
                            sources
                                .iter()
                                .map(|source| &source.name)
                                .collect::<StringArray>(),
                        ),
                        Arc::new(
                            sources
                                .iter()
                                .map(|source| &source.uri)
                                .collect::<StringArray>(),
                        ),
                    ],
                )
            }
            ShowType::Streams => {
                let streams = inner
                    .storage
                    .definition_list()?
                    .into_iter()
                    .filter_map(|definition| match definition {
                        Definition::Stream(stream_definition) => Some(stream_definition),
                        _ => None,
                    })
                    .collect_vec();
                let status = streams
                    .iter()
                    .map(|stream| inner.storage.get_stream_state(&stream.name))
                    .collect_vec();
                let streams = streams
                    .into_iter()
                    .zip(status)
                    .filter_map(|(stream_definition, status)| match status {
                        Ok(Some(status)) => Some((stream_definition, status)),
                        _ => None,
                    })
                    .collect_vec();
                DataSet::try_new(
                    SHOW_STREAMS_SCHEMA.clone(),
                    vec![
                        Arc::new(
                            streams
                                .iter()
                                .map(|(stream, _)| &stream.name)
                                .collect::<StringArray>(),
                        ),
                        Arc::new(
                            streams
                                .iter()
                                .map(|(stream, _)| &stream.to)
                                .collect::<StringArray>(),
                        ),
                        Arc::new(
                            streams
                                .iter()
                                .map(|(_, status)| status.to_string())
                                .collect::<StringArray>(),
                        ),
                    ],
                )
            }
            ShowType::Sinks => {
                let sinks = inner
                    .storage
                    .definition_list()?
                    .into_iter()
                    .filter_map(|definition| match definition {
                        Definition::Sink(sink_definition) => Some(sink_definition),
                        _ => None,
                    })
                    .collect_vec();
                DataSet::try_new(
                    SHOW_SINKS_SCHEMA.clone(),
                    vec![
                        Arc::new(sinks.iter().map(|sink| &sink.name).collect::<StringArray>()),
                        Arc::new(sinks.iter().map(|sink| &sink.uri).collect::<StringArray>()),
                    ],
                )
            }
        }
    }

    async fn execute_select(
        &self,
        stmt: StmtSelect,
    ) -> Result<BoxStream<'static, Result<ExecuteStreamItem>>> {
        let inner = self.inner.lock().await;
        let df = DataFrame::from_sql_select(&SqlContext(&*inner), stmt.select)?;
        let mut input = df.into_stream(None)?;

        Ok(Box::pin(async_stream::try_stream! {
            while let Some(dataset) = input.next().await.transpose()? {
                yield ExecuteStreamItem::DataSet(dataset);
            }
            let metrics = ExecuteStreamItem::Metrics(input.metrics());
            yield metrics;
        }))
    }
}
