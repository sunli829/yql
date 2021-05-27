use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use futures_util::future::FutureExt;
use futures_util::stream::BoxStream;
use itertools::Itertools;
use once_cell::sync::Lazy;
use tokio::sync::{oneshot, Mutex};
use yql_core::array::{ArrayRef, BooleanBuilder, DataType, StringArray, StringBuilder};
use yql_core::dataset::{DataSet, Field, Schema, SchemaRef};
use yql_core::sql::SqlSourceProvider;
use yql_core::{DataFrame, ExecutionContext, SinkProvider};

use crate::registry::Registry;
use crate::sink_provider::create_sink_provider;
use crate::source_provider::create_source_provider;
use crate::sql::{
    ShowType, Stmt, StmtCreateSink, StmtCreateSource, StmtCreateStream, StmtDeleteSink,
    StmtDeleteSource, StmtDeleteStream, StmtSelect, StmtShow, StmtStartStream, StmtStopStream,
};
use crate::storage::{Definition, SourceDefinition, Storage, StreamState};
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

struct StreamStorage {
    name: String,
    inner: Arc<Mutex<ServiceInner>>,
}

#[async_trait::async_trait]
impl yql_core::Storage for StreamStorage {
    async fn save_state(&self, data: Vec<u8>) -> Result<()> {
        self.inner
            .lock()
            .await
            .storage
            .set_stream_state_data(&self.name, &data)
    }

    async fn load_state(&self) -> Result<Option<Vec<u8>>> {
        self.inner
            .lock()
            .await
            .storage
            .get_stream_state_data(&self.name)
    }
}

pub struct ServiceInner {
    storage: Storage,
    registry: Registry,
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

    pub async fn execute(&self, sql: &str) -> Result<BoxStream<'static, Result<DataSet>>> {
        let (_, stmt) = crate::sql::stmt(sql).map_err(|err| anyhow::anyhow!("{}", err))?;

        match stmt {
            Stmt::CreateSource(stmt) => Ok(once_stream(self.execute_create_source(stmt).await?)),
            Stmt::CreateStream(stmt) => Ok(once_stream(self.execute_create_stream(stmt).await?)),
            Stmt::CreateSink(stmt) => Ok(once_stream(self.execute_create_sink(stmt).await?)),
            Stmt::DeleteSource(stmt) => Ok(once_stream(self.execute_delete_source(stmt).await?)),
            Stmt::DeleteStream(stmt) => Ok(once_stream(self.execute_delete_stream(stmt).await?)),
            Stmt::DeleteSink(stmt) => Ok(once_stream(self.execute_delete_sink(stmt).await?)),
            Stmt::StartStream(stmt) => Ok(once_stream(self.execute_start_stream(stmt).await?)),
            Stmt::StopStream(stmt) => Ok(once_stream(self.execute_stop_stream(stmt).await?)),
            Stmt::Show(stmt) => Ok(once_stream(self.execute_show(stmt).await?)),
            Stmt::Select(stmt) => self.execute_select(stmt).await,
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

        let mut inner = self.inner.lock().await;
        anyhow::ensure!(!inner.registry.is_running(&stmt.name), "already running");

        let definition = inner
            .storage
            .get_definition(&stmt.name)?
            .ok_or_else(|| anyhow::anyhow!("not found"))?;
        let stream_definition = match definition {
            Definition::Stream(stream_definition) => stream_definition,
            _ => anyhow::bail!("not stream"),
        };
        let sink = inner.create_sink_provider(&stream_definition.to)?;
        let df = DataFrame::from_sql_select(&SqlContext(&*inner), stream_definition.select)?;
        let ctx = ExecutionContext::new(stmt.name.clone()).with_storage(StreamStorage {
            name: stmt.name.clone(),
            inner: service_inner.clone(),
        });
        let (tx_shutdown, rx_shutdown) = oneshot::channel::<()>();

        if stmt.restart {
            inner.storage.delete_stream_state_data(&stmt.name)?;
        }
        let fut = df.into_task_with_graceful_shutdown(ctx, sink, Some(rx_shutdown.map(|_| ())));

        inner
            .storage
            .set_stream_state(&stmt.name, StreamState::Started)?;
        inner.registry.add(&stmt.name, tx_shutdown);

        let name = stmt.name.clone();
        tokio::spawn(async move {
            let res = fut.await;
            let mut inner = service_inner.lock().await;

            match res {
                Ok(()) => {
                    inner
                        .storage
                        .set_stream_state(&name, StreamState::Stop)
                        .ok();
                }
                Err(err) => {
                    inner
                        .storage
                        .set_stream_state(&name, StreamState::Error(err.to_string()))
                        .ok();
                }
            }
            inner.registry.remove(&name);
        });

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
    ) -> Result<BoxStream<'static, Result<DataSet>>> {
        let inner = self.inner.lock().await;
        let df = DataFrame::from_sql_select(&SqlContext(&*inner), stmt.select)?;
        let ctx = ExecutionContext::new("noname");
        Ok(df.into_stream(ctx))
    }
}

fn once_stream(dataset: DataSet) -> BoxStream<'static, Result<DataSet>> {
    Box::pin(futures_util::stream::once(async move { Ok(dataset) }))
}
