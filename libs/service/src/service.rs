use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use futures_util::future::FutureExt;
use once_cell::sync::Lazy;
use tokio::sync::{oneshot, Mutex};
use yql_core::array::{ArrayRef, BooleanBuilder, DataType, StringBuilder};
use yql_core::dataset::{DataSet, Field, Schema, SchemaRef};
use yql_core::sql::SqlSourceProvider;
use yql_core::{DataFrame, ExecutionContext, SinkProvider};

use crate::registry::Registry;
use crate::sink_provider::create_sink_provider;
use crate::source_provider::create_source_provider;
use crate::sql::{
    Stmt, StmtCreateSink, StmtCreateSource, StmtCreateStream, StmtDeleteSink, StmtDeleteSource,
    StmtDeleteStream, StmtStartStream, StmtStopStream,
};
use crate::storage::{Definition, SourceDefinition, Storage, StreamState};

static ACTION_RESULT_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    let fields = vec![
        Field::new("action", DataType::String),
        Field::new("success", DataType::Boolean),
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

    pub async fn execute(&self, sql: &str) -> Result<DataSet> {
        let (_, stmt) = crate::sql::stmt(sql).map_err(|err| anyhow::anyhow!("{}", err))?;

        match stmt {
            Stmt::CreateSource(stmt) => self.execute_create_source(stmt).await,
            Stmt::CreateStream(stmt) => self.execute_create_stream(stmt).await,
            Stmt::CreateSink(stmt) => self.execute_create_sink(stmt).await,
            Stmt::DeleteSource(stmt) => self.execute_delete_source(stmt).await,
            Stmt::DeleteStream(stmt) => self.execute_delete_stream(stmt).await,
            Stmt::DeleteSink(stmt) => self.execute_delete_sink(stmt).await,
            Stmt::StartStream(stmt) => self.execute_start_stream(stmt).await,
            Stmt::StopStream(stmt) => self.execute_stop_stream(stmt).await,
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

        create_action_result_dataset("Create Stream", true)
    }

    async fn execute_create_sink(&self, stmt: StmtCreateSink) -> Result<DataSet> {
        let inner = self.inner.lock().await;
        anyhow::ensure!(
            !inner.storage.definition_exists(&stmt.name)?,
            "already exists"
        );

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
}
