use std::path::Path;

use anyhow::Result;
use derive_more::Display;
use rocksdb::{DBCompressionType, Options, DB};
use serde::{Deserialize, Serialize};
use yql_core::dataset::SchemaRef;
use yql_core::expr::Expr;
use yql_core::sql::ast::Select;

#[derive(Debug, Serialize, Deserialize)]
pub struct SourceDefinition {
    pub name: String,
    pub schema: SchemaRef,
    pub uri: String,
    pub time_expr: Option<Expr>,
    pub watermark_expr: Option<Expr>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SinkDefinition {
    pub name: String,
    pub uri: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StreamDefinition {
    pub name: String,
    pub select: Select,
    pub to: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Definition {
    Source(SourceDefinition),
    Stream(StreamDefinition),
    Sink(SinkDefinition),
}

#[derive(Debug, Serialize, Deserialize, Display)]
pub enum StreamState {
    #[display(fmt = "created")]
    Created,

    #[display(fmt = "started")]
    Started,

    #[display(fmt = "stop")]
    Stop,

    #[display(fmt = "finish")]
    Finish,

    #[display(fmt = "error: {}", _0)]
    Error(String),
}

impl Definition {
    fn name(&self) -> &str {
        match self {
            Definition::Source(source) => &source.name,
            Definition::Stream(stream) => &stream.name,
            Definition::Sink(sink) => &sink.name,
        }
    }
}

pub struct Storage {
    db: DB,
}

impl Storage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(DBCompressionType::Lz4);

        let db = Storage {
            db: DB::open(&opts, path)?,
        };
        Ok(db)
    }

    pub fn create_definition(&self, definition: Definition) -> Result<()> {
        let key = format!("definition/{}", definition.name());
        anyhow::ensure!(
            self.db.get_pinned(&key)?.is_none(),
            "definition '{}' already exists",
            definition.name()
        );
        self.db.put(key, bincode::serialize(&definition)?)?;
        Ok(())
    }

    pub fn definition_list(&self) -> Result<Vec<Definition>> {
        let mut definitions = Vec::new();

        for (key, value) in self.db.prefix_iterator("definition/") {
            if key.starts_with(b"definition/") {
                definitions.push(bincode::deserialize(&value)?);
            }
        }

        Ok(definitions)
    }

    pub fn delete_definition(&self, name: &str) -> Result<()> {
        let key = format!("definition/{}", name);
        self.db.delete(key)?;
        Ok(())
    }

    pub fn get_definition(&self, name: &str) -> Result<Option<Definition>> {
        let key = format!("definition/{}", name);
        match self.db.get_pinned(key)? {
            Some(data) => Ok(Some(bincode::deserialize(&data)?)),
            None => Ok(None),
        }
    }

    pub fn definition_exists(&self, name: &str) -> Result<bool> {
        let key = format!("definition/{}", name);
        Ok(self.db.get_pinned(key)?.is_some())
    }

    pub fn get_stream_state_data(&self, name: &str) -> Result<Option<Vec<u8>>> {
        let key = format!("stream_state_data/{}", name);
        Ok(self.db.get(key)?)
    }

    pub fn set_stream_state_data(&self, name: &str, data: &[u8]) -> Result<()> {
        let key = format!("stream_state_data/{}", name);
        Ok(self.db.put(key, data)?)
    }

    pub fn delete_stream_state_data(&self, name: &str) -> Result<()> {
        let key = format!("stream_state_data/{}", name);
        self.db.delete(key)?;
        Ok(())
    }

    pub fn get_stream_state(&self, name: &str) -> Result<Option<StreamState>> {
        let key = format!("stream_state/{}", name);
        match self.db.get_pinned(key)? {
            Some(data) => Ok(Some(bincode::deserialize(&data)?)),
            None => Ok(None),
        }
    }

    pub fn set_stream_state(&self, name: &str, state: StreamState) -> Result<()> {
        let key = format!("stream_state/{}", name);
        self.db.put(key, bincode::serialize(&state)?)?;
        Ok(())
    }

    pub fn delete_stream_state(&self, name: &str) -> Result<()> {
        let key = format!("stream_state/{}", name);
        self.db.delete(key)?;
        Ok(())
    }
}
