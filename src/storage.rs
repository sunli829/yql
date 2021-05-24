// use std::path::Path;
//
// use anyhow::Result;
// use parking_lot::RwLock;
// use rocksdb::{DBCompressionType, Options, DB};
// use serde::{Deserialize, Serialize};
// use yql_core::logical_plan::LogicalPlan;
// use yql_dataset::SchemaRef;
// use yql_expr::Expr;
//
// #[derive(Debug, Serialize, Deserialize)]
// pub struct SourceDefinition {
//     name: String,
//     schema: SchemaRef,
//     uri: String,
//     time_expr: Option<Expr>,
//     watermark_expr: Option<Expr>,
// }
//
// #[derive(Debug, Serialize, Deserialize)]
// pub struct SinkDefinition {
//     name: String,
//     uri: String,
// }
//
// #[derive(Debug, Serialize, Deserialize)]
// pub struct StreamDefinition {
//     name: String,
//     plan: LogicalPlan,
//     to: String,
// }
//
// #[derive(Debug, Serialize, Deserialize)]
// pub enum Definition {
//     Source(SourceDefinition),
//     Stream(StreamDefinition),
//     Sink(SinkDefinition),
// }
//
// impl Definition {
//     fn name(&self) -> &str {
//         match self {
//             Definition::Source(source) => &source.name,
//             Definition::Stream(stream) => &stream.name,
//             Definition::Sink(sink) => &sink.name,
//         }
//     }
// }
//
// pub struct Storage {
//     db: DB,
//     meta_lock: RwLock<()>,
// }
//
// impl Storage {
//     pub fn open(path: impl AsRef<Path>) -> Result<Self> {
//         let mut opts = Options::default();
//         opts.create_if_missing(true);
//         opts.set_compression_type(DBCompressionType::Lz4);
//
//         let db = Storage {
//             db: DB::open(&opts, path)?,
//             meta_lock: Default::default(),
//         };
//         Ok(db)
//     }
//
//     pub fn create_definition(&self, definition: Definition) -> Result<()> {
//         let _ = self.meta_lock.write();
//         let key = format!("definition/{}", definition.name());
//         anyhow::ensure!(
//             self.db.get_pinned(&key)?.is_none(),
//             "definition '{}' already exists",
//             definition.name()
//         );
//         self.db.put(key, bincode::serialize(&definition)?)?;
//         Ok(())
//     }
//
//     pub fn definition_list(&self) -> Result<Vec<Definition>> {
//         let _ = self.meta_lock.read();
//         let mut definitions = Vec::new();
//
//         for (key, value) in self.db.prefix_iterator("definition/") {
//             if key.starts_with(b"definition/") {
//                 definitions.push(bincode::deserialize(&value)?);
//             }
//         }
//
//         Ok(definitions)
//     }
//
//     pub fn delete_definition(&self, name: &str) -> Result<()> {
//         let key = format!("definition/{}", name);
//         self.db.delete(key)?;
//         Ok(())
//     }
//
//     pub fn get_definition(&self, name: &str) -> Result<Option<Definition>> {
//         let key = format!("definition/{}", name);
//         match self.db.get_pinned(key)? {
//             Some(data) => Ok(Some(bincode::deserialize(&data)?)),
//             None => Ok(None),
//         }
//     }
//
//     pub fn get_stream_state(&self, name: &str) -> Result<Option<Vec<u8>>> {
//         let key = format!("stream_state/{}", name);
//         Ok(self.db.get(key)?)
//     }
//
//     pub fn set_stream_state(&self, name: &str, data: &[u8]) -> Result<()> {
//         let key = format!("stream_state/{}", name);
//         Ok(self.db.put(key, data)?)
//     }
// }
