use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::dataset::{DataSet, SchemaRef};

#[derive(Copy, Clone, Serialize, Deserialize)]
pub enum DataFormat {
    Json,
}

impl Default for DataFormat {
    fn default() -> Self {
        Self::Json
    }
}

impl DataFormat {
    pub fn parse(&self, schema: SchemaRef, data: &[u8]) -> Result<DataSet> {
        todo!()
    }
}
