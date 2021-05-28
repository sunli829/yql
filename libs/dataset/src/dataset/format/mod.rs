mod json;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::dataset::{DataSet, SchemaRef};

use json::parse_json;

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
        match self {
            DataFormat::Json => parse_json(schema, data),
        }
    }
}
