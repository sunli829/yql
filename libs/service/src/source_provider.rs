use std::sync::Arc;

use anyhow::{Context, Result};
use url::Url;
use yql_core::sql::SqlSourceProvider;
use yql_core::{sources, SourceProviderWrapper};

use crate::storage::SourceDefinition;

pub fn create_source_provider(definition: &SourceDefinition) -> Result<SqlSourceProvider> {
    let url: Url = definition
        .uri
        .parse()
        .with_context(|| format!("invalid source uri: {}", definition.uri))?;

    if let Ok(path) = url.to_file_path() {
        if path.extension().and_then(|ext| ext.to_str()) == Some("csv") {
            let options =
                serde_qs::from_str::<sources::csv::Options>(url.query().unwrap_or_default())
                    .with_context(|| "failed to parse csv options")?;
            let source_provider =
                sources::csv::Provider::new(options, definition.schema.clone(), path);
            return Ok(SqlSourceProvider {
                source_provider: Arc::new(SourceProviderWrapper(source_provider)),
                time_expr: definition.time_expr.clone(),
            });
        }
    }

    anyhow::bail!("unsupported source: '{}'", definition.uri)
}
