use std::sync::Arc;

use anyhow::{Context, Result};
use url::Url;
use yql_core::dataset::CsvOptions;
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
            let options = match url.query() {
                Some(query) => serde_qs::from_str::<CsvOptions>(query)
                    .with_context(|| "failed to parse csv options")?,
                None => CsvOptions::default(),
            };
            let source_provider = sources::Csv::new(options, Some(definition.schema.clone()), path)
                .with_context(|| "failed to create csv reader")?;
            return Ok(SqlSourceProvider {
                source_provider: Arc::new(SourceProviderWrapper(source_provider)),
                time_expr: definition.time_expr.clone(),
                watermark_expr: definition.watermark_expr.clone(),
            });
        }
    }

    anyhow::bail!("unsupported source: '{}'", definition.uri)
}
