use anyhow::Result;
use url::Url;
use yql_core::{sinks, SinkProvider};

use crate::SinkDefinition;

pub fn create_sink_provider(definition: &SinkDefinition) -> Result<Box<dyn SinkProvider>> {
    let url: Url = definition.uri.parse()?;

    if url.scheme().eq_ignore_ascii_case("console") {
        return Ok(Box::new(sinks::Console));
    }

    anyhow::bail!("unsupported sink: '{}'", definition.uri)
}
