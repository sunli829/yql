use yql_dataset::SchemaRef;

use crate::source_provider::SourceProvider;

pub struct PhysicalSourceNode {
    pub id: usize,
    pub schema: SchemaRef,
    pub provider: SourceProvider,
}
