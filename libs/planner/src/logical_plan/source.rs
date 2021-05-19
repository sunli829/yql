use crate::source_provider::SourceProvider;

pub struct LogicalSourcePlan {
    pub name: String,
    pub qualifier: Option<String>,
    pub provider: SourceProvider,
}
