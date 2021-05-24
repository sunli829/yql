use yql_core::SourceProvider;

pub trait SqlContext {
    fn create_source_provider(&self, name: &str) -> Option<SourceProvider>;
}
