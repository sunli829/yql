use anyhow::Result;

#[async_trait::async_trait]
pub trait Storage: Send + Sync + 'static {
    async fn save_state(&self, data: Vec<u8>) -> Result<()>;

    async fn load_state(&self) -> Result<Option<Vec<u8>>>;
}
