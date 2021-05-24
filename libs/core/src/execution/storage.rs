use anyhow::Result;

pub trait Storage: Send + Sync + 'static {
    fn save_state(&self, data: Vec<u8>) -> Result<()>;

    fn load_state(&self) -> Result<Vec<u8>>;
}
