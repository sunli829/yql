use anyhow::Result;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::array::{ArrayRef, DataType};
use crate::expr::signature::Signature;

pub trait StatefulFunction: dyn_clone::DynClone + Sync + Send + 'static {
    fn call(&mut self, args: &[ArrayRef]) -> Result<ArrayRef>;

    fn save_state(&self) -> Result<Vec<u8>>;

    fn load_state(&mut self, state: Vec<u8>) -> Result<()>;
}

dyn_clone::clone_trait_object!(StatefulFunction);

#[derive(Clone)]
pub struct AggregateFunction<T>
where
    T: Serialize + DeserializeOwned + Clone + Sync + Send + Default + 'static,
{
    state: T,
    f: fn(&mut T, &[ArrayRef]) -> Result<ArrayRef>,
}

impl<T> AggregateFunction<T>
where
    T: Serialize + DeserializeOwned + Clone + Sync + Send + Default + 'static,
{
    pub fn new(f: fn(&mut T, &[ArrayRef]) -> Result<ArrayRef>) -> Self {
        Self {
            state: T::default(),
            f,
        }
    }
}

impl<T> StatefulFunction for AggregateFunction<T>
where
    T: Serialize + DeserializeOwned + Clone + Sync + Send + Default + 'static,
{
    fn call(&mut self, args: &[ArrayRef]) -> Result<ArrayRef> {
        (self.f)(&mut self.state, args)
    }

    fn save_state(&self) -> Result<Vec<u8>> {
        bincode::serialize(&self.state)
            .map_err(|err| anyhow::anyhow!("failed to serialize function state: {}", err))
    }

    fn load_state(&mut self, state: Vec<u8>) -> Result<()> {
        let state = bincode::deserialize(&state)
            .map_err(|err| anyhow::anyhow!("failed to deserialize function state: {}", err))?;
        self.state = state;
        Ok(())
    }
}

#[derive(Clone)]
pub enum FunctionType {
    Stateless(fn(&[ArrayRef]) -> Result<ArrayRef>),
    Stateful(fn() -> Box<dyn StatefulFunction>),
}

impl FunctionType {
    #[cfg(test)]
    pub fn create_stateful_fun(&self) -> Box<dyn StatefulFunction> {
        match self {
            FunctionType::Stateless(_) => panic!("not a stateful function!"),
            FunctionType::Stateful(f) => f(),
        }
    }
}

pub struct Function {
    pub namespace: Option<&'static str>,
    pub name: &'static str,
    pub signature: &'static Signature,
    pub return_type: fn(&[DataType]) -> DataType,
    pub function_type: FunctionType,
}
