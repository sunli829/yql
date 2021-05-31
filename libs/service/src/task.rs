use std::sync::Arc;

use anyhow::Result;
use futures_util::StreamExt;
use tokio::sync::{oneshot, Mutex};
use tokio::time::Interval;
use yql_core::{BoxSink, DataStream};

use crate::service::ServiceInner;
use crate::storage::StreamState;

async fn internal_start_task(
    service: Arc<Mutex<ServiceInner>>,
    name: String,
    mut interval: Interval,
    mut stream: DataStream,
    mut sink: BoxSink,
) -> Result<()> {
    let (tx_shutdown, mut rx_shutdown) = oneshot::channel::<()>();
    let mut inner = service.lock().await;
    inner
        .storage
        .set_stream_state(&name, StreamState::Started)?;
    inner.registry.add(&name, tx_shutdown);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let data = stream.save_state()?;
                let inner = service.lock().await;
                inner.storage.set_stream_state_data(&name, &data)?;
            }
            _ = &mut rx_shutdown => {
                let data = stream.save_state()?;
                let inner = service.lock().await;
                inner.storage.set_stream_state_data(&name, &data)?;
                return Ok(());
            }
            item = stream.next() => {
                match item {
                    Some(Ok(dataset)) => sink.send(dataset).await?,
                    Some(Err(err)) => {
                        let data = stream.save_state()?;
                        let inner = service.lock().await;
                        inner.storage.set_stream_state_data(&name, &data)?;
                        return Err(err);
                    }
                    None => {
                        let data = stream.save_state()?;
                        let inner = service.lock().await;
                        inner.storage.set_stream_state_data(&name, &data)?;
                        return Ok(());
                    }
                }
            }
        }
    }
}

pub async fn start_task(
    service: Arc<Mutex<ServiceInner>>,
    name: String,
    interval: Interval,
    stream: DataStream,
    sink: BoxSink,
) {
    let res = internal_start_task(service.clone(), name.clone(), interval, stream, sink).await;
    let mut inner = service.lock().await;
    inner
        .storage
        .set_stream_state(
            &name,
            match res {
                Ok(()) => StreamState::Stop,
                Err(err) => StreamState::Error(err.to_string()),
            },
        )
        .ok();
    inner.registry.remove(&name);
}
