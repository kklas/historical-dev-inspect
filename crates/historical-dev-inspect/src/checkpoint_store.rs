use std::{sync::Arc, time::Duration};

use backoff::ExponentialBackoff;
use futures::{Stream, StreamExt, stream::BoxStream};
use object_store::{ObjectStore, path::Path};
use thiserror::Error;
use tokio::{
    signal::unix::{SignalKind, signal},
    sync::watch,
};

#[derive(Error, Debug)]
pub enum CheckpointStoreError {
    #[error("Object store error: {0}")]
    ObjectStoreError(#[from] object_store::Error),
    #[error("RocksDB error: {0}")]
    RocksDBError(#[from] rocksdb::Error),
}

pub trait CheckpointStoreReader {
    fn get_checkpoint(
        &self,
        seq: u64,
    ) -> impl std::future::Future<Output = Result<Option<Vec<u8>>, CheckpointStoreError>> + Send;

    fn stream_checkpoints(
        &self,
        seqs: impl Stream<Item = u64> + Send + 'static,
        max_concurrent_workers: usize,
    ) -> impl Stream<Item = Result<(u64, Option<Vec<u8>>), CheckpointStoreError>> + Send + 'static;
}

impl CheckpointStoreReader for Arc<Box<dyn ObjectStore>> {
    async fn get_checkpoint(&self, seq: u64) -> Result<Option<Vec<u8>>, CheckpointStoreError> {
        let path = Path::from(format!("{}.chk", seq));
        let response = match self.get(&path).await {
            Ok(resp) => resp,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(CheckpointStoreError::ObjectStoreError(e)),
        };
        let bytes = response.bytes().await?;
        Ok(Some(bytes.to_vec()))
    }

    fn stream_checkpoints(
        &self,
        to_process: impl Stream<Item = u64> + Send + 'static,
        max_concurrent_workers: usize,
    ) -> impl Stream<Item = Result<(u64, Option<Vec<u8>>), CheckpointStoreError>> + Send + 'static
    {
        let store = self.clone();
        let backoff = Arc::new(ExponentialBackoff {
            initial_interval: Duration::from_millis(100),
            max_interval: Duration::from_secs(60 * 10), // 10 minutes
            multiplier: 2.0,
            max_elapsed_time: None,
            ..Default::default()
        });

        to_process
            .map(move |seq_num| {
                let store = store.clone();
                let backoff = backoff.clone();

                async move {
                    let res = backoff::future::retry(backoff.as_ref().clone(), || async {
                        match CheckpointStoreReader::get_checkpoint(&store, seq_num).await {
                            Ok(response) => match response {
                                Some(bytes) => Ok((seq_num, Some(bytes.to_vec()))),
                                None => Ok((seq_num, None)),
                            },
                            Err(e) => Err(backoff::Error::Transient {
                                err: e,
                                retry_after: None,
                            }),
                        }
                    })
                    .await;

                    if let Err(e) = &res {
                        eprintln!(
                            "Failed to process checkpoint {} after retries: {:?}",
                            seq_num, e
                        );
                    }

                    res
                }
            })
            .buffer_unordered(max_concurrent_workers)
    }
}

pub fn iterator_stream_with_graceful_shutdown<I>(iter: I) -> BoxStream<'static, u64>
where
    I: IntoIterator<Item = u64> + Send + 'static,
    I::IntoIter: Send + 'static,
{
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    tokio::spawn(async move {
        // Create the signal handlers before the `select!` block
        let mut sigterm_signal = signal(SignalKind::terminate()).unwrap();
        let mut sigint_signal = signal(SignalKind::interrupt()).unwrap();

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("Received Ctrl+C, initiating graceful shutdown...");
            }
            _ = sigterm_signal.recv() => {
                println!("Received SIGTERM, initiating graceful shutdown...");
            }
            _ = sigint_signal.recv() => {
                println!("Received SIGINT, initiating graceful shutdown...");
            }
        }

        shutdown_sender
            .send(true)
            .expect("Failed to send shutdown signal");
    });

    let shutdown_future = {
        let mut shutdown_receiver_clone = shutdown_receiver.clone();
        async move {
            shutdown_receiver_clone.changed().await.ok();
        }
    };

    Box::pin(futures::stream::iter(iter).take_until(shutdown_future))
}
