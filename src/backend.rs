use std::io::Write;
use std::time::Duration;
use thiserror::Error;

use bytes::{buf::BufMutExt, BufMut, Bytes, BytesMut};
use futures::{Future, SinkExt, StreamExt, TryFutureExt, TryStream};
use log::{debug, error, warn};
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedSender;
use tokio::{select, spawn};
use tokio_util::codec::{Decoder, Encoder};

use ring_channel::ring_channel;

use bioyino_metric::NamedMetric64;

use crate::config::Backend;
use crate::util::*;
use crate::{c, s};

#[derive(Error, Debug)]
pub enum CarbonClientError {
    #[error("network i/o error")]
    TokioIo(#[from] tokio::io::Error),

    #[error("other error")]
    Util(#[from] OtherError),

    #[error("no retries left for backend")]
    NoRetries,
}

#[derive(Clone)]
pub struct CarbonClient {
    pub(crate) addr: String,
    name: Bytes,
    input: RingMetricReceiver,
    own: UnboundedSender<(Bytes, usize)>,
    backoff: Backoff,
    scale: usize,
    config_hash: u64,
}

impl CarbonClient {
    pub(crate) fn new(
        config: Backend,
        name: String,
        own: UnboundedSender<(Bytes, usize)>,
    ) -> (Self, RingMetricSender) {
        let (tx, rx) = ring_channel(config.checked_queue_len);
        (
            Self {
                addr: config.address,
                name: Bytes::copy_from_slice(name.as_bytes()),
                input: rx,
                own,
                backoff: Backoff::default(),
                scale: config.scale,
                config_hash: c!(hash),
            },
            tx,
        )
    }

    pub(crate) async fn scaled(self) {
        let mut handles = Vec::new();
        for _ in 0..self.scale {
            handles.push(spawn(self.clone().retried()));
        }
        futures::future::join_all(handles).await;
    }

    pub(crate) async fn retried(mut self) -> Result<(), CarbonClientError> {
        loop {
            let addr = self.addr.clone();
            let backend = self.clone();
            let name = String::from_utf8_lossy(&self.name[..]);
            if c!(hash) != self.config_hash {
                warn!(
                    "backend '{}' stopped {:?} due to config reload",
                    &name, addr
                );
                break;
            }
            match backend.main().await {
                Ok(()) => {
                    warn!("backend '{}' stopped {:?}", &name, addr);
                    break;
                }
                Err(e) => {
                    error!("error starting backend '{}' {:?}: {:?}", &name, addr, e);
                    self.backoff
                        .sleep()
                        .map_err(|e| {
                            error!("gave up retrying backend '{}' {:?}: {:?}", &name, addr, e);
                            CarbonClientError::NoRetries
                        })
                        .await?;
                    continue;
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn main(mut self) -> Result<(), CarbonClientError> {
        debug!("starting backend");
        let addr = resolve_with_port(&self.addr, 2003).await?;
        let conn = TcpStream::connect(addr.clone()).await?;
        let mut codec = CarbonEncoder.framed(conn);

        let backend_loop = async move {
            let mut egress = 0;
            let mut interval = tokio::time::interval(Duration::from_millis(1000));

            loop {
                select! {
                    // on input data - encode and send it
                    m = self.input.next() => {
                        match m {
                            Some(m) => {
                                egress += 1;
                                codec.send(m).map_err(|e|{s!(errors); e}).await?
                            },
                            None => break,
                        }
                    },
                    // on timer tick - gather stats and send them to stats collector
                    _ = interval.tick() => {
                        self.own.send((self.name.clone(), egress)).unwrap();
                        egress = 0;
                    }
                }
            }
            Ok::<_, CarbonClientError>(())
        };
        backend_loop.await
    }
}

pub struct CarbonEncoder;

impl Encoder for CarbonEncoder {
    type Item = NamedMetric64;
    type Error = CarbonClientError;

    fn encode(&mut self, m: Self::Item, buf: &mut BytesMut) -> Result<(), Self::Error> {
        // it's too long to count characters for each number
        // anyways, we most probably write more that one metric, so allocating 20 bytes for float and 20 for u64 saves CPU at the cost of little memory and
        // few extra allocs in rare edge cases
        buf.reserve(m.name.name.len() + 20 + 20);
        buf.put(&m.name.name[..]);
        buf.put_u8(b' ');

        let mut wr = buf.writer();
        ftoa::write(&mut wr, m.value.value)?;
        wr.write(&b" "[..])?;

        // in impossible case metric does not have timestamp, use zero
        let ts = m.value.timestamp.unwrap_or(0u64);
        itoa::write(&mut wr, ts)?;
        wr.write(&b"\n"[..])?;

        Ok(())
    }
}

impl Decoder for CarbonEncoder {
    type Item = ();
    type Error = CarbonClientError;

    fn decode(&mut self, _buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::TryFutureExt;
    use tokio::runtime::Builder;

    use tokio::sync::mpsc::unbounded_channel;

    use crate::util::test::*;

    #[test]
    fn carbon_single_backend() {
        init_config();

        let mut runtime = Builder::new()
            .basic_scheduler()
            .enable_all()
            .build()
            .expect("creating runtime for test thread");

        let mut cfg = crate::config::Backend::default();
        cfg.address = "localhost:2004".into();
        cfg.scale = 4;
        cfg.build().unwrap();
        let (own_tx, mut own_rx) = unbounded_channel();
        runtime.spawn(async move { for _ in own_rx.recv().await {} });

        let (backend, mut tx) = CarbonClient::new(cfg, "test_backend".into(), own_tx);
        let metrics = vec![
            new_named_metric(
                &b"apps.gorets.bobez.1;tag1=value1;tag2=value2"[..],
                10001.0,
                1576644030,
            ),
            new_named_metric(
                &b"apps.gorets.bobez.2;tag1=value1;tag2=value2"[..],
                10002.0,
                1576644030,
            ),
        ];

        let expected = metrics.clone();

        let sender = async move {
            for m in metrics {
                tx.send(m).unwrap();
            }
            Ok::<(), ()>(())
        };

        let listener =
            asserting_listener("127.0.0.1:2004".parse().unwrap(), expected, "backend_test");

        let counter = Counter::new(0);
        let everything = async {
            futures::try_join!(
                counter.zero_after(3),
                sender,
                listener,
                backend.main().map_err(|_| ())
            )
        };

        runtime.block_on(everything).unwrap();
        // TODO: internal metric of failed metrics should be 0
    }
}
