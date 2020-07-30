use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use log::{debug, error, warn};
use thiserror::Error;

use bytes::{BufMut, Bytes, BytesMut};
use futures::{Future, TryFutureExt, TryFuture};
use ring_channel::{RingReceiver, RingSender};
use tokio::select;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::time::interval;
use trust_dns_resolver::TokioAsyncResolver;

use bioyino_metric::{Metric, MetricName, MetricType, NamedMetric64};

use crate::c;
use crate::config::{Config, CONFIG};

#[macro_export]
macro_rules! s {
    ($path:ident) => {{
        STATS
            .$path
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }};
}

pub type RingMetricSender = RingSender<NamedMetric64>;
pub type RingMetricReceiver = RingReceiver<NamedMetric64>;
pub type MetricSender = Sender<NamedMetric64>;
pub type MetricReceiver = Receiver<NamedMetric64>;

#[derive(Error, Debug)]
pub enum OtherError {
    #[error("resolving")]
    Resolving(#[from] trust_dns_resolver::error::ResolveError),

    #[error("integer parsing error")]
    ParseInt(#[from] std::num::ParseIntError),

    #[error("no IP addresses found for {}", _0)]
    NotFound(String),
}

pub async fn resolve_with_port(host: &str, default_port: u16) -> Result<SocketAddr, OtherError> {
    let (host, port) = if let Some(pos) = host.find(':') {
        let (host, port) = host.split_at(pos);
        let port = &port[1..]; // remove ':'
        let port: u16 = port.parse()?;
        (host, port)
    } else {
        (host, default_port)
    };

    let ip = resolve_to_first(host).await?;

    Ok(SocketAddr::new(ip, port))
}

pub async fn resolve_to_first(host: &str) -> Result<IpAddr, OtherError> {
    let resolver =
        // TODO: create using tokio_from_system_conf
        //TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default()).await?;
        TokioAsyncResolver::tokio_from_system_conf().await?;

    let response = resolver.lookup_ip(host).await?;

    // Run the lookup until it resolves or errors

    // There can be many addresses associated with the name,
    response
        .iter()
        .next()
        .ok_or(OtherError::NotFound(host.to_string()))
}

#[derive(Clone)]
pub struct Backoff {
    pub delay: u64,
    pub delay_mul: f32,
    pub delay_max: u64,
    pub retries: usize,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            delay: 500,
            delay_mul: 2f32,
            delay_max: 10000,
            retries: std::usize::MAX,
        }
    }
}

impl Backoff {
    pub async fn sleep(&mut self) -> Result<(), ()> {
        if self.retries == 0 {
            Err(())
        } else {
            self.retries -= 1;
            let delay = self.delay as f32 * self.delay_mul;
            let delay = if delay <= self.delay_max as f32 {
                delay as u64
            } else {
                self.delay_max as u64
            };

            tokio::time::delay_for(Duration::from_millis(delay)).await;
            Ok(())
        }
    }
}

pub async fn retry_with_backoff<F, I, R, E>(c: Backoff, mut f: F) -> Result<R, E>
where
    I: Future<Output = Result<R, E>>,
    F: FnMut() -> I,
    {
        loop {
            let mut bo = c.clone();
            match f().await {
                r @ Ok(_) => break r,
                Err(e) => {
                    bo.sleep().map_err(|()| e).await?;
                    continue;
                }
            }
        }
    }

pub async fn config_watcher(notify: tokio::sync::mpsc::UnboundedSender<()>) {
    // An infinite stream of hangup signals.
    let mut signal_stream = signal(SignalKind::hangup()).expect("setting signal handler");

    let mut hash = c!(hash);
    // Print whenever a HUP signal is received
    loop {
        signal_stream.recv().await;
        warn!("got signal HUP");
        let reload = {
            let old_config: &mut Config = &mut CONFIG.lock().unwrap();
            let mut new_config = old_config.clone();
            match new_config.load() {
                Ok(()) => {
                    if new_config.hash != hash {
                        hash = new_config.hash;
                        *old_config = new_config;
                        warn!("config hash been reloaded");
                        //dbg!(&old_config);
                        true
                    } else {
                        warn!("config did not change");
                        false
                    }
                }
                Err(e) => {
                    error!("error loading new config: {}", e);
                    false
                }
            }
        };

        if reload {
            notify.send(()).expect("scheduler ended unexpectedly");
        }
    }
}

pub struct Stats {
    pub errors: AtomicUsize,
    pub ingress: AtomicUsize,
    pub parsing: AtomicUsize,
    pub forwards: AtomicUsize,
    pub rewrites: AtomicUsize,
    pub connects: AtomicUsize,
    pub disconnects: AtomicUsize,
}

pub static STATS: Stats = Stats {
    errors: AtomicUsize::new(0),
    ingress: AtomicUsize::new(0),
    parsing: AtomicUsize::new(0),
    forwards: AtomicUsize::new(0),
    rewrites: AtomicUsize::new(0),
    connects: AtomicUsize::new(0),
    disconnects: AtomicUsize::new(0),
};

pub struct OwnStats {
    ts: u64,
    buf: BytesMut,
    tx: MetricSender,
    backends: UnboundedReceiver<(Bytes, usize)>,
}

macro_rules! send_stat {
    ($path:ident, $name:expr, $self:ident) => {{
        let value = STATS.$path.swap(0, Ordering::Relaxed).into();
        let metric = $self.build_metric(value, None, $name.as_bytes());
        $self.tx.send(metric).map_err(|_| ()).await
    }};
}

impl OwnStats {
    pub fn new(tx: MetricSender, backends: UnboundedReceiver<(Bytes, usize)>) -> Self {
        Self {
            ts: 0,
            buf: BytesMut::new(),
            tx,
            backends,
        }
    }

    fn build_metric(
        &mut self,
        value: usize,
        suffix: Option<&[u8]>,
        postfix: &[u8],
    ) -> NamedMetric64 {
        let prefix = c!(stats_prefix);
        let suffix_len = suffix.map(|s| s.len() + 1).unwrap_or(0);
        self.buf
            .reserve(prefix.len() + 1 + suffix_len + postfix.len());

        self.buf.put(prefix.as_bytes());
        self.buf.put_u8(b'.');
        suffix.map(|s| {
            self.buf.put(s);
            self.buf.put_u8(b'.');
        });
        self.buf.put(postfix);

        let name = self.buf.split().freeze();
        let name = MetricName::from_raw_parts(name, None);
        let value =
            Metric::new(value as f64, MetricType::Gauge(None), Some(self.ts), None).unwrap();
        NamedMetric64 { name, value }
    }

    pub async fn main(mut self) -> Result<(), ()> {
        let mut interval = interval(Duration::from_millis(c!(stats_interval)));

        let mut backend_stats: HashMap<Bytes, usize> = HashMap::new();
        loop {
            select! {
                _ = interval.tick() => {
                    self.ts = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                        .into();
                    //let names = BytesMut::with_capacity();
                    send_stat!(errors, "errors", self)?;
                    send_stat!(ingress, "ingress", self)?;
                    send_stat!(parsing, "parsing-e", self)?;
                    send_stat!(forwards, "forwards", self)?;
                    send_stat!(rewrites, "rewrites", self)?;
                    send_stat!(connects, "connects", self)?;
                    send_stat!(disconnects, "disconnects", self)?;
                    for (name, value) in backend_stats.drain() {
                        let metric = self.build_metric(value, Some(&b"egress"[..]), &name[..]);
                        self.tx.send(metric).map_err(|_| ()).await?
                    }
                },
                backend = self.backends.recv() => {
                    if let Some((name, value)) = backend {
                        backend_stats.entry(name)
                            .and_modify(|prev| *prev+=value)
                            .or_insert(value);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::{Buf, BytesMut};
    use futures::{SinkExt, StreamExt, TryFutureExt, TryStream, TryStreamExt};
    use tokio::net::TcpListener;
    use tokio::time::timeout;
    use tokio_util::codec::{Decoder, Encoder};

    use bioyino_metric::{name::TagFormat, Metric, MetricName, MetricType, NamedMetric64};

    use crate::carbon::CarbonDecoder;
    use crate::config::CONFIG;

    // multiple tests are run in parallel, but we only want to parse config once
    static TEST_CONFIG_DONE: std::sync::Once = std::sync::Once::new();

    pub(crate) fn init_config() {
        TEST_CONFIG_DONE.call_once(|| {
            let mut test_config = CONFIG.lock().unwrap();
            test_config.listen = "127.0.0.1:20003".parse().unwrap();
            test_config.file = "test/fixtures/config.yaml".into();
            test_config.load().expect("loading test config");
            dbg!(&test_config);
        });
    }

    pub(crate) fn new_name_graphite(n: &[u8]) -> MetricName {
        let mut buf = Vec::with_capacity(9000);
        buf.resize(9000, 0u8);
        MetricName::new(BytesMut::from(n), TagFormat::Graphite, &mut buf).unwrap()
    }

    pub(crate) fn new_named_metric(n: &[u8], value: f64, ts: u64) -> NamedMetric64 {
        let mut buf = Vec::with_capacity(9000);
        buf.resize(9000, 0u8);
        let name = MetricName::new(BytesMut::from(n), TagFormat::Graphite, &mut buf).unwrap();
        let value = Metric::new(value, MetricType::Gauge(None), Some(ts), None).unwrap();
        NamedMetric64::new(name, value)
    }

    pub(crate) async fn asserting_listener(
        addr: SocketAddr,
        mut expected: Vec<NamedMetric64>,
        location: &'static str,
    ) -> Result<(), ()> {
        let mut listener = TcpListener::bind(&addr).await.map_err(|_| ())?;
        let (socket, _) = listener.accept().await.map_err(|_| ())?;

        let codec = CarbonDecoder::new(2048);
        let mut input = codec.framed(socket);
        let mut metrics = timeout(
            Duration::from_secs(1),
            input.map(|m| m.unwrap()).collect::<Vec<_>>(),
        )
            .await
            .map_err(|_| ())?;

        metrics.sort_by(|m1, m2| m1.name.name.partial_cmp(&m2.name.name).unwrap());
        expected.sort_by(|m1, m2| m1.name.name.partial_cmp(&m2.name.name).unwrap());
        assert_eq!(metrics, expected, "assert at {}", location);
        Ok(())
    }

    /// a simple lock based counter useful in tests
    #[derive(Clone)]
    pub struct Counter {
        inner: Arc<tokio::sync::Mutex<usize>>,
    }

    impl Counter {
        pub(crate) fn new(c: usize) -> Self {
            Self {
                inner: Arc::new(tokio::sync::Mutex::new(c)),
            }
        }

        pub(crate) async fn dec(&self) {
            let mut c = self.inner.lock().await;
            *c = *c - 1;
        }
        pub(crate) async fn decn(&self, n: usize) {
            let mut c = self.inner.lock().await;
            *c = *c - n;
        }

        pub(crate) async fn zero_after(&self, timeout: u64) -> Result<(), ()> {
            tokio::time::delay_for(Duration::from_secs(timeout)).await;
            self.is_zero()
        }

        pub(crate) fn is_zero(&self) -> Result<(), ()> {
            let counter = self.inner.try_lock().map_err(|_| ())?;
            if *counter == 0 {
                Ok::<(), ()>(())
            } else {
                Err(())
            }
        }
    }
}
