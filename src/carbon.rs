use std::net::SocketAddr;
use std::str;

use thiserror::Error;

use bytes::{Buf, BytesMut};
use futures::stream::StreamExt;
use futures::TryFutureExt;
use log::info;
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;

use tokio_util::codec::{Decoder, Encoder};

use crate::util::*;
use crate::{c, s};
use bioyino_metric::{name::TagFormat, Metric, MetricName, MetricType, NamedMetric64};

#[derive(Error, Debug)]
pub enum CarbonServerError {
    #[error("i/o error")]
    TokioIo(#[from] tokio::io::Error),

    #[error("no data to parse")]
    Empty,

    #[error("no value provided after name")]
    ValueExpected,

    #[error("no timestamp after value")]
    TimestampExpected,

    #[error("float parsing error")]
    ParseFloat(#[from] std::num::ParseFloatError),

    #[error("integer parsing error")]
    ParseInt(#[from] std::num::ParseIntError),

    #[error("utf8 parsing error")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("name parsing error")]
    Name,

    #[error("sending metric to scheduler")]
    Scheduling,
}

#[derive(Clone)]
pub struct CarbonServer {
    addr: SocketAddr,
    max_line_len: usize,
    sched: MetricSender,
}

impl CarbonServer {
    pub(crate) fn new(sched: MetricSender) -> Self {
        Self {
            addr: c!(listen),
            max_line_len: c!(carbon.max_line_len),
            sched,
        }
    }

    pub(crate) async fn main(self) -> Result<(), CarbonServerError> {
        let Self {
            addr,
            max_line_len,
            sched,
        } = self;
        let mut listener = TcpListener::bind(&addr).await?;
        loop {
            let max_line_len = max_line_len.clone();
            let (socket, _) = listener.accept().await?;
            s!(connects);
            let mut sched = sched.clone();
            let parser = async move {
                let codec = CarbonDecoder::new(max_line_len.clone());
                let mut input = codec.framed(socket);
                loop {
                    let metric = match input.next().await {
                        Some(Ok(m)) => m,
                        Some(Err(_e)) => continue, // TODO: process error
                        None => break,
                    };
                    sched
                        .send(metric)
                        .map_err(|_| CarbonServerError::Scheduling)
                        .await?;
                    // TODO error
                }
                s!(disconnects);
                Ok::<(), CarbonServerError>(())
            };

            spawn(parser);
        }
    }
}

pub struct CarbonDecoder {
    sort_buf: Vec<u8>,
    last_pos: usize,
    max_len: usize,
}

impl CarbonDecoder {
    pub fn new(max_len: usize) -> Self {
        Self {
            last_pos: 0,
            sort_buf: vec![0u8; max_len],
            max_len,
        }
    }

    fn parse_line(&mut self, mut line: BytesMut) -> Result<NamedMetric64, CarbonServerError> {
        let (name_len, value, ts) = {
            let parts = &mut line[..]
                .split(|&c| c == b' ')
                .filter(|part| !part.is_empty());
            let name_len = parts.next().ok_or(CarbonServerError::Empty)?.len();
            let value = parts.next().ok_or(CarbonServerError::ValueExpected)?;

            // TODO: probably use nom::...double and atoi crate for parsing directly from bytes to avoid
            // walking the buffer one extra time
            let value: f64 = str::from_utf8(value)?.parse()?;
            let ts = parts.next().ok_or(CarbonServerError::TimestampExpected)?;
            let ts: u64 = str::from_utf8(ts)?.parse()?;

            (name_len, value, ts)
        };

        let name = MetricName::new(
            BytesMut::from(&line.split_to(name_len)[..]),
            TagFormat::Graphite,
            &mut self.sort_buf,
        )
        .map_err(|()| CarbonServerError::Name)?;

        // unwrap is OK here
        // in carbon we don't know the type of metric, so all of them are of type gauge
        let metric = Metric::new(value, MetricType::Gauge(None), Some(ts), None).unwrap();
        Ok(NamedMetric64::new(name, metric))
    }
}

impl Decoder for CarbonDecoder {
    type Item = NamedMetric64;
    type Error = CarbonServerError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.len() == 0 {
            self.last_pos = 0;
            return Ok(None);
        }

        let log_parse_errors = c!(log_parse_errors);
        // we need  loop here for skipping parsing errors, it only continues when there is an error
        // between some newlines
        // in all good cases it just returns
        loop {
            let len = buf.len();
            // we dont' do any job until we meet a new line or we are at eof
            // (or eof, but this case is in decode_eof function)
            match &buf[self.last_pos..len].iter().position(|c| *c == b'\n') {
                Some(pos) => {
                    let line = buf.split_to(self.last_pos + *pos);
                    buf.advance(1); // remove \n itself
                    self.last_pos = 0;
                    let eline = if log_parse_errors {
                        Some(line.clone())
                    } else {
                        None
                    };
                    match self.parse_line(line) {
                        Ok(res) => return Ok(Some(res)),
                        Err(e) => {
                            s!(parsing);
                            if let Some(line) = eline {
                                info!(
                                    "parsing metric from {:?}: {:?}",
                                    String::from_utf8_lossy(&line),
                                    e
                                );
                            }
                            continue;
                        }
                    }
                }
                None => {
                    // we've scanned the buffer and found no newlines
                    if self.last_pos > self.max_len + 1 {
                        // when out of max_len, cut the shit out of the whole buffer
                        // TODO: log error here
                        buf.advance(self.last_pos);
                        self.last_pos = 0;
                    } else {
                        // otherways it's still ok
                        self.last_pos = len - 1;
                    }
                    //  regardless of result we have not anough data to do anything
                    return Ok(None);
                }
            }
        }
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // even at EOF we may have many newline-separated metrics, so first we try
        // the original decode
        match self.decode(buf)? {
            res @ Some(_) => Ok(res),
            None => {
                // after decode returned None we are sure buffer does not contain newlines,
                // so we try to parse the rest of it
                // TODO consider this in unit tests
                match self.parse_line(buf.split()) {
                    Ok(res) => return Ok(Some(res)),
                    Err(e) => {
                        if let CarbonServerError::Empty = e {
                        } else {
                            info!("parsing metric: {:?}", e);
                        }
                        s!(parsing);
                        return Ok(None);
                    }
                }
            }
        }
    }
}

impl Encoder for CarbonDecoder {
    type Item = ();
    type Error = CarbonServerError;

    fn encode(&mut self, _: Self::Item, _buf: &mut BytesMut) -> Result<(), Self::Error> {
        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::prelude::*;
    use tokio::runtime::Builder;
    use tokio::sync::mpsc::channel;

    use crate::config::CONFIG;
    use crate::util::test::*;

    #[test]
    fn carbon_server() {
        init_config();
        let input = Bytes::from("gorets.bobez.1;tag2=value;tag1=value 1000 1576644030\n\ngorets.bobez.2;tag2=value2;tag1=value   -10e4   1576644030\n");

        let mut runtime = Builder::new()
            .basic_scheduler()
            .enable_all()
            .build()
            .expect("creating runtime for main thread");

        let (sched_tx, mut sched_rx) = channel(c!(task_queue_size));
        // spawn carbon server
        let server = CarbonServer::new(sched_tx);
        runtime.spawn(server.main());

        let counter = Counter::new(2);
        let expected = vec![
            new_named_metric(b"gorets.bobez.1;tag1=value;tag2=value", 1000.0, 1576644030),
            new_named_metric(
                &b"gorets.bobez.2;tag1=value;tag2=value2"[..],
                -100000.0,
                1576644030,
            ),
        ];

        let c = counter.clone();
        let receiver = async move {
            let metric = sched_rx.recv().await.unwrap();

            assert_eq!(metric, expected[0]);
            c.dec().await;

            let metric = sched_rx.recv().await.unwrap();
            //dbg!(&metric);
            assert_eq!(metric, expected[1]);
            c.dec().await;

            // TODO: must be None when server close implemented
            // TODO: make counter = 3 and dec it here to check if await was really unblocked
            sched_rx.recv().await.unwrap();
        };

        runtime.spawn(receiver);

        // connect to same address server is listening
        let addr: SocketAddr = c!(listen);
        let sender = async move {
            let mut conn = TcpStream::connect(&addr).await?;
            conn.write_all(&input).await?;
            Ok::<(), io::Error>(())
        };

        runtime.spawn(sender);

        runtime.block_on(counter.zero_after(3)).unwrap();
    }
}
