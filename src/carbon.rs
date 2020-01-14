use std::io::Write;
use std::net::SocketAddr;
use std::str;

use thiserror::Error;

use bytes::{Buf, BytesMut};
use futures::stream::{StreamExt, TryStreamExt};
use futures::{future, TryFutureExt};
use log::info;
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;

use tokio_util::codec::{Decoder, Encoder};

use crate::c;
use crate::Float;
use bioyino_metric::{name::TagFormat, Metric, MetricName, MetricType};

#[derive(Error, Debug)]
pub enum CarbonServerError {
    #[error("i/o error")]
    TokioIo(#[from] tokio::io::Error),

    #[error("spawning blocking task")]
    SpawnBlocking(#[from] tokio::task::JoinError),

    #[error("decode error")]
    Decode,

    #[error("float parsing error")]
    ParseFloat(#[from] std::num::ParseFloatError),

    #[error("integer parsing error")]
    ParseInt(#[from] std::num::ParseIntError),

    #[error("utf8 parsing error")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("name parsing error")]
    Name,
}

#[derive(Clone)]
pub struct CarbonServer {
    addr: SocketAddr,
    max_line_len: usize,
}

impl CarbonServer {
    //pub(crate) fn new(addr: SocketAddr, channel: Sender<Metric<Float>>, log: Logger) -> Self {
    pub(crate) fn new() -> Self {
        Self {
            addr: c!(listen),
            max_line_len: 65535,
        }
    }

    pub(crate) async fn main(self) -> Result<(), CarbonServerError> {
        let Self { addr, max_line_len } = self;
        let mut listener = TcpListener::bind(&addr).await?;
        loop {
            let (mut socket, _) = listener.accept().await?;
            let codec = CarbonDecoder::new(max_line_len);
            let parser = codec.framed(socket).try_for_each(|metric| {
                tokio::task::spawn_blocking(|| {
                    // TODO "smart batching":  stack+heap vector or rope-based something
                    dbg!(metric);
                })
                .map_err(|e| e.into())
            });
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

    fn parse_line(
        &mut self,
        mut line: BytesMut,
    ) -> Result<(MetricName, Metric<Float>), CarbonServerError> {
        let (name_len, value, ts) = {
            let parts = &mut line[..]
                .split(|&c| c == b' ')
                .filter(|part| !part.is_empty());
            let name_len = parts.next().ok_or(CarbonServerError::Decode)?.len();
            let value = parts.next().ok_or(CarbonServerError::Decode)?;

            // TODO: probably use nom::...double and atoi crate for parsing directly from bytes to avoid
            // walking the buffer one extra time
            let value: f64 = str::from_utf8(value)?.parse()?;
            let ts = parts.next().ok_or(CarbonServerError::Decode)?;
            let ts: u64 = str::from_utf8(ts)?.parse()?;

            (name_len, value, ts)
        };

        let name = MetricName::new(
            bytes4::BytesMut::from(&line.split_to(name_len)[..]),
            TagFormat::Graphite,
            &mut self.sort_buf,
        )
        .map_err(|()| CarbonServerError::Name)?;

        // unwrap is OK here
        // in carbon we don't know the type of metric, so all of them are of type gauge
        let metric = Metric::new(value, MetricType::Gauge(None), Some(ts), None).unwrap();
        Ok((name, metric))
    }
}

impl Decoder for CarbonDecoder {
    type Item = (MetricName, Metric<Float>);
    type Error = CarbonServerError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.len() == 0 {
            self.last_pos = 0;
            return Ok(None);
        }

        // we need  loop here for skipping parsing errors, it only continues when there is an error
        // between some newlines
        // in all good cases it just returns
        loop {
            let len = buf.len();
            // we dont' do any job until we meet a new line or we are at eof
            // (or eof, but this case is in decode_eof function)
            match &buf[self.last_pos..len - 1].iter().position(|c| *c == b'\n') {
                Some(pos) => {
                    let line = buf.split_to(*pos);
                    buf.advance(1); // remove \n itself
                    self.last_pos = 0;
                    match self.parse_line(line) {
                        Ok(res) => return Ok(Some(res)),
                        Err(e) => {
                            // TODO Count errors as metric
                            info!("parsing metric: {:?}", e);
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
                        // otherway it's still ok
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
                        // TODO Count errors as metric
                        info!("parsing metric: {:?}", e);
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
        todo!()
    }
}

/*
pub struct CarbonEncoder;

impl Encoder for CarbonEncoder {
    type Item = (Bytes, Metric<Float>); // Metric name, value and timestamp
    type Error = Error;

    fn encode(&mut self, m: Self::Item, buf: &mut BytesMut) -> Result<(), Self::Error> {
        buf.reserve(m.0.len() + 20 + 20); // FIXME: too long to count, just allocate 19 bytes for float and 19 for u64
        buf.put(m.0);
        buf.put(" ");

        let mut wr = buf.writer();
        ftoa::write(&mut wr, m.1.value).map_err(|_| GeneralError::CarbonBackend)?;
        wr.write(&b" "[..]).unwrap();

        itoa::write(&mut wr, m.1.timestamp.unwrap()).map_err(|_| GeneralError::CarbonBackend)?;
        wr.write(&b"\n"[..]).unwrap();

        // let len = m.0.len() + 1 + m.1.len() + 1 + m.2.len() + 1;
        //buf.reserve(len);
        //buf.put(m.0);
        //buf.put(" ");
        //buf.put(m.1);
        //buf.put(" ");
        //buf.put(m.2);
        // buf.put("\n");
        Ok(())
    }
}

impl Decoder for CarbonEncoder {
    type Item = ();
    // It could be a separate error here, but it's useless, since there is no errors in process of
    // encoding
    type Error = GeneralError;

    fn decode(&mut self, _buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        unreachable!()
    }
}
*/

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use bytes::Bytes;
    use once_cell::sync::Lazy;
    use tokio::prelude::*;
    use tokio::runtime::Builder;

    use crate::config::CONFIG;

    /*
        #[test]
        fn carbon_parser() {
            /*
            let lines = [
                b"gorets.bobez;tag2=value;tag1=value 1000 1576644030",
                b"gorets.bobez;tag2=value;tag1=value   -10e4   1576644030",
            ];
            */

            //
        }
    */

    fn init_config() {
        let mut test_config = CONFIG.lock().unwrap();
        test_config.listen = "127.0.0.1:20003".parse().unwrap();
        test_config.file = "test/fixtures/config.yaml".into();
        test_config.load().expect("loading test config");
        dbg!(&test_config);
    }

    #[test]
    fn carbon_server() {
        init_config();
        let input = Bytes::from("gorets.bobez.1;tag2=value;tag1=value 1000 1576644030\n\ngorets.bobez.2;tag2=value;tag1=value   -10e4   1576644030");
        //let ts = SystemTime::now().duration_since(time::UNIX_EPOCH).unwrap();
        //let ts: Bytes = ts.as_secs().to_string().into();

        let mut runtime = Builder::new()
            .basic_scheduler()
            .enable_all()
            .build()
            .expect("creating runtime for main thread");
        //let (bufs_tx, bufs_rx) = channel(10);

        // spawn carbon server
        let server = CarbonServer::new();
        runtime.spawn(server.main());
        // let receiver = bufs_rx.for_each(move |msg| {
        ////if bufs_rx != "qwer.asdf.zxcv1 20 "
        ////TODO correct test
        //println!("RECV: {:?}", msg);
        //Ok(())
        //});

        //runtime.spawn(receiver);

        // connect to same address server is listening
        let addr: SocketAddr = c!(listen);
        let sender = async move {
            let mut conn = TcpStream::connect(&addr).await?;
            conn.write_all(&input).await?;
            Ok::<(), io::Error>(())
        };
        runtime.spawn(sender);
        //let writer = CarbonEncoder.framed(conn);
        //let sender = writer
        //.send(("qwer.asdf.zxcv1".into(), "10".into(), ts.clone()))
        //.and_then(move |writer| {
        //writer.send(("qwer.asdf.zxcv2".into(), "20".into(), ts.clone()))
        //})
        //.map(|_| ())
        //.map_err(|_| ());
        //spawn(sender.map_err(|_| ()));
        //Ok(())
        //});

        //runtime.spawn(sender.map_err(|_| ()));
        //runtime.block_on(receiver).expect("runtime");
        let test_delay = async { tokio::time::delay_for(Duration::from_secs(2)).await };
        runtime.block_on(test_delay);
    }
}
