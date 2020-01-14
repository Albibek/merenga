mod carbon;
mod config;

use std::io;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

//use log::info;
use slog::{o, Drain, Level};

use bytes::Bytes;
use futures::channel::mpsc;
use futures::{FutureExt, SinkExt, StreamExt};
use tokio::spawn;

use crate::carbon::CarbonServer;
use once_cell::sync::Lazy;

use crate::config::CONFIG;

//pub static OUTS: Lazy<RwLock<mpsc::Sender<Bytes>>> = sync_lazy! {
//let (tx, _) = mpsc::channel(1);
//RwLock::new(tx)
//};

pub type Float = f64;

#[tokio::main]
async fn async_main() {
    //let carbon_server_addr: std::net::SocketAddr = //"127.0.0.1:2003".parse().expect("parsing");
    let carbon_server = CarbonServer::new();
    //spawn(carbon_server.main().map(|_|()));
    carbon_server.main().map(|_| ()).await;

    //let (tx, rx) = oneshot::channel();

    /*
     *
    tokio::spawn(async move {
        let ch_rx: mpsc::Receiver<Bytes> = rx.await.unwrap();

        ch_rx.for_each(
        //TODO maybe concurrent processing
        // rx.for_each_concurrent(
        //    None,
            |query| async move {
                client::http_query(query).await
            }).await;
    });

    // cache updater
    tokio::spawn(async move {
        use std::time::Duration;
        let tick = tokio::timer::Interval::new_interval(Duration::from_millis(10000));

        tick.for_each(
            |_| async move {
                let mut expired = (*CACHE).list_expired();
                for query in expired.drain(..) {
                    info!("REFRESH {:?}", query);
                    client::http_query(query).await
                }
            }).await;
    });
     */
}

fn main() {
    let verbosity = "info";
    let verbosity = Level::from_str(&verbosity).expect("bad verbosity");

    // load config
    {
        let mut c = CONFIG.lock().unwrap();
        c.load_from_cli().expect("loading config");
    }

    // Set logging
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let filter = slog::LevelFilter::new(drain, verbosity).fuse();
    let drain = slog_async::Async::new(filter).build().fuse();
    let rlog = slog::Logger::root(drain, o!("program"=>"bioproxy"));
    // this lets root logger live as long as it needs
    let _guard = slog_scope::set_global_logger(rlog.clone());

    let _scope_guard = slog_scope::set_global_logger(rlog);
    let _log_guard = slog_stdlog::init().unwrap();

    async_main();
}
