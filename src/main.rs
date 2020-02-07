mod backend;
mod carbon;
mod config;
mod scheduler;
mod util;

use std::str::FromStr;

use log::warn;
use slog::{o, Drain, Level};

use futures::FutureExt;
use tokio::spawn;
use tokio::sync::mpsc::{channel, unbounded_channel};

use crate::carbon::CarbonServer;
use crate::scheduler::{Scheduler, SchedulingError};

use crate::config::CONFIG;
use crate::util::OwnStats;

#[tokio::main]
async fn async_main() {
    let (sched_tx, sched_rx) = channel(c!(task_queue_size));
    let (cmd_tx, cmd_rx) = unbounded_channel();
    let (own_tx, own_rx) = unbounded_channel();

    let own_stats = OwnStats::new(sched_tx.clone(), own_rx);
    spawn(own_stats.main());

    let carbon_server = CarbonServer::new(sched_tx);

    // TODO error
    spawn(carbon_server.main().map(|_| ()));

    spawn(crate::util::config_watcher(cmd_tx));

    // backends will be run by scheduler because they are dynamically created based on config
    // re-reads
    let scheduler = Scheduler::new(own_tx);
    scheduler
        .main(sched_rx, cmd_rx)
        .await
        .expect("scheduler exit");
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
    let drain = slog_async::Async::new(filter)
        .chan_size(65536)
        .overflow_strategy(slog_async::OverflowStrategy::Drop)
        .build()
        .fuse();
    let rlog = slog::Logger::root(drain, o!("program"=>"merenga"));
    // this lets root logger live as long as it needs
    let _guard = slog_scope::set_global_logger(rlog.clone());

    let _scope_guard = slog_scope::set_global_logger(rlog);
    let _log_guard = slog_stdlog::init().unwrap();

    async_main();
}
