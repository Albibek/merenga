use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::{
    app_from_crate, crate_authors, crate_description, crate_name, crate_version, value_t, Arg,
    SubCommand,
};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_yaml::from_reader;
use thiserror::Error;

pub static CONFIG: Lazy<Arc<Mutex<Config>>> = Lazy::new(|| Arc::new(Mutex::new(Config::default())));

#[macro_export]
macro_rules! c {
    ($path:tt) => {{
        let cfg_ = crate::config::CONFIG.lock().unwrap();
        cfg_.$path.clone()
    }};
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("I/O error")]
    Io(String, #[source] ::std::io::Error),

    #[error("config file must be string")]
    MustBeString,

    #[error("parsing yaml")]
    Parsing(#[from] serde_yaml::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct Config {
    pub(crate) file: String,

    /// Logging level
    pub verbosity: String,

    /// Number of threads, use 0 for number of CPUs
    //pub n_threads: usize,

    /// Carbon server listening address
    pub listen: SocketAddr,

    // queue size for single counting thread before packet is dropped
    //pub task_queue_size: usize,
    /// How often to gather own stats, in ms. Use 0 to disable (stats are still gathered, but not included in
    /// metric dump)
    //pub stats_interval: u64,

    /// Prefix to send own metrics with
    //pub stats_prefix: String,

    /// list of backends configuration
    pub backends: HashMap<String, Backend>,

    /// list of buckets configuration
    //pub buckets: HashMap<String, Bucket>,
    pub rules: Vec<Rule>,

    pub default_routes: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            file: String::new(),
            verbosity: "warn".to_string(),
            //n_threads: 4,
            listen: "127.0.0.1:2003".parse().unwrap(),
            //task_queue_size: 2048,
            //stats_interval: 10000,
            //stats_prefix: "resources.monitoring.bioyino".to_string(),
            backends: HashMap::new(),
            rules: Vec::new(),
        }
    }
}

impl Config {
    pub fn load_from_cli(&mut self) -> Result<(), ConfigError> {
        let (require_config, default_config) = match env::var("MERENGA_CONFIG") {
            Ok(value) => (false, value),
            Err(_) => (true, "/etc/merenga/config.yaml".into()),
        };
        // This is a first copy of args - with the "config" option
        let app = app_from_crate!()
            .long_version(concat!(
                crate_version!(),
                " ",
                env!("VERGEN_COMMIT_DATE"),
                " ",
                env!("VERGEN_SHA_SHORT")
            ))
            .arg(
                Arg::with_name("config")
                    .help("configuration file path")
                    .long("config")
                    .short("c")
                    .required(require_config)
                    .takes_value(true)
                    .default_value(&default_config),
            )
            .arg(
                Arg::with_name("verbosity")
                    .short("v")
                    .help("logging level")
                    .takes_value(true),
            )
            .subcommand(
                SubCommand::with_name("query")
                    .about("send a management command to running bioyino server")
                    .arg(
                        Arg::with_name("host")
                            .short("h")
                            .default_value("127.0.0.1:8137"),
                    )
                    .subcommand(SubCommand::with_name("status").about("get server state"))
                    .subcommand(
                        SubCommand::with_name("consensus")
                            .arg(Arg::with_name("action").index(1))
                            .arg(
                                Arg::with_name("leader_action")
                                    .index(2)
                                    .default_value("unchanged"),
                            ),
                    ),
            )
            .get_matches();
        self.file =
            value_t!(app.value_of("config"), String).map_err(|_| ConfigError::MustBeString)?;
        if let Some(v) = app.value_of("verbosity") {
            self.verbosity = v.into()
        }

        self.load()
    }

    pub fn load(&mut self) -> Result<(), ConfigError> {
        let mut file = File::open(&self.file)
            .map_err(|e| ConfigError::Io(format!("opening file at '{}'", self.file), e))?;
        let mut config: Self = serde_yaml::from_reader(&file)?;
        // all parameter postprocessing goes here
        config.prepare()?;
        config.file = self.file.clone();
        *self = config;
        Ok(())
    }

    pub fn prepare(&mut self) -> Result<(), ConfigError> {
        Ok(())
    }
}

/*
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct Bucket {
    /// time required to expire the bucket
    timer: usize,

    // /// lua function to call when bucket is ready
    // end_function: String,
    /// where to send this bucket data
    routes: Vec<String>,
}

impl Default for Bucket {
    fn default() -> Self {
        Self {
            timer: 30,
            routes: Vec::new(),
        }
    }
}
*/

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct Backend {
    /// time required to expire the bucket
    pub address: String,
}

impl Default for Backend {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:2003".parse().unwrap(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct Rule {
    /// time required to expire the bucket
    #[serde(rename(deserialize = "match"))]
    pub match_: String,

    pub routes: Vec<String>,
}

impl Default for Rule {
    fn default() -> Self {
        Self {
            match_: String::new(),
            routes: Vec::new(),
        }
    }
}
