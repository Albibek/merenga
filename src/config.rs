use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use clap::{
    app_from_crate, crate_authors, crate_description, crate_name, crate_version, value_t, Arg,
    SubCommand,
};
use once_cell::sync::Lazy;
use regex::bytes::Regex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub static CONFIG: Lazy<Arc<Mutex<Config>>> = Lazy::new(|| Arc::new(Mutex::new(Config::default())));

#[macro_export]
macro_rules! c {
    ($path:ident) => {{
        let cfg_ = crate::config::CONFIG.lock().unwrap();
        cfg_.$path.clone()
    }};
    ($path:ident.$subpath:ident) => {{
        let cfg_ = crate::config::CONFIG.lock().unwrap();
        cfg_.$path.$subpath.clone()
    }};
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("I/O error")]
    Io(String, #[source] ::std::io::Error),

    #[error("parsing regular expression error")]
    Regex(#[source] regex::Error),

    #[error("config file must be string")]
    MustBeString,

    #[error("parsing yaml")]
    Parsing(#[from] serde_yaml::Error),

    #[error("backend {} not found in config", _0)]
    UnknownBackend(String),

    #[error("duplicate name used in cluster and backends specs '{}'", _0)]
    DuplicateBackend(String),

    #[error("option '{}' cannot be zero", _0)]
    MustBeNonZero(&'static str),
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct Config {
    pub(crate) file: String,
    pub(crate) hash: u64,

    /// Logging level
    pub verbosity: String,

    /// Number of scheduler threads, use 0 for number of CPUs
    pub workers: usize,

    /// Carbon server listening address
    pub listen: SocketAddr,

    /// queue size for incoming metrics before backpressure is applied
    pub task_queue_size: usize,

    /// How often to gather own stats, in ms. Use 0 to disable (stats are still gathered, but not included in
    /// metric dump)
    pub stats_interval: u64,

    /// Prefix to send own metrics with
    pub stats_prefix: String,

    /// If parse errors should be print to logs
    pub log_parse_errors: bool,

    /// Settings for carbon protocol processing
    pub carbon: Carbon,

    /// list of backends configuration
    pub backends: BTreeMap<String, Backend>,

    /// list of clusters configuration
    pub clusters: BTreeMap<String, Cluster>,

    /// list of buckets configuration
    //pub buckets: BTreeMap<String, Bucket>,
    pub rules: Vec<Rule>,

    /// rule for metrics that didn't match all other rules
    /// aka catch-all rule
    pub nomatch: Rule,
}

impl Default for Config {
    fn default() -> Self {
        let mut nomatch = Rule::default();
        nomatch.stop = true;

        Self {
            file: String::new(),
            hash: 0,
            verbosity: "warn".to_string(),
            workers: 2,
            listen: "127.0.0.1:2003".parse().unwrap(),
            task_queue_size: 65535,
            stats_interval: 25000,
            stats_prefix: "resources.monitoring.merenga".to_string(),
            log_parse_errors: false,
            carbon: Carbon::default(),
            backends: BTreeMap::new(),
            clusters: BTreeMap::new(),
            rules: Vec::new(),
            nomatch,
        }
    }
}

impl Config {
    pub fn load_from_cli(&mut self) -> Result<(), ConfigError> {
        let (require_config, default_config) = match env::var("MERENGA_CONFIG") {
            Ok(value) => (false, value),
            Err(_) => (true, "/etc/merenga/config.yaml".into()),
        };

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
            .get_matches();
        self.file =
            value_t!(app.value_of("config"), String).map_err(|_| ConfigError::MustBeString)?;
        if let Some(v) = app.value_of("verbosity") {
            self.verbosity = v.into()
        }

        self.load()
    }

    pub fn load(&mut self) -> Result<(), ConfigError> {
        let file = File::open(&self.file)
            .map_err(|e| ConfigError::Io(format!("opening file at '{}'", self.file), e))?;
        let mut config: Self = serde_yaml::from_reader(&file)?;
        // all parameter postprocessing goes here
        config.prepare()?;
        config.file = self.file.clone();
        config.hash = 0; // always count hash with hash field itself set to 0
        let mut hasher = DefaultHasher::new();
        config.hash(&mut hasher);
        config.hash = hasher.finish();
        *self = config;
        Ok(())
    }

    pub fn prepare(&mut self) -> Result<(), ConfigError> {
        let cloned = self.clone(); // reborrowed self is too damn hard
        for rule in &mut self.rules {
            rule.build(&cloned)?
        }

        self.stats_prefix = self.stats_prefix.trim_end_matches('.').to_string();
        for (_, backend) in &mut self.backends {
            backend.build()?
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct Carbon {
    pub max_line_len: usize,
}

impl Default for Carbon {
    fn default() -> Self {
        Self {
            max_line_len: 65535,
        }
    }
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct Backend {
    /// address of a backend
    pub address: String,

    /// lenght of the ring buffer in items(i.e. metrics)
    pub queue_len: usize,

    /// Number of instances of this backend
    pub scale: usize,

    // the value we'll get after checking size
    #[serde(skip)]
    pub(crate) checked_queue_len: NonZeroUsize,
}

impl Default for Backend {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:2003".parse().unwrap(),
            queue_len: 65536,
            checked_queue_len: NonZeroUsize::new(65536).unwrap(),
            scale: 1,
        }
    }
}

impl Backend {
    pub fn build(&mut self) -> Result<(), ConfigError> {
        self.checked_queue_len =
            NonZeroUsize::new(self.queue_len).ok_or(ConfigError::MustBeNonZero("queue-len"))?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum ClusterStrategy {
    Even,
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct Cluster {
    /// list of backend addresses to send metrics to
    pub backends: Vec<String>,

    /// strategy to use for multiple backends
    pub strategy: ClusterStrategy,
}

impl Default for Cluster {
    fn default() -> Self {
        Self {
            backends: Vec::new(),
            strategy: ClusterStrategy::Even,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct Rule {
    /// time required to expire the bucket
    #[serde(rename(deserialize = "match"))]
    pub match_: String,

    #[serde(skip)]
    pub(crate) re: Regex,

    pub forward: Option<Vec<String>>,
    pub rewrite: Option<String>,

    pub stop: bool,
}

impl Default for Rule {
    fn default() -> Self {
        Self {
            match_: String::new(),
            re: Regex::new(&String::new()).unwrap(),
            forward: None,
            rewrite: None,
            stop: false,
        }
    }
}

impl Hash for Rule {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.match_.hash(state);
        self.forward.hash(state);
        self.rewrite.hash(state);
        self.stop.hash(state);
    }
}

impl Rule {
    fn build(&mut self, config: &Config) -> Result<(), ConfigError> {
        // forward must contain only available backends or clusters
        if let Some(list) = &self.forward {
            for backend in list {
                if !config.backends.contains_key(backend) && !config.clusters.contains_key(backend)
                {
                    return Err(ConfigError::UnknownBackend(backend.clone()));
                } else if config.backends.contains_key(backend)
                    && config.clusters.contains_key(backend)
                {
                    return Err(ConfigError::DuplicateBackend(backend.clone()));
                }
            }
        }
        self.re = Regex::new(&self.match_).map_err(ConfigError::Regex)?;
        Ok(())
    }
}
