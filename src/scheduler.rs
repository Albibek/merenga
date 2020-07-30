use std::sync::{Arc, Mutex};
use std::time::Duration;

use thiserror::Error;

use bioyino_metric::NamedMetric64;
use bytes::Bytes;
use futures::TryFutureExt;
use log::error;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::Semaphore;
use tokio::task::spawn_blocking;
use tokio::{join, select, spawn};

use crate::backend::CarbonClient;
use crate::config::Rule;
use crate::util::*;
use crate::{c, s};

#[derive(Error, Debug)]
pub enum SchedulingError {
    #[error("spawning blocking task")]
    SpawnBlocking(#[from] tokio::task::JoinError),
}

pub struct RuleJob {
    outputs: Arc<Vec<Mutex<RingMetricSender>>>,
    offsets: Arc<Vec<usize>>,
    rules: Arc<Vec<Rule>>,
}

impl RuleJob {
    fn run(self, mut metric: NamedMetric64) {
        let last_idx = self.rules.len() - 1; // shold be safe since we always add nomatch at the end
        for (i, rule) in self.rules.iter().enumerate() {
            if rule.re.is_match(&metric.name.name[..]) || i == last_idx {
                self.apply_rule(i, rule, &mut metric);
                if rule.stop {
                    // we've set unmatched.stop = true before, so we always stop at last rule
                    break;
                }
            }
        }
    }

    fn apply_rule(&self, idx: usize, rule: &Rule, metric: &mut NamedMetric64) {
        // TODO: document that (obviously) rewrite is done before forwarding if met in the same
        // rule

        if let Some(ref replace) = rule.rewrite {
            let replaced = rule.re.replace(&metric.name.name[..], replace.as_bytes());
            // TODO detect if Cow is not copied and try to leave original slice
            metric.name.name = Bytes::copy_from_slice(&replaced[..]);
            s!(rewrites);
            //dbg!("REPLACE", idx);
        }
        if rule.forward.is_some() {
            //dbg!("FORWARD", idx);
            let output_idx = self.offsets[idx];
            let mut output = self.outputs[output_idx].lock().unwrap();
            // send is not a Sink method, but RingSender's, which is always
            // non-blocking by design
            // TODO: process task scheduling error somehow (find a way to detect if buffer was
            // overflown or not)
            match output.send(metric.clone()) {
                Ok(()) => {
                    s!(forwards);
                }
                Err(e) => {
                    //dbg!("FORWARD ERR", e);
                    s!(errors);
                    // TODO: count loss (must be unreachable actually)
                    ()
                }
            };
        }
    }
}

pub struct Scheduler {
    //outputs: Arc<Vec<RingMetricSender>>,
    outputs: Arc<Vec<Mutex<RingMetricSender>>>,
    offsets: Arc<Vec<usize>>,
    backends: Vec<CarbonClient>,
    rules: Arc<Vec<Rule>>,
    semaphore: Arc<Semaphore>,

    own: UnboundedSender<(Bytes, usize)>,
}

impl Scheduler {
    pub(crate) fn new(own: UnboundedSender<(Bytes, usize)>) -> Self {
        let semaphore = Arc::new(Semaphore::new(c!(workers)));
        let mut s = Self {
            outputs: Arc::new(Vec::new()),
            offsets: Arc::new(Vec::new()),
            backends: Vec::new(),
            rules: Arc::new(Vec::new()),
            own,
            semaphore,
        };
        s.reload();
        s
    }

    fn reload(&mut self) {
        // since we are the only one who owns backend tx-es,
        // they will be automatically dropped
        let mut rules = c!(rules);
        let backends_cfg = c!(backends);
        let clusters_cfg = c!(clusters);

        // each rule can lead to multiple sending attempts
        // so for this to be fast, we match each rule index with an offset in outputs vector
        // to jump right at the start of channels where we should begin sending with
        let mut offsets = Vec::with_capacity(rules.len());

        let mut outputs = Vec::new();
        let mut backends = Vec::new();
        let mut unmatched = c!(nomatch).clone();
        unmatched.stop = true; // always stop at last rule
        rules.push(unmatched);
        for rule in &rules {
            offsets.push(outputs.len());
            if let Some(ref fw_outs) = rule.forward {
                for output in fw_outs {
                    if let Some(config) = backends_cfg.get(output) {
                        let (backend, backend_tx) =
                            CarbonClient::new(config.clone(), output.clone(), self.own.clone());
                        outputs.push(Mutex::new(backend_tx));
                        backends.push(backend);
                    } else if let Some(config) = clusters_cfg.get(output) {
                        todo!("clusters");
                    }
                }
            }
        }

        self.semaphore = Arc::new(Semaphore::new(c!(workers)));
        self.backends = backends;
        self.outputs = Arc::new(outputs);
        self.offsets = Arc::new(offsets);
        self.rules = Arc::new(rules);
    }

    pub(crate) async fn main(
        mut self,
        mut input: MetricReceiver,
        mut reload: UnboundedReceiver<()>,
    ) -> Result<(), SchedulingError> {
        loop {
            let schedule = async {
                // start the backends
                self.backends
                    .drain(..)
                    .map(|backend| {
                        spawn(backend.scaled());
                    })
                    .last();

                loop {
                    // semaphore limits a number of parallel jobs running.
                    // Since we want not more than N rule-matching jobs executing at a time, we try to
                    // acquire the semaphore along with the channel reading

                    //.matcIh join!(input.recv(), self.semaphore.acquire()) {
                    let receive = async { join!(input.recv(), self.semaphore.acquire()) };
                    select! {
                        _ = reload.recv() => break Ok::<_, SchedulingError>(Some(())),
                        v = receive => match v {
                            (Some(metric), permit) => {
                                s!(ingress);
                                permit.forget();
                                let semaphore = self.semaphore.clone();
                                let job = RuleJob {
                                    outputs: self.outputs.clone(),
                                    offsets: self.offsets.clone(),
                                    rules: self.rules.clone(),
                                };

                                // TODO "smart batching":  stack+heap vector or rope-based something
                                spawn(async move {
                                    job.run(metric);
                                    semaphore.add_permits(1);
                                    futures::future::ok::<_, ()>(())
                                });
                            }
                            (None, _) => break Ok(None),
                        },
                        else => break Ok(None),
                    }
                }
            };
            match schedule.await? {
                Some(()) => {
                    self.reload();
                    continue;
                }
                None => break,
            }
        }

        Ok::<(), SchedulingError>(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::FutureExt;
    use tokio::runtime::Builder;
    use tokio::sync::mpsc::channel;
    use tokio::sync::mpsc::unbounded_channel;

    use crate::c;
    use crate::util::test::*;

    #[test]
    fn schedule_everything() {
        init_config();

        let mut runtime = Builder::new()
            .basic_scheduler()
            .enable_all()
            .build()
            .expect("creating runtime for main thread");

        let (mut sched_tx, sched_rx) = channel(c!(task_queue_size));
        let (own_tx, mut own_rx) = unbounded_channel();
        runtime.spawn(async move { for _ in own_rx.recv().await {} });

        let scheduler = Scheduler::new(own_tx);
        let metrics = vec![
            // must be fileterd out
            new_named_metric(
                &b"bad-bad-prefix123.gorets.bobez.1;tag1=value;tag2=value"[..],
                1000.0,
                1576644030,
            ),
            // must have rewritten name
            new_named_metric(
                &b"rewrite-me.gorets.bobez.2;tag1=value1;tag2=value2"[..],
                10001.0,
                1576644030,
            ),
            // must be sent to main backend AND unmathced backend (because stop = false)
            new_named_metric(
                &b"apps.gorets.bobez.2;tag1=value1;tag2=value2"[..],
                10000.0,
                1576644030,
            ),
            // must be sent to unmatched backend
            new_named_metric(
                &b"unmatched.gorets.bobez.2;tag1=value1;tag2=value2"[..],
                10000.0,
                1576644030,
            ),
        ];

        let mut expected_main = vec![metrics[2].clone()];
        let expected_rewritten = vec![new_named_metric(
            &b"apps.rewritten.gorets.bobez.2;tag1=value1;tag2=value2"[..],
            10001.0,
            1576644030,
        )];

        let mut expected_unmatched = vec![
            //
            metrics[2].clone(),
            metrics[3].clone(),
        ];

        let sender = async move {
            for m in metrics {
                sched_tx.send(m).await.unwrap();
            }
            Ok::<(), ()>(())
        };

        let counter = Counter::new(3);
        let main_listener = asserting_listener(
            "127.0.0.1:2004".parse().unwrap(),
            expected_main,
            "main_listener",
        )
        .and_then(|_| counter.dec().unit_error());

        let unmatched_listener = asserting_listener(
            "127.0.0.1:2005".parse().unwrap(),
            expected_unmatched,
            "unmatched_listener",
        )
        .and_then(|_| counter.dec().unit_error());

        let rewrite_listener = asserting_listener(
            "127.0.0.1:2006".parse().unwrap(),
            expected_rewritten,
            "rewrite_listener",
        )
        .and_then(|_| counter.dec().unit_error());
        // TODO: internal metric of failed metrics should be 0

        let (_, rel_rx) = unbounded_channel();

        let everything = async {
            futures::try_join!(
                counter.zero_after(3),
                sender,
                main_listener,
                unmatched_listener,
                rewrite_listener,
                scheduler.main(sched_rx, rel_rx).map_err(|_| ()),
            )
        };

        runtime.block_on(everything).unwrap();
    }
}
