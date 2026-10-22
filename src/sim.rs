//! Deterministic simulation: a seeded network that delays, reorders, drops,
//! duplicates and partitions messages, plus node crashes and restarts.
//! Every run is a pure function of its [`Config`] and seed.

use crate::kv::{Command, Key, Op, Output};
use crate::lin::{self, Event, RegOp, Verdict};
use crate::raft::{Bugs, Msg, Node, NodeId, Role, Time, Timing};
use crate::rng::Rng;
use std::collections::HashMap;

/// Simulation parameters.
#[derive(Debug, Clone)]
pub struct Config {
    /// Cluster size.
    pub nodes: usize,
    /// Concurrent clients (each issues one operation at a time).
    pub clients: usize,
    /// Number of distinct keys.
    pub keys: u8,
    /// Ticks with faults enabled.
    pub chaos_ticks: Time,
    /// Ticks with faults disabled at the end (heal, then let ops finish).
    pub settle_ticks: Time,
    /// Message drop probability.
    pub drop: f64,
    /// Message duplication probability.
    pub duplicate: f64,
    /// One-way delay range in ticks `[min, max)`; different delays reorder.
    pub delay: (Time, Time),
    /// Per-tick probability of starting a partition when none is active.
    pub partition_rate: f64,
    /// Partition duration range.
    pub partition_len: (Time, Time),
    /// Per-tick, per-node crash probability.
    pub crash_rate: f64,
    /// Downtime range for a crashed node.
    pub down_len: (Time, Time),
    /// Client retry timeout.
    pub client_timeout: Time,
    /// Pause between a client's operations.
    pub think: (Time, Time),
    /// Entries applied between snapshots.
    pub snapshot_every: u64,
    /// Raft timing.
    pub timing: Timing,
    /// Injected bugs.
    pub bugs: Bugs,
    /// Linearizability search budget per key.
    pub lin_budget: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            nodes: 5,
            clients: 4,
            keys: 3,
            chaos_ticks: 6_000,
            settle_ticks: 4_000,
            drop: 0.05,
            duplicate: 0.02,
            delay: (1, 25),
            partition_rate: 0.002,
            partition_len: (100, 1_200),
            crash_rate: 0.0002,
            down_len: (50, 800),
            client_timeout: 400,
            think: (0, 40),
            snapshot_every: 16,
            timing: Timing::default(),
            bugs: Bugs::default(),
            lin_budget: 2_000_000,
        }
    }
}

/// A recorded client operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistOp {
    /// Client id.
    pub client: u64,
    /// Key.
    pub key: Key,
    /// Invocation time.
    pub call: Time,
    /// Response time (`None` if never answered).
    pub ret: Option<Time>,
    /// Written value, or `None` for reads.
    pub write: Option<u64>,
    /// Read result.
    pub read: Option<u64>,
}

/// Outcome of one seed.
#[derive(Debug, Clone, Default)]
pub struct Report {
    /// Seed.
    pub seed: u64,
    /// Safety violations found by the online invariant checks.
    pub invariant_violations: Vec<String>,
    /// Keys whose history is not linearizable.
    pub non_linearizable_keys: Vec<Key>,
    /// Keys where the checker ran out of budget.
    pub unknown_keys: Vec<Key>,
    /// Operations still unanswered after the settle phase.
    pub unfinished: usize,
    /// Client operation history.
    pub history: Vec<HistOp>,
    /// Counters.
    pub stats: Stats,
}

/// Counters for one run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Stats {
    /// Messages sent.
    pub sent: u64,
    /// Messages dropped (random loss, partitions, dead receivers).
    pub dropped: u64,
    /// Messages duplicated.
    pub duplicated: u64,
    /// Leader elections won.
    pub elections: u64,
    /// Partitions started.
    pub partitions: u64,
    /// Crashes.
    pub crashes: u64,
    /// Snapshots taken.
    pub snapshots_taken: u64,
    /// Snapshots installed via InstallSnapshot.
    pub snapshots_installed: u64,
    /// Completed client operations.
    pub completed_ops: u64,
    /// Highest committed index.
    pub commit_index: u64,
}

impl Report {
    /// True when no check failed.
    pub fn ok(&self) -> bool {
        self.invariant_violations.is_empty()
            && self.non_linearizable_keys.is_empty()
            && self.unfinished == 0
    }
}

struct InFlight {
    at: Time,
    seq: u64,
    from: NodeId,
    to: NodeId,
    msg: Msg,
}

struct Pending {
    hist: usize,
    cmd: Command,
    sent_to: Option<NodeId>,
    deadline: Time,
}

struct Client {
    id: u64,
    seq: u64,
    pending: Option<Pending>,
    next_op_at: Time,
    target: NodeId,
    rng: Rng,
}

/// Run one seed. An internal assertion failure inside a node (only
/// possible when safety is already broken) is reported as a violation.
pub fn run(cfg: &Config, seed: u64) -> Report {
    let cfg2 = cfg.clone();
    match std::panic::catch_unwind(move || Sim::new(cfg2, seed).run()) {
        Ok(r) => r,
        Err(e) => {
            let msg = e
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| e.downcast_ref::<String>().cloned())
                .unwrap_or_default();
            Report {
                seed,
                invariant_violations: vec![format!("node assertion failed: {msg}")],
                ..Report::default()
            }
        }
    }
}

struct Sim {
    cfg: Config,
    rng: Rng,
    now: Time,
    nodes: Vec<Node>,
    up: Vec<bool>,
    restart_at: Vec<Time>,
    side: Vec<bool>,
    partition_until: Option<Time>,
    net: Vec<InFlight>,
    msg_seq: u64,
    clients: Vec<Client>,
    history: Vec<HistOp>,
    leaders: HashMap<u64, NodeId>,
    committed: Vec<(u64, Command)>,
    was_leader: Vec<bool>,
    report: Report,
}

impl Sim {
    fn new(cfg: Config, seed: u64) -> Self {
        let mut rng = Rng::new(seed);
        let nodes = (0..cfg.nodes)
            .map(|i| {
                let mut n = Node::new(i, cfg.nodes, cfg.timing, cfg.bugs, rng.fork());
                n.snapshot_every = cfg.snapshot_every;
                n
            })
            .collect();
        let clients = (0..cfg.clients)
            .map(|i| Client {
                id: i as u64 + 1,
                seq: 0,
                pending: None,
                next_op_at: 0,
                target: i % cfg.nodes,
                rng: rng.fork(),
            })
            .collect();
        Sim {
            up: vec![true; cfg.nodes],
            restart_at: vec![0; cfg.nodes],
            side: vec![false; cfg.nodes],
            was_leader: vec![false; cfg.nodes],
            partition_until: None,
            net: Vec::new(),
            msg_seq: 0,
            clients,
            history: Vec::new(),
            leaders: HashMap::new(),
            committed: Vec::new(),
            nodes,
            report: Report {
                seed,
                ..Report::default()
            },
            now: 0,
            rng,
            cfg,
        }
    }

    fn chaos(&self) -> bool {
        self.now < self.cfg.chaos_ticks
    }

    fn violation(&mut self, s: String) {
        if self.report.invariant_violations.len() < 20 {
            self.report
                .invariant_violations
                .push(format!("t={} {}", self.now, s));
        }
    }

    fn send(&mut self, from: NodeId, out: Vec<(NodeId, Msg)>) {
        for (to, msg) in out {
            self.report.stats.sent += 1;
            let copies = if self.chaos() && self.rng.chance(self.cfg.duplicate) {
                self.report.stats.duplicated += 1;
                2
            } else {
                1
            };
            for _ in 0..copies {
                if self.chaos() && self.rng.chance(self.cfg.drop) {
                    self.report.stats.dropped += 1;
                    continue;
                }
                let at = self.now + self.rng.range(self.cfg.delay.0, self.cfg.delay.1);
                self.msg_seq += 1;
                self.net.push(InFlight {
                    at,
                    seq: self.msg_seq,
                    from,
                    to,
                    msg: msg.clone(),
                });
            }
        }
    }

    fn faults(&mut self) {
        if !self.chaos() {
            if self.partition_until.is_some() {
                self.partition_until = None;
                self.side = vec![false; self.cfg.nodes];
            }
            for i in 0..self.cfg.nodes {
                if !self.up[i] {
                    self.restart(i);
                }
            }
            return;
        }
        match self.partition_until {
            Some(t) if self.now >= t => {
                self.partition_until = None;
                self.side = vec![false; self.cfg.nodes];
            }
            None if self.rng.chance(self.cfg.partition_rate) => {
                for s in self.side.iter_mut() {
                    *s = self.rng.chance(0.5);
                }
                let len = self
                    .rng
                    .range(self.cfg.partition_len.0, self.cfg.partition_len.1);
                self.partition_until = Some(self.now + len);
                self.report.stats.partitions += 1;
            }
            _ => {}
        }
        for i in 0..self.cfg.nodes {
            if self.up[i] && self.rng.chance(self.cfg.crash_rate) {
                self.up[i] = false;
                self.was_leader[i] = false;
                self.restart_at[i] =
                    self.now + self.rng.range(self.cfg.down_len.0, self.cfg.down_len.1);
                self.report.stats.crashes += 1;
            } else if !self.up[i] && self.now >= self.restart_at[i] {
                self.restart(i);
            }
        }
    }

    fn restart(&mut self, i: NodeId) {
        let n = &self.nodes[i];
        self.report.stats.snapshots_taken += n.snapshots_taken;
        self.report.stats.snapshots_installed += n.snapshots_installed;
        self.nodes[i] = n.restart(self.now);
        self.up[i] = true;
    }

    fn deliver(&mut self) {
        let now = self.now;
        let mut due: Vec<InFlight> = Vec::new();
        let mut i = 0;
        while i < self.net.len() {
            if self.net[i].at <= now {
                due.push(self.net.swap_remove(i));
            } else {
                i += 1;
            }
        }
        due.sort_by_key(|m| (m.at, m.seq));
        for m in due {
            if !self.up[m.to] || self.side[m.from] != self.side[m.to] {
                self.report.stats.dropped += 1;
                continue;
            }
            let out = self.nodes[m.to].handle(now, m.from, m.msg);
            self.after_step(m.to);
            self.send(m.to, out);
        }
    }

    fn after_step(&mut self, i: NodeId) {
        let applied = std::mem::take(&mut self.nodes[i].applied);
        for a in applied {
            let idx = a.index as usize;
            if idx <= self.committed.len() {
                let (t, c) = self.committed[idx - 1];
                if t == u64::MAX {
                    self.committed[idx - 1] = (a.term, a.cmd);
                } else if t != a.term || c != a.cmd {
                    self.violation(format!(
                        "state machine safety: node {i} applied {:?}@{} at index {idx}, others applied {:?}@{}",
                        a.cmd, a.term, c, t
                    ));
                }
            } else if idx == self.committed.len() + 1 {
                self.committed.push((a.term, a.cmd));
            } else {
                // Applied past a gap: earlier entries came from a snapshot.
                // Fill from this node's view is impossible, so record only
                // when contiguous; the gap is checked when other nodes apply.
                while self.committed.len() + 1 < idx {
                    self.committed.push((
                        u64::MAX,
                        Command {
                            client: u64::MAX,
                            seq: 0,
                            op: Op::Noop,
                        },
                    ));
                }
                self.committed.push((a.term, a.cmd));
            }
        }
        let is_leader = self.nodes[i].role == Role::Leader;
        if is_leader && !self.was_leader[i] {
            self.report.stats.elections += 1;
            let term = self.nodes[i].p.current_term;
            if let Some(&other) = self.leaders.get(&term) {
                if other != i {
                    self.violation(format!(
                        "election safety: nodes {other} and {i} both leader in term {term}"
                    ));
                }
            } else {
                self.leaders.insert(term, i);
            }
            let node = &self.nodes[i];
            // Leader completeness: every committed entry is in the new leader's log.
            let snap = node.p.snapshot.index;
            let mut missing = None;
            for (k, (t, _)) in self.committed.iter().enumerate() {
                let idx = k as u64 + 1;
                if idx <= snap || *t == u64::MAX {
                    continue;
                }
                if node.term_at(idx) != Some(*t) {
                    missing = Some(idx);
                    break;
                }
            }
            if let Some(idx) = missing {
                self.violation(format!(
                    "leader completeness: leader {i} (term {term}) lacks committed index {idx}"
                ));
            }
        }
        self.was_leader[i] = is_leader;
        let node = &self.nodes[i];
        if node.p.snapshot.index > node.commit_index {
            self.violation(format!("node {i} snapshot beyond commit index"));
        }
    }

    fn clients(&mut self) {
        let now = self.now;
        for c in 0..self.clients.len() {
            // Completion.
            if let Some(p) = &self.clients[c].pending {
                let client = self.clients[c].id;
                if let Some(to) = p.sent_to {
                    if self.up[to] {
                        if let Some(out) = self.nodes[to].kv.result_of(client, p.cmd.seq) {
                            let h = &mut self.history[p.hist];
                            h.ret = Some(now);
                            if let Output::Value(v) = out {
                                h.read = Some(v);
                            }
                            self.report.stats.completed_ops += 1;
                            let cl = &mut self.clients[c];
                            cl.pending = None;
                            cl.next_op_at =
                                now + 1 + cl.rng.range(self.cfg.think.0, self.cfg.think.1);
                            continue;
                        }
                    }
                }
                if now >= p.deadline {
                    self.submit(c);
                }
                continue;
            }
            // New operation (only while the workload runs).
            if self.chaos() && now >= self.clients[c].next_op_at {
                let cl = &mut self.clients[c];
                cl.seq += 1;
                let key = cl.rng.range(0, self.cfg.keys as u64) as Key;
                let op = if cl.rng.chance(0.5) {
                    Op::Put(key, cl.id * 1_000_000 + cl.seq)
                } else {
                    Op::Get(key)
                };
                let cmd = Command {
                    client: cl.id,
                    seq: cl.seq,
                    op,
                };
                self.history.push(HistOp {
                    client: cl.id,
                    key,
                    call: now,
                    ret: None,
                    write: if let Op::Put(_, v) = op {
                        Some(v)
                    } else {
                        None
                    },
                    read: None,
                });
                cl.pending = Some(Pending {
                    hist: self.history.len() - 1,
                    cmd,
                    sent_to: None,
                    deadline: now,
                });
                self.submit(c);
            }
        }
    }

    fn submit(&mut self, c: usize) {
        let now = self.now;
        let target = self.clients[c].target;
        let p = self.clients[c].pending.as_ref().expect("pending");
        let (cmd, hist) = (p.cmd, p.hist);
        if !self.up[target] {
            let cl = &mut self.clients[c];
            cl.target = cl.rng.range(0, self.cfg.nodes as u64) as usize;
            cl.pending.as_mut().unwrap().deadline = now + 10;
            return;
        }
        if let Op::Get(k) = cmd.op {
            if let Some(v) = self.nodes[target].local_read(k) {
                let h = &mut self.history[hist];
                h.ret = Some(now);
                h.read = Some(v);
                self.report.stats.completed_ops += 1;
                let cl = &mut self.clients[c];
                cl.pending = None;
                cl.next_op_at = now + 1 + cl.rng.range(self.cfg.think.0, self.cfg.think.1);
                return;
            }
        }
        let mut out = Vec::new();
        let res = self.nodes[target].propose(now, cmd, &mut out);
        self.after_step(target);
        self.send(target, out);
        let n = self.cfg.nodes as u64;
        let timeout = self.cfg.client_timeout;
        let cl = &mut self.clients[c];
        let p = cl.pending.as_mut().unwrap();
        match res {
            Ok(_) => {
                p.sent_to = Some(target);
                p.deadline = now + timeout;
            }
            Err(hint) => {
                cl.target = hint
                    .filter(|h| *h != target)
                    .unwrap_or_else(|| cl.rng.range(0, n) as usize);
                p.deadline = now + 10;
            }
        }
    }

    fn run(mut self) -> Report {
        let end = self.cfg.chaos_ticks + self.cfg.settle_ticks;
        while self.now < end {
            self.faults();
            self.deliver();
            for i in 0..self.cfg.nodes {
                if self.up[i] {
                    let out = self.nodes[i].tick(self.now);
                    self.after_step(i);
                    self.send(i, out);
                }
            }
            self.clients();
            self.now += 1;
        }
        for n in &self.nodes {
            self.report.stats.snapshots_taken += n.snapshots_taken;
            self.report.stats.snapshots_installed += n.snapshots_installed;
            self.report.stats.commit_index = self.report.stats.commit_index.max(n.commit_index);
        }
        self.report.unfinished = self.clients.iter().filter(|c| c.pending.is_some()).count();
        self.report.history = std::mem::take(&mut self.history);
        check_history(&mut self.report, self.cfg.keys, self.cfg.lin_budget);
        self.report
    }
}

/// Run the linearizability checker over a report's history, per key.
pub fn check_history(report: &mut Report, keys: u8, budget: u64) {
    for key in 0..keys {
        let evs: Vec<Event> = report
            .history
            .iter()
            .filter(|h| h.key == key)
            .map(|h| Event {
                call: h.call,
                ret: h.ret,
                op: match h.write {
                    Some(v) => RegOp::Write(v),
                    None => RegOp::Read(h.read.unwrap_or(u64::MAX)),
                },
            })
            .collect();
        match lin::check(&evs, 0, budget) {
            Verdict::Ok => {}
            Verdict::Illegal => report.non_linearizable_keys.push(key),
            Verdict::Unknown => report.unknown_keys.push(key),
        }
    }
}
