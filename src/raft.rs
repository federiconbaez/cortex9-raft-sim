//! Raft node as a pure state machine: no threads, no clock, no I/O.
//! The simulator calls [`Node::tick`] and [`Node::handle`] and routes the
//! messages they return.
//!
//! Implements leader election, log replication with fast conflict backoff,
//! commitment restricted to current-term entries, leader no-op on election,
//! log compaction into snapshots and `InstallSnapshot`.

use crate::kv::{Command, Kv, Op, Output};
use crate::rng::Rng;

/// Node index in the cluster.
pub type NodeId = usize;
/// Logical time in ticks (1 tick ≈ 1 ms of the simulated world).
pub type Time = u64;

/// Timing parameters.
#[derive(Debug, Clone, Copy)]
pub struct Timing {
    /// Leader heartbeat period.
    pub heartbeat: Time,
    /// Minimum election timeout.
    pub election_min: Time,
    /// Maximum election timeout (exclusive).
    pub election_max: Time,
}

impl Default for Timing {
    fn default() -> Self {
        Timing {
            heartbeat: 50,
            election_min: 150,
            election_max: 300,
        }
    }
}

/// Deliberately injectable bugs, used to show the checkers catch them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Bugs {
    /// Leaders answer reads from local state without going through the log.
    pub stale_reads: bool,
    /// `voted_for` is not persisted across restarts.
    pub forget_vote: bool,
    /// Commit and election quorum is `n / 2` instead of `n / 2 + 1`.
    pub short_quorum: bool,
}

/// Log entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Entry {
    /// Term in which the leader created it.
    pub term: u64,
    /// Command.
    pub cmd: Command,
}

/// Snapshot of the state machine up to `index`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Last included index.
    pub index: u64,
    /// Term of the last included entry.
    pub term: u64,
    /// State machine contents.
    pub kv: Kv,
}

/// RPCs (fields follow the Raft paper, Figure 2).
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Msg {
    /// Candidate asks for a vote.
    RequestVote {
        term: u64,
        last_log_index: u64,
        last_log_term: u64,
    },
    /// Vote reply.
    RequestVoteResp { term: u64, granted: bool },
    /// Replication / heartbeat.
    AppendEntries {
        term: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
    },
    /// Replication reply. On failure `index` is where the leader should retry.
    AppendEntriesResp {
        term: u64,
        success: bool,
        index: u64,
    },
    /// Snapshot transfer (whole snapshot in one message).
    InstallSnapshot { term: u64, snapshot: Snapshot },
    /// Snapshot reply.
    InstallSnapshotResp { term: u64, index: u64 },
}

impl Msg {
    /// Term carried by the message.
    pub fn term(&self) -> u64 {
        match self {
            Msg::RequestVote { term, .. }
            | Msg::RequestVoteResp { term, .. }
            | Msg::AppendEntries { term, .. }
            | Msg::AppendEntriesResp { term, .. }
            | Msg::InstallSnapshot { term, .. }
            | Msg::InstallSnapshotResp { term, .. } => *term,
        }
    }
}

/// Role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Follower.
    Follower,
    /// Candidate.
    Candidate,
    /// Leader.
    Leader,
}

/// State that survives a crash (assumed fsynced before any message leaves).
#[derive(Debug, Clone)]
pub struct Persistent {
    /// Current term.
    pub current_term: u64,
    /// Vote in the current term.
    pub voted_for: Option<NodeId>,
    /// Latest snapshot.
    pub snapshot: Snapshot,
    /// Entries after `snapshot.index`.
    pub log: Vec<Entry>,
}

/// An entry applied to the state machine (reported to the simulator).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Applied {
    /// Log index.
    pub index: u64,
    /// Entry term.
    pub term: u64,
    /// Command.
    pub cmd: Command,
    /// Output.
    pub output: Output,
}

/// One Raft server.
#[derive(Debug, Clone)]
pub struct Node {
    /// Own id.
    pub id: NodeId,
    n: usize,
    timing: Timing,
    bugs: Bugs,
    /// Snapshot when the log exceeds this many applied entries.
    pub snapshot_every: u64,
    /// Persistent state.
    pub p: Persistent,
    /// Role.
    pub role: Role,
    /// Highest index known committed.
    pub commit_index: u64,
    /// Highest index applied.
    pub last_applied: u64,
    /// State machine.
    pub kv: Kv,
    /// Last known leader.
    pub leader_hint: Option<NodeId>,
    election_deadline: Time,
    next_heartbeat: Time,
    votes: Vec<bool>,
    next_index: Vec<u64>,
    match_index: Vec<u64>,
    rng: Rng,
    /// Applied entries not yet drained by the simulator.
    pub applied: Vec<Applied>,
    /// Number of snapshots taken.
    pub snapshots_taken: u64,
    /// Number of snapshots installed from a leader.
    pub snapshots_installed: u64,
}

/// Outgoing messages.
pub type Out = Vec<(NodeId, Msg)>;

impl Node {
    /// Fresh node.
    pub fn new(id: NodeId, n: usize, timing: Timing, bugs: Bugs, rng: Rng) -> Self {
        let p = Persistent {
            current_term: 0,
            voted_for: None,
            snapshot: Snapshot {
                index: 0,
                term: 0,
                kv: Kv::default(),
            },
            log: Vec::new(),
        };
        let mut node = Node {
            id,
            n,
            timing,
            bugs,
            snapshot_every: 32,
            p,
            role: Role::Follower,
            commit_index: 0,
            last_applied: 0,
            kv: Kv::default(),
            leader_hint: None,
            election_deadline: 0,
            next_heartbeat: 0,
            votes: vec![false; n],
            next_index: vec![1; n],
            match_index: vec![0; n],
            rng,
            applied: Vec::new(),
            snapshots_taken: 0,
            snapshots_installed: 0,
        };
        node.reset_election(0);
        node
    }

    /// Rebuild a node after a crash from its persistent state only.
    pub fn restart(&self, now: Time) -> Node {
        let mut p = self.p.clone();
        if self.bugs.forget_vote {
            p.voted_for = None;
        }
        let mut node = Node::new(self.id, self.n, self.timing, self.bugs, self.rng.clone());
        node.snapshot_every = self.snapshot_every;
        node.kv = p.snapshot.kv.clone();
        node.commit_index = p.snapshot.index;
        node.last_applied = p.snapshot.index;
        node.p = p;
        node.rng.next_u64();
        node.reset_election(now);
        node
    }

    fn majority(&self) -> usize {
        if self.bugs.short_quorum {
            (self.n / 2).max(1)
        } else {
            self.n / 2 + 1
        }
    }

    /// Index of the last log entry (or of the snapshot).
    pub fn last_index(&self) -> u64 {
        self.p.snapshot.index + self.p.log.len() as u64
    }

    /// Term at `index`; `None` if compacted away or beyond the log.
    pub fn term_at(&self, index: u64) -> Option<u64> {
        let s = self.p.snapshot.index;
        if index == s {
            Some(self.p.snapshot.term)
        } else if index < s || index > self.last_index() {
            None
        } else {
            Some(self.p.log[(index - s - 1) as usize].term)
        }
    }

    /// Entry at `index` if still in the log.
    pub fn entry_at(&self, index: u64) -> Option<&Entry> {
        let s = self.p.snapshot.index;
        if index <= s || index > self.last_index() {
            None
        } else {
            Some(&self.p.log[(index - s - 1) as usize])
        }
    }

    fn last_term(&self) -> u64 {
        self.term_at(self.last_index()).unwrap_or(0)
    }

    fn reset_election(&mut self, now: Time) {
        self.election_deadline = now
            + self
                .rng
                .range(self.timing.election_min, self.timing.election_max);
    }

    fn become_follower(&mut self, now: Time, term: u64) {
        if term > self.p.current_term {
            self.p.current_term = term;
            self.p.voted_for = None;
        }
        if self.role != Role::Follower {
            self.reset_election(now);
        }
        self.role = Role::Follower;
    }

    /// Advance timers.
    pub fn tick(&mut self, now: Time) -> Out {
        let mut out = Vec::new();
        match self.role {
            Role::Leader => {
                if now >= self.next_heartbeat {
                    self.broadcast_append(now, &mut out);
                }
            }
            _ => {
                if now >= self.election_deadline {
                    self.start_election(now, &mut out);
                }
            }
        }
        out
    }

    fn start_election(&mut self, now: Time, out: &mut Out) {
        self.role = Role::Candidate;
        self.p.current_term += 1;
        self.p.voted_for = Some(self.id);
        self.leader_hint = None;
        self.votes = vec![false; self.n];
        self.votes[self.id] = true;
        self.reset_election(now);
        if self.majority() == 1 {
            self.become_leader(now, out);
            return;
        }
        let msg = Msg::RequestVote {
            term: self.p.current_term,
            last_log_index: self.last_index(),
            last_log_term: self.last_term(),
        };
        for peer in 0..self.n {
            if peer != self.id {
                out.push((peer, msg.clone()));
            }
        }
    }

    fn become_leader(&mut self, now: Time, out: &mut Out) {
        self.role = Role::Leader;
        self.leader_hint = Some(self.id);
        let next = self.last_index() + 1;
        self.next_index = vec![next; self.n];
        self.match_index = vec![0; self.n];
        let term = self.p.current_term;
        self.p.log.push(Entry {
            term,
            cmd: Command {
                client: 0,
                seq: 0,
                op: Op::Noop,
            },
        });
        self.match_index[self.id] = self.last_index();
        self.advance_commit();
        self.broadcast_append(now, out);
    }

    /// Propose a client command. Returns the log index on success.
    pub fn propose(
        &mut self,
        now: Time,
        cmd: Command,
        out: &mut Out,
    ) -> Result<u64, Option<NodeId>> {
        if self.role != Role::Leader {
            return Err(self.leader_hint);
        }
        self.p.log.push(Entry {
            term: self.p.current_term,
            cmd,
        });
        let idx = self.last_index();
        self.match_index[self.id] = idx;
        self.advance_commit();
        self.broadcast_append(now, out);
        Ok(idx)
    }

    /// Buggy local read (only with `Bugs::stale_reads`).
    pub fn local_read(&self, key: crate::kv::Key) -> Option<u64> {
        (self.bugs.stale_reads && self.role == Role::Leader).then(|| self.kv.get(key))
    }

    fn broadcast_append(&mut self, now: Time, out: &mut Out) {
        self.next_heartbeat = now + self.timing.heartbeat;
        for peer in 0..self.n {
            if peer != self.id {
                out.push((peer, self.append_for(peer)));
            }
        }
    }

    fn append_for(&self, peer: NodeId) -> Msg {
        let next = self.next_index[peer].min(self.last_index() + 1);
        if next <= self.p.snapshot.index {
            return Msg::InstallSnapshot {
                term: self.p.current_term,
                snapshot: self.p.snapshot.clone(),
            };
        }
        let prev = next - 1;
        let start = (next - self.p.snapshot.index - 1) as usize;
        let end = (start + 64).min(self.p.log.len());
        Msg::AppendEntries {
            term: self.p.current_term,
            prev_log_index: prev,
            prev_log_term: self.term_at(prev).expect("prev within log"),
            entries: self.p.log[start..end].to_vec(),
            leader_commit: self.commit_index,
        }
    }

    /// Handle one message.
    pub fn handle(&mut self, now: Time, from: NodeId, msg: Msg) -> Out {
        let mut out = Vec::new();
        if msg.term() > self.p.current_term {
            self.become_follower(now, msg.term());
        }
        let term = self.p.current_term;
        match msg {
            Msg::RequestVote {
                term: t,
                last_log_index,
                last_log_term,
            } => {
                let up_to_date = last_log_term > self.last_term()
                    || (last_log_term == self.last_term() && last_log_index >= self.last_index());
                let granted = t == term && up_to_date && self.p.voted_for.is_none_or_eq(from);
                if granted {
                    self.p.voted_for = Some(from);
                    self.reset_election(now);
                }
                out.push((from, Msg::RequestVoteResp { term, granted }));
            }
            Msg::RequestVoteResp { term: t, granted } => {
                if self.role == Role::Candidate && t == term && granted {
                    self.votes[from] = true;
                    if self.votes.iter().filter(|v| **v).count() >= self.majority() {
                        self.become_leader(now, &mut out);
                    }
                }
            }
            Msg::AppendEntries {
                term: t,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                if t < term {
                    out.push((
                        from,
                        Msg::AppendEntriesResp {
                            term,
                            success: false,
                            index: 0,
                        },
                    ));
                } else {
                    if self.role != Role::Follower {
                        self.role = Role::Follower;
                    }
                    self.leader_hint = Some(from);
                    self.reset_election(now);
                    let reply =
                        self.accept_entries(prev_log_index, prev_log_term, entries, leader_commit);
                    out.push((from, reply));
                }
            }
            Msg::AppendEntriesResp {
                term: t,
                success,
                index,
            } => {
                if self.role == Role::Leader && t == term {
                    if success {
                        if index > self.match_index[from] {
                            self.match_index[from] = index;
                        }
                        self.next_index[from] =
                            self.next_index[from].max(self.match_index[from] + 1);
                        self.advance_commit();
                        if self.next_index[from] <= self.last_index() {
                            out.push((from, self.append_for(from)));
                        }
                    } else if index > 0 {
                        let next = index
                            .max(self.match_index[from] + 1)
                            .min(self.next_index[from]);
                        if next < self.next_index[from] {
                            self.next_index[from] = next;
                            out.push((from, self.append_for(from)));
                        }
                    }
                }
            }
            Msg::InstallSnapshot { term: t, snapshot } => {
                if t == term {
                    self.role = Role::Follower;
                    self.leader_hint = Some(from);
                    self.reset_election(now);
                    let idx = snapshot.index;
                    self.install_snapshot(snapshot);
                    out.push((from, Msg::InstallSnapshotResp { term, index: idx }));
                } else {
                    out.push((from, Msg::InstallSnapshotResp { term, index: 0 }));
                }
            }
            Msg::InstallSnapshotResp { term: t, index } => {
                if self.role == Role::Leader && t == term && index > 0 {
                    self.match_index[from] = self.match_index[from].max(index);
                    self.next_index[from] = self.next_index[from].max(self.match_index[from] + 1);
                    self.advance_commit();
                }
            }
        }
        self.apply_committed();
        out
    }

    fn accept_entries(
        &mut self,
        mut prev: u64,
        mut prev_term: u64,
        mut entries: Vec<Entry>,
        leader_commit: u64,
    ) -> Msg {
        let term = self.p.current_term;
        let snap = self.p.snapshot.index;
        if prev < snap {
            // Prefix already compacted (and therefore committed) here.
            let skip = (snap - prev) as usize;
            if entries.len() <= skip {
                return Msg::AppendEntriesResp {
                    term,
                    success: true,
                    index: snap,
                };
            }
            entries.drain(..skip);
            prev = snap;
            prev_term = self.p.snapshot.term;
        }
        if prev > self.last_index() {
            return Msg::AppendEntriesResp {
                term,
                success: false,
                index: self.last_index() + 1,
            };
        }
        let my_prev_term = self.term_at(prev).expect("prev >= snapshot index");
        if my_prev_term != prev_term {
            // Back up to the first index of the conflicting term.
            let mut i = prev;
            while i > snap + 1 && self.term_at(i - 1) == Some(my_prev_term) {
                i -= 1;
            }
            return Msg::AppendEntriesResp {
                term,
                success: false,
                index: i.max(snap + 1),
            };
        }
        let n = entries.len() as u64;
        for (k, e) in entries.into_iter().enumerate() {
            let idx = prev + 1 + k as u64;
            match self.term_at(idx) {
                Some(t) if t == e.term => {}
                Some(_) => {
                    self.p.log.truncate((idx - snap - 1) as usize);
                    self.p.log.push(e);
                }
                None => self.p.log.push(e),
            }
        }
        let last_new = prev + n;
        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min(last_new).max(self.commit_index);
        }
        Msg::AppendEntriesResp {
            term,
            success: true,
            index: last_new,
        }
    }

    fn install_snapshot(&mut self, s: Snapshot) {
        if s.index <= self.commit_index {
            return;
        }
        if self.term_at(s.index) == Some(s.term) {
            let keep_from = (s.index - self.p.snapshot.index) as usize;
            self.p.log.drain(..keep_from);
        } else {
            self.p.log.clear();
        }
        self.kv = s.kv.clone();
        self.commit_index = s.index;
        self.last_applied = s.index;
        self.p.snapshot = s;
        self.snapshots_installed += 1;
    }

    fn advance_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let mut n = self.last_index();
        while n > self.commit_index {
            let t = self.term_at(n);
            // Only entries of the current term are committed by counting (§5.4.2).
            if t == Some(self.p.current_term) {
                let count = (0..self.n)
                    .filter(|&p| p == self.id || self.match_index[p] >= n)
                    .count();
                if count >= self.majority() {
                    self.commit_index = n;
                    break;
                }
            }
            n -= 1;
        }
        self.apply_committed();
    }

    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let e = *self
                .entry_at(self.last_applied)
                .expect("committed entry present");
            let output = self.kv.apply(&e.cmd);
            self.applied.push(Applied {
                index: self.last_applied,
                term: e.term,
                cmd: e.cmd,
                output,
            });
        }
        if self.last_applied - self.p.snapshot.index >= self.snapshot_every {
            let idx = self.last_applied;
            let term = self.term_at(idx).expect("applied entry present");
            let drop = (idx - self.p.snapshot.index) as usize;
            self.p.log.drain(..drop);
            self.p.snapshot = Snapshot {
                index: idx,
                term,
                kv: self.kv.clone(),
            };
            self.snapshots_taken += 1;
        }
    }
}

trait VoteExt {
    fn is_none_or_eq(&self, id: NodeId) -> bool;
}

impl VoteExt for Option<NodeId> {
    fn is_none_or_eq(&self, id: NodeId) -> bool {
        self.is_none() || *self == Some(id)
    }
}
