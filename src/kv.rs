//! Replicated key-value state machine with per-client sessions, so retried
//! commands are applied at most once.

use std::collections::BTreeMap;

/// Key type.
pub type Key = u8;
/// Value type (`0` means absent).
pub type Value = u64;

/// Client operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Op {
    /// Write a value.
    Put(Key, Value),
    /// Read a value.
    Get(Key),
    /// Leader no-op appended at the start of each term.
    Noop,
}

/// A command as stored in the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Command {
    /// Client id (0 for the leader no-op).
    pub client: u64,
    /// Per-client sequence number, starting at 1.
    pub seq: u64,
    /// Operation.
    pub op: Op,
}

/// Result of an applied command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Output {
    /// Put/Noop acknowledged.
    Ok,
    /// Get result.
    Value(Value),
}

/// The state machine. Cloned wholesale into snapshots.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Kv {
    data: BTreeMap<Key, Value>,
    /// client -> (last applied seq, its output)
    sessions: BTreeMap<u64, (u64, Output)>,
}

impl Kv {
    /// Apply one committed command. Duplicates (seq ≤ last) are not re-executed.
    pub fn apply(&mut self, c: &Command) -> Output {
        if c.client != 0 {
            if let Some((last, out)) = self.sessions.get(&c.client) {
                if c.seq <= *last {
                    return *out;
                }
            }
        }
        let out = match c.op {
            Op::Put(k, v) => {
                self.data.insert(k, v);
                Output::Ok
            }
            Op::Get(k) => Output::Value(self.get(k)),
            Op::Noop => Output::Ok,
        };
        if c.client != 0 {
            self.sessions.insert(c.client, (c.seq, out));
        }
        out
    }

    /// Local read (no consensus). Used only by the deliberately buggy mode.
    pub fn get(&self, k: Key) -> Value {
        self.data.get(&k).copied().unwrap_or(0)
    }

    /// Output of `(client, seq)` if it has been applied.
    pub fn result_of(&self, client: u64, seq: u64) -> Option<Output> {
        match self.sessions.get(&client) {
            Some((s, out)) if *s == seq => Some(*out),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retries_apply_once() {
        let mut kv = Kv::default();
        let put = Command {
            client: 1,
            seq: 1,
            op: Op::Put(0, 5),
        };
        assert_eq!(kv.apply(&put), Output::Ok);
        let put2 = Command {
            client: 2,
            seq: 1,
            op: Op::Put(0, 6),
        };
        kv.apply(&put2);
        // Retry of client 1's first put must not overwrite client 2's write.
        kv.apply(&put);
        assert_eq!(kv.get(0), 6);
        let get = Command {
            client: 1,
            seq: 2,
            op: Op::Get(0),
        };
        assert_eq!(kv.apply(&get), Output::Value(6));
        assert_eq!(kv.result_of(1, 2), Some(Output::Value(6)));
        assert_eq!(kv.result_of(1, 1), None);
    }
}
