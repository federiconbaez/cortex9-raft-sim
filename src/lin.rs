//! Porcupine-style linearizability checker (Wing & Gong search with Lowe's
//! memoisation of `(linearized set, state)`), specialised to a
//! single-register model. Histories are partitioned by key first
//! (P-compositionality), so each search is small.

use std::collections::HashSet;

/// Register operation as observed by a client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegOp {
    /// Write `v`.
    Write(u64),
    /// Read that returned `v`.
    Read(u64),
}

/// One operation in a history. `ret == None` means the client never saw a
/// response: the operation may take effect at any point after `call`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Event {
    /// Invocation time.
    pub call: u64,
    /// Response time.
    pub ret: Option<u64>,
    /// Operation.
    pub op: RegOp,
}

/// Checker verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// A valid linearization exists.
    Ok,
    /// No linearization exists.
    Illegal,
    /// Search budget exhausted.
    Unknown,
}

#[derive(Clone, Copy)]
struct Node {
    op: usize,
    is_call: bool,
    /// For a call node: index of its return node.
    partner: usize,
    prev: usize,
    next: usize,
}

const NIL: usize = usize::MAX;

fn step(state: u64, op: RegOp) -> Option<u64> {
    match op {
        RegOp::Write(v) => Some(v),
        RegOp::Read(v) => (v == state).then_some(state),
    }
}

/// Check a register history starting from `initial`. `budget` bounds the
/// number of search steps.
pub fn check(history: &[Event], initial: u64, budget: u64) -> Verdict {
    // Reads without a response carry no information.
    let ops: Vec<Event> = history
        .iter()
        .copied()
        .filter(|e| e.ret.is_some() || matches!(e.op, RegOp::Write(_)))
        .collect();
    let n = ops.len();
    if n == 0 {
        return Verdict::Ok;
    }
    if n > 4096 {
        return Verdict::Unknown;
    }
    // Events: (time, kind) with calls before returns at equal time
    // (treating such pairs as concurrent is the permissive choice).
    let mut evs: Vec<(u64, u8, usize)> = Vec::with_capacity(2 * n);
    for (i, e) in ops.iter().enumerate() {
        evs.push((e.call, 0, i));
        evs.push((e.ret.unwrap_or(u64::MAX), 1, i));
    }
    evs.sort();
    // nodes[0] is the head sentinel; nodes[k+1] is evs[k].
    let mut nodes = vec![
        Node {
            op: NIL,
            is_call: false,
            partner: NIL,
            prev: NIL,
            next: NIL
        };
        2 * n + 1
    ];
    let mut ret_pos = vec![0usize; n];
    for (k, &(_, kind, op)) in evs.iter().enumerate() {
        nodes[k + 1] = Node {
            op,
            is_call: kind == 0,
            partner: NIL,
            prev: k,
            next: if k + 2 <= 2 * n { k + 2 } else { NIL },
        };
        if kind == 1 {
            ret_pos[op] = k + 1;
        }
    }
    nodes[0].next = 1;
    for k in 1..=2 * n {
        if nodes[k].is_call {
            nodes[k].partner = ret_pos[nodes[k].op];
        }
    }

    fn unlink(nodes: &mut [Node], i: usize) {
        let (p, nx) = (nodes[i].prev, nodes[i].next);
        nodes[p].next = nx;
        if nx != NIL {
            nodes[nx].prev = p;
        }
    }
    fn relink(nodes: &mut [Node], i: usize) {
        let (p, nx) = (nodes[i].prev, nodes[i].next);
        nodes[p].next = i;
        if nx != NIL {
            nodes[nx].prev = i;
        }
    }

    let words = n.div_ceil(64);
    let mut linearized = vec![0u64; words];
    let mut cache: HashSet<(Vec<u64>, u64)> = HashSet::new();
    let mut stack: Vec<(usize, u64)> = Vec::new();
    let mut state = initial;
    let mut entry = nodes[0].next;
    let mut steps = 0u64;
    loop {
        if nodes[0].next == NIL {
            return Verdict::Ok;
        }
        steps += 1;
        if steps > budget {
            return Verdict::Unknown;
        }
        if entry == NIL {
            // Unreachable: every call is followed by its return.
            return Verdict::Illegal;
        }
        let node = nodes[entry];
        if node.is_call {
            if let Some(next_state) = step(state, ops[node.op].op) {
                linearized[node.op / 64] |= 1 << (node.op % 64);
                if cache.insert((linearized.clone(), next_state)) {
                    stack.push((entry, state));
                    state = next_state;
                    unlink(&mut nodes, node.partner);
                    unlink(&mut nodes, entry);
                    entry = nodes[0].next;
                    continue;
                }
                linearized[node.op / 64] &= !(1 << (node.op % 64));
            }
            entry = node.next;
        } else {
            // A return with its call still pending: backtrack.
            let Some((call, prev_state)) = stack.pop() else {
                return Verdict::Illegal;
            };
            let op = nodes[call].op;
            linearized[op / 64] &= !(1 << (op % 64));
            state = prev_state;
            relink(&mut nodes, call);
            let partner = nodes[call].partner;
            relink(&mut nodes, partner);
            entry = nodes[call].next;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use RegOp::*;

    fn ev(call: u64, ret: u64, op: RegOp) -> Event {
        Event {
            call,
            ret: Some(ret),
            op,
        }
    }

    #[test]
    fn sequential_ok() {
        let h = [
            ev(0, 1, Write(1)),
            ev(2, 3, Read(1)),
            ev(4, 5, Write(2)),
            ev(6, 7, Read(2)),
        ];
        assert_eq!(check(&h, 0, 1_000_000), Verdict::Ok);
    }

    #[test]
    fn stale_read_after_completed_write_is_illegal() {
        let h = [ev(0, 1, Write(1)), ev(2, 3, Read(0))];
        assert_eq!(check(&h, 0, 1_000_000), Verdict::Illegal);
    }

    #[test]
    fn concurrent_write_allows_either_read() {
        let h = [ev(0, 10, Write(1)), ev(1, 2, Read(0)), ev(3, 4, Read(1))];
        assert_eq!(check(&h, 0, 1_000_000), Verdict::Ok);
        // ...but not going back once the new value was observed.
        let h = [ev(0, 10, Write(1)), ev(1, 2, Read(1)), ev(3, 4, Read(0))];
        assert_eq!(check(&h, 0, 1_000_000), Verdict::Illegal);
    }

    #[test]
    fn porcupine_style_two_writers() {
        // Classic example: two concurrent writes, readers must agree on an order.
        let h = [
            ev(0, 10, Write(1)),
            ev(0, 10, Write(2)),
            ev(1, 9, Read(2)),
            ev(2, 8, Read(1)),
            ev(11, 12, Read(1)),
        ];
        assert_eq!(check(&h, 0, 1_000_000), Verdict::Ok);
        let h = [
            ev(0, 10, Write(1)),
            ev(0, 10, Write(2)),
            ev(11, 12, Read(1)),
            ev(13, 14, Read(2)),
        ];
        assert_eq!(check(&h, 0, 1_000_000), Verdict::Illegal);
    }

    #[test]
    fn pending_write_may_land_late() {
        let h = [
            Event {
                call: 0,
                ret: None,
                op: Write(7),
            },
            ev(1, 2, Read(0)),
            ev(5, 6, Read(7)),
        ];
        assert_eq!(check(&h, 0, 1_000_000), Verdict::Ok);
        // A pending read is ignored entirely.
        let h = [Event {
            call: 0,
            ret: None,
            op: Read(99),
        }];
        assert_eq!(check(&h, 0, 1_000_000), Verdict::Ok);
    }

    #[test]
    fn value_from_nowhere_is_illegal() {
        let h = [ev(0, 1, Write(1)), ev(0, 1, Read(5))];
        assert_eq!(check(&h, 0, 1_000_000), Verdict::Illegal);
    }

    #[test]
    fn budget_is_respected() {
        let h: Vec<Event> = (0..200)
            .map(|i| {
                ev(
                    0,
                    1000,
                    if i % 2 == 0 {
                        Write(i)
                    } else {
                        Read(1_000_000)
                    },
                )
            })
            .collect();
        assert_eq!(check(&h, 0, 100), Verdict::Unknown);
    }

    /// Brute force over all permutations for tiny histories.
    fn brute(h: &[Event], init: u64) -> bool {
        let ops: Vec<Event> = h
            .iter()
            .copied()
            .filter(|e| e.ret.is_some() || matches!(e.op, Write(_)))
            .collect();
        fn go(ops: &[Event], used: &mut Vec<bool>, state: u64, done: usize) -> bool {
            if done == ops.len() {
                return true;
            }
            for i in 0..ops.len() {
                if used[i] {
                    continue;
                }
                // i can go next only if no unused op returned before i was called.
                let blocked = (0..ops.len())
                    .any(|j| !used[j] && j != i && ops[j].ret.is_some_and(|r| r < ops[i].call));
                if blocked {
                    continue;
                }
                if let Some(s) = step(state, ops[i].op) {
                    used[i] = true;
                    if go(ops, used, s, done + 1) {
                        used[i] = false;
                        return true;
                    }
                    used[i] = false;
                }
            }
            false
        }
        go(&ops, &mut vec![false; ops.len()], init, 0)
    }

    #[test]
    fn agrees_with_brute_force_on_random_histories() {
        let mut rng = crate::rng::Rng::new(99);
        let mut illegal = 0;
        for _ in 0..3000 {
            let n = rng.range(1, 7) as usize;
            let h: Vec<Event> = (0..n)
                .map(|_| {
                    let call = rng.range(0, 10);
                    let ret = if rng.chance(0.1) {
                        None
                    } else {
                        Some(call + rng.range(0, 6))
                    };
                    let op = if rng.chance(0.5) {
                        Write(rng.range(0, 3))
                    } else {
                        Read(rng.range(0, 3))
                    };
                    Event { call, ret, op }
                })
                .collect();
            let want = brute(&h, 0);
            let got = check(&h, 0, 1_000_000);
            assert_eq!(got == Verdict::Ok, want, "{h:?}");
            if !want {
                illegal += 1;
            }
        }
        assert!(illegal > 100, "generator too easy: {illegal}");
    }
}
