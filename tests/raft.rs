//! Direct tests of the Raft node with hand-routed messages (no simulator).

use cortex9_raft_sim::kv::{Command, Op, Output};
use cortex9_raft_sim::raft::{Bugs, Msg, Node, NodeId, Role, Timing};
use cortex9_raft_sim::rng::Rng;

fn cluster(n: usize) -> Vec<Node> {
    let mut rng = Rng::new(1);
    (0..n)
        .map(|i| Node::new(i, n, Timing::default(), Bugs::default(), rng.fork()))
        .collect()
}

/// Deliver messages FIFO until quiet. Returns number delivered.
fn pump(nodes: &mut [Node], now: u64, mut q: Vec<(NodeId, NodeId, Msg)>, down: &[NodeId]) -> usize {
    let mut delivered = 0;
    while !q.is_empty() {
        let (from, to, m) = q.remove(0);
        if down.contains(&to) || down.contains(&from) {
            continue;
        }
        delivered += 1;
        for (dst, m2) in nodes[to].handle(now, from, m) {
            q.push((to, dst, m2));
        }
        assert!(delivered < 100_000, "message storm");
    }
    delivered
}

fn elect(nodes: &mut [Node], who: NodeId, now: u64, down: &[NodeId]) {
    let out = nodes[who].tick(now);
    let q = out.into_iter().map(|(to, m)| (who, to, m)).collect();
    pump(nodes, now, q, down);
}

fn cmd(client: u64, seq: u64, op: Op) -> Command {
    Command { client, seq, op }
}

#[test]
fn single_node_elects_itself_and_commits() {
    let mut nodes = cluster(1);
    let out = nodes[0].tick(1_000);
    assert!(out.is_empty());
    assert_eq!(nodes[0].role, Role::Leader);
    let mut out = Vec::new();
    let idx = nodes[0]
        .propose(1_000, cmd(1, 1, Op::Put(0, 9)), &mut out)
        .unwrap();
    assert_eq!(nodes[0].commit_index, idx);
    assert_eq!(nodes[0].kv.get(0), 9);
}

#[test]
fn three_nodes_elect_and_replicate() {
    let mut nodes = cluster(3);
    elect(&mut nodes, 0, 1_000, &[]);
    assert_eq!(nodes[0].role, Role::Leader);
    assert_eq!(nodes[1].role, Role::Follower);
    let mut out = Vec::new();
    nodes[0]
        .propose(1_001, cmd(1, 1, Op::Put(2, 7)), &mut out)
        .unwrap();
    pump(
        &mut nodes,
        1_001,
        out.into_iter().map(|(t, m)| (0, t, m)).collect(),
        &[],
    );
    // A heartbeat carries the new commit index to followers.
    let hb = nodes[0].tick(2_000);
    pump(
        &mut nodes,
        2_000,
        hb.into_iter().map(|(t, m)| (0, t, m)).collect(),
        &[],
    );
    for n in &nodes {
        assert_eq!(n.kv.get(2), 7, "node {}", n.id);
        assert_eq!(n.kv.result_of(1, 1), Some(Output::Ok));
    }
}

#[test]
fn propose_on_follower_returns_hint() {
    let mut nodes = cluster(3);
    elect(&mut nodes, 1, 1_000, &[]);
    let mut out = Vec::new();
    assert_eq!(
        nodes[2].propose(1_000, cmd(1, 1, Op::Get(0)), &mut out),
        Err(Some(1))
    );
}

#[test]
fn uncommitted_entries_of_a_deposed_leader_are_overwritten() {
    let mut nodes = cluster(3);
    elect(&mut nodes, 0, 1_000, &[]);
    // Leader 0 appends an entry but is cut off before replicating it.
    let mut lost = Vec::new();
    nodes[0]
        .propose(1_001, cmd(1, 1, Op::Put(0, 111)), &mut lost)
        .unwrap();
    // Node 1 becomes leader of {1, 2} and commits a different value.
    elect(&mut nodes, 1, 5_000, &[0]);
    assert_eq!(nodes[1].role, Role::Leader);
    let mut out = Vec::new();
    nodes[1]
        .propose(5_001, cmd(2, 1, Op::Put(0, 222)), &mut out)
        .unwrap();
    pump(
        &mut nodes,
        5_001,
        out.into_iter().map(|(t, m)| (1, t, m)).collect(),
        &[0],
    );
    // Heal: the old leader hears the new term and its conflicting suffix is replaced.
    let hb = nodes[1].tick(9_000);
    pump(
        &mut nodes,
        9_000,
        hb.into_iter().map(|(t, m)| (1, t, m)).collect(),
        &[],
    );
    let hb = nodes[1].tick(9_100);
    pump(
        &mut nodes,
        9_100,
        hb.into_iter().map(|(t, m)| (1, t, m)).collect(),
        &[],
    );
    assert_eq!(nodes[0].role, Role::Follower);
    for n in &nodes {
        assert_eq!(n.kv.get(0), 222, "node {}", n.id);
        assert_eq!(n.kv.result_of(1, 1), None);
    }
}

#[test]
fn lagging_follower_catches_up_through_install_snapshot() {
    let mut nodes = cluster(3);
    for n in nodes.iter_mut() {
        n.snapshot_every = 4;
    }
    elect(&mut nodes, 0, 1_000, &[]);
    for seq in 1..=20 {
        let mut out = Vec::new();
        nodes[0]
            .propose(1_000 + seq, cmd(1, seq, Op::Put(1, seq)), &mut out)
            .unwrap();
        pump(
            &mut nodes,
            1_000 + seq,
            out.into_iter().map(|(t, m)| (0, t, m)).collect(),
            &[2],
        );
    }
    assert!(nodes[0].p.snapshot.index > 0, "leader compacted its log");
    assert_eq!(nodes[2].last_applied, 0);
    let hb = nodes[0].tick(5_000);
    pump(
        &mut nodes,
        5_000,
        hb.into_iter().map(|(t, m)| (0, t, m)).collect(),
        &[],
    );
    let hb = nodes[0].tick(5_100);
    pump(
        &mut nodes,
        5_100,
        hb.into_iter().map(|(t, m)| (0, t, m)).collect(),
        &[],
    );
    assert_eq!(nodes[2].snapshots_installed, 1);
    assert_eq!(nodes[2].kv.get(1), 20);
    assert_eq!(nodes[2].commit_index, nodes[0].commit_index);
}

#[test]
fn restart_keeps_persistent_state_only() {
    let mut nodes = cluster(3);
    elect(&mut nodes, 0, 1_000, &[]);
    let mut out = Vec::new();
    nodes[0]
        .propose(1_001, cmd(1, 1, Op::Put(0, 5)), &mut out)
        .unwrap();
    pump(
        &mut nodes,
        1_001,
        out.into_iter().map(|(t, m)| (0, t, m)).collect(),
        &[],
    );
    let term = nodes[0].p.current_term;
    let r = nodes[0].restart(2_000);
    assert_eq!(r.role, Role::Follower);
    assert_eq!(r.p.current_term, term);
    assert_eq!(r.p.voted_for, Some(0));
    assert_eq!(r.last_index(), nodes[0].last_index());
    // Volatile state is rebuilt from the snapshot (none yet), not kept.
    assert_eq!(r.commit_index, 0);
}

#[test]
fn stale_term_messages_are_rejected() {
    let mut nodes = cluster(3);
    elect(&mut nodes, 0, 1_000, &[]);
    elect(&mut nodes, 1, 9_000, &[]);
    let t = nodes[1].p.current_term;
    let out = nodes[2].handle(
        9_001,
        0,
        Msg::AppendEntries {
            term: t - 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        },
    );
    assert_eq!(
        out,
        vec![(
            0,
            Msg::AppendEntriesResp {
                term: t,
                success: false,
                index: 0
            }
        )]
    );
}
