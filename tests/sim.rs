//! Simulation tests over many seeds.

use cortex9_raft_sim::raft::Bugs;
use cortex9_raft_sim::sim::{run, Config};

fn seeds() -> u64 {
    if cfg!(debug_assertions) {
        150
    } else {
        1_000
    }
}

#[test]
fn correct_raft_passes_all_checks_default_faults() {
    let cfg = Config::default();
    let (mut installs, mut ops) = (0, 0);
    for seed in 0..seeds() {
        let r = run(&cfg, seed);
        assert!(
            r.ok(),
            "seed {seed}: {:?} {:?} unfinished={}",
            r.invariant_violations,
            r.non_linearizable_keys,
            r.unfinished
        );
        assert!(r.unknown_keys.is_empty(), "seed {seed}: checker budget");
        installs += r.stats.snapshots_installed;
        ops += r.stats.completed_ops;
    }
    assert!(installs > 0, "InstallSnapshot never exercised");
    assert!(ops > seeds() * 100);
}

#[test]
fn correct_raft_passes_crash_heavy_three_nodes() {
    let cfg = Config {
        nodes: 3,
        crash_rate: 0.002,
        down_len: (50, 200),
        ..Config::default()
    };
    for seed in 0..seeds() {
        let r = run(&cfg, seed);
        assert!(
            r.ok(),
            "seed {seed}: {:?} {:?} unfinished={}",
            r.invariant_violations,
            r.non_linearizable_keys,
            r.unfinished
        );
    }
}

#[test]
fn same_seed_same_run() {
    let cfg = Config::default();
    let a = run(&cfg, 1234);
    let b = run(&cfg, 1234);
    assert_eq!(a.history, b.history);
    assert_eq!(a.stats, b.stats);
    let c = run(&cfg, 1235);
    assert_ne!(a.history, c.history);
}

#[test]
fn checker_catches_stale_leader_reads() {
    let cfg = Config {
        bugs: Bugs {
            stale_reads: true,
            ..Bugs::default()
        },
        ..Config::default()
    };
    let failing = (0..100)
        .filter(|&s| !run(&cfg, s).non_linearizable_keys.is_empty())
        .count();
    assert!(failing > 0, "stale reads went unnoticed in 100 seeds");
}

#[test]
fn invariants_catch_unpersisted_votes() {
    let cfg = Config {
        nodes: 3,
        crash_rate: 0.002,
        down_len: (50, 200),
        bugs: Bugs {
            forget_vote: true,
            ..Bugs::default()
        },
        ..Config::default()
    };
    let found = (0..200).any(|s| {
        run(&cfg, s)
            .invariant_violations
            .iter()
            .any(|v| v.contains("election safety"))
    });
    assert!(found, "double vote went unnoticed in 200 seeds");
}

#[test]
fn single_node_cluster_is_linearizable() {
    let cfg = Config {
        nodes: 1,
        partition_rate: 0.0,
        ..Config::default()
    };
    for seed in 0..20 {
        assert!(run(&cfg, seed).ok(), "seed {seed}");
    }
}

#[test]
fn checks_catch_a_short_quorum() {
    let cfg = Config {
        bugs: Bugs {
            short_quorum: true,
            ..Bugs::default()
        },
        ..Config::default()
    };
    let failing = (0..20).filter(|&s| !run(&cfg, s).ok()).count();
    assert!(
        failing >= 10,
        "only {failing}/20 seeds caught a quorum of n/2"
    );
}
