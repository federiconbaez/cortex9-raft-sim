//! CLI: run many seeds and report safety, linearizability and liveness.

use cortex9_raft_sim::raft::Bugs;
use cortex9_raft_sim::sim::{self, Config};
use std::process::ExitCode;
use std::time::Instant;

const USAGE: &str = "usage: cortex9-raft-sim [--seeds N] [--start S] [--seed S] [--nodes N] [--clients N]
                        [--chaos-ticks T] [--drop P] [--crash-rate P] [--down-max T] [--partition-rate P] [--bug stale-reads|forget-vote|short-quorum]
                        [--expect-failure] [--history]

Runs N seeds (default 200) starting at S. Exit 0 if every seed passes
(or, with --expect-failure, if at least one seed fails).";

fn main() -> ExitCode {
    let mut cfg = Config::default();
    let mut start = 0u64;
    let mut seeds = 200u64;
    let mut expect_failure = false;
    let mut print_history = false;
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    let num = |s: Option<&String>| -> Option<f64> { s.and_then(|v| v.parse().ok()) };
    while i < args.len() {
        let a = args[i].as_str();
        let v = args.get(i + 1);
        let mut takes = true;
        match a {
            "--seeds" => seeds = num(v).map(|x| x as u64).unwrap_or(seeds),
            "--start" => start = num(v).map(|x| x as u64).unwrap_or(start),
            "--seed" => {
                start = num(v).map(|x| x as u64).unwrap_or(0);
                seeds = 1;
            }
            "--nodes" => cfg.nodes = num(v).map(|x| x as usize).unwrap_or(cfg.nodes),
            "--clients" => cfg.clients = num(v).map(|x| x as usize).unwrap_or(cfg.clients),
            "--chaos-ticks" => {
                cfg.chaos_ticks = num(v).map(|x| x as u64).unwrap_or(cfg.chaos_ticks)
            }
            "--drop" => cfg.drop = num(v).unwrap_or(cfg.drop),
            "--crash-rate" => cfg.crash_rate = num(v).unwrap_or(cfg.crash_rate),
            "--down-max" => cfg.down_len.1 = num(v).map(|x| x as u64).unwrap_or(cfg.down_len.1),
            "--partition-rate" => cfg.partition_rate = num(v).unwrap_or(cfg.partition_rate),
            "--bug" => match v.map(String::as_str) {
                Some("stale-reads") => {
                    cfg.bugs = Bugs {
                        stale_reads: true,
                        ..Bugs::default()
                    }
                }
                Some("short-quorum") => {
                    cfg.bugs = Bugs {
                        short_quorum: true,
                        ..Bugs::default()
                    }
                }
                Some("forget-vote") => {
                    cfg.bugs = Bugs {
                        forget_vote: true,
                        ..Bugs::default()
                    }
                }
                _ => {
                    eprintln!("{USAGE}");
                    return ExitCode::from(2);
                }
            },
            "--expect-failure" => {
                expect_failure = true;
                takes = false;
            }
            "--history" => {
                print_history = true;
                takes = false;
            }
            _ => {
                eprintln!("{USAGE}");
                return ExitCode::from(2);
            }
        }
        i += if takes { 2 } else { 1 };
    }
    if cfg.nodes == 0 || cfg.clients == 0 {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    }

    let t0 = Instant::now();
    let (mut failed, mut ops, mut msgs, mut unknown) = (0u64, 0u64, 0u64, 0u64);
    let (mut elections, mut snaps, mut installs, mut partitions, mut crashes) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    for seed in start..start + seeds {
        let r = sim::run(&cfg, seed);
        ops += r.stats.completed_ops;
        msgs += r.stats.sent;
        elections += r.stats.elections;
        snaps += r.stats.snapshots_taken;
        installs += r.stats.snapshots_installed;
        partitions += r.stats.partitions;
        crashes += r.stats.crashes;
        unknown += r.unknown_keys.len() as u64;
        if print_history {
            for h in &r.history {
                println!("{h:?}");
            }
        }
        if !r.ok() {
            failed += 1;
            if failed <= 5 {
                println!(
                    "seed {seed}: FAIL invariants={:?} non_linearizable_keys={:?} unfinished={}",
                    r.invariant_violations.first(),
                    r.non_linearizable_keys,
                    r.unfinished
                );
            }
        }
    }
    let secs = t0.elapsed().as_secs_f64();
    println!(
        "seeds={seeds} failed={failed} ops={ops} msgs={msgs} elections={elections} partitions={partitions} crashes={crashes} snapshots={snaps} installs={installs} lin_unknown={unknown} time={secs:.2}s ({:.1} seeds/s)",
        seeds as f64 / secs
    );
    let ok = if expect_failure {
        failed > 0
    } else {
        failed == 0
    };
    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}
