//! cortex9-raft-sim: Raft under deterministic simulation testing.
//!
//! - [`raft`]: the protocol as a pure state machine.
//! - [`sim`]: seeded network/crash simulator with online safety invariants.
//! - [`lin`]: Porcupine-style linearizability checker.

#![warn(missing_docs)]

pub mod kv;
pub mod lin;
pub mod raft;
pub mod rng;
pub mod sim;
