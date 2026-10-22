# cortex9-raft-sim

[![CI](https://github.com/federiconbaez/cortex9-raft-sim/actions/workflows/ci.yml/badge.svg)](https://github.com/federiconbaez/cortex9-raft-sim/actions/workflows/ci.yml)
![License](https://img.shields.io/badge/license-Apache--2.0-blue)

Raft (elección de líder, replicación del log, snapshots) probado con **simulación determinista** y un **checker de linealizabilidad** estilo Porcupine. Sin dependencias.

Raft (leader election, log replication, snapshots) tested with **deterministic simulation** and a Porcupine-style **linearizability checker**. Zero dependencies.

Parte de la serie Cortex-9 / Part of the Cortex-9 series: [github.com/federiconbaez](https://github.com/federiconbaez#cortex-9).

> **ES** primero · **EN** below

---

## ES

### Qué hay

| Módulo | Contenido |
|---|---|
| `raft.rs` | Nodo Raft como máquina de estados pura (sin hilos, reloj ni I/O): elección con timeouts aleatorios, `AppendEntries` con retroceso rápido por término en conflicto, commit solo de entradas del término actual (§5.4.2), no-op del líder al asumir, compactación en snapshots e `InstallSnapshot`. |
| `kv.rs` | Máquina de estados KV con sesiones por cliente: un reintento se aplica a lo sumo una vez. |
| `sim.rs` | Simulador con semilla: red que **retrasa (1–25 ticks, reordena), descarta (5 %), duplica (2 %) y particiona** mensajes; **crashes** y reinicios conservando solo el estado persistente; clientes concurrentes con timeout y reintento. |
| `lin.rs` | Checker de linealizabilidad (búsqueda Wing–Gong con la memoización de Lowe sobre `(conjunto linealizado, estado)`), modelo de registro, historia particionada por clave. Operaciones sin respuesta se tratan como "pudo ocurrir en cualquier momento después de invocarse". |

Invariantes chequeados en línea en cada paso: **election safety** (≤ 1 líder por término), **state machine safety** (toda aplicación en el índice *i* es idéntica en todos los nodos), **leader completeness** (un líder nuevo contiene todas las entradas comprometidas), snapshot ≤ commit. Al final: **liveness** (con la red sana, todas las operaciones pendientes terminan) y linealizabilidad por clave.

Todo es función de la semilla: misma semilla ⇒ misma historia (hay un test que lo verifica).

### Uso

```sh
cargo run --release -- --seeds 10000
cargo run --release -- --seed 42 --history          # reproducir una semilla
cargo run --release -- --seeds 1000 --nodes 3 --crash-rate 0.002 --down-max 200
cargo run --release -- --seeds 200 --bug stale-reads --expect-failure
```

### Resultados medidos

Máquina: VM Linux 6.12, Intel Xeon, 8 vCPU; un solo hilo; `--release`. Config por defecto: 5 nodos, 4 clientes, 3 claves, 6000 ticks con fallas + 4000 de recuperación.

| Corrida | Semillas | Fallan | Ops completadas | Snapshots instalados | Tiempo |
|---|---:|---:|---:|---:|---:|
| Raft correcto, fallas por defecto | 10 000 | **0** | 3 675 183 | 206 247 | 23.4 s (427 semillas/s) |
| Raft correcto, 3 nodos, crashes frecuentes | 5 000 | **0** | 1 257 100 | 48 589 | 5.3 s |

El checker nunca agotó su presupuesto (`lin_unknown=0`).

**Controles negativos** (bugs inyectados a propósito, para mostrar que los chequeos no son vacíos):

| Bug | Qué detecta | Semillas que fallan |
|---|---|---:|
| `stale-reads`: el líder responde lecturas desde su estado local, sin pasar por el log | checker de linealizabilidad | 218 / 1000 |
| `forget-vote`: `voted_for` no se persiste al reiniciar (3 nodos, crashes frecuentes) | election safety | 12 / 1000 |
| `short-quorum`: quórum de `n/2` en vez de `n/2+1` | election safety, leader completeness, linealizabilidad | 981 / 1000 |

### Tests

24 tests: 11 unitarios (PRNG, sesiones KV, checker; el checker se compara contra fuerza bruta en 3000 historias aleatorias), 7 del nodo Raft con mensajes ruteados a mano (elección, replicación, sobrescritura de un sufijo no comprometido, `InstallSnapshot` a un seguidor atrasado, reinicio, términos viejos) y 7 de simulación (150 semillas en debug y 1000 en release por config, determinismo y los tres controles negativos). CI: ubuntu + macOS (fmt, clippy `-D warnings`, test debug y release), un job que corre 5000 + 3000 semillas y los controles negativos, y build con MSRV 1.75. No hay `unsafe`, así que no se corre Miri.

### Límites (honestos)

- Es un simulador, no un servidor: no hay red real, disco ni fsync. "Persistente" significa que el estado sobrevive al crash simulado, asumiendo fsync antes de enviar.
- Sin cambios de membresía, sin lecturas por lease/ReadIndex (las lecturas pasan por el log), sin envío de snapshot por chunks.
- El escenario de la Figura 8 del paper (comprometer entradas de términos viejos contando réplicas) **no** se logró reproducir con este simulador: con el no-op del líder la ventana es muy chica. Por eso no se ofrece como control negativo.
- Un control negativo que falla en el 1.2 % de las semillas (`forget-vote`) muestra que algunos bugs necesitan miles de semillas; 0 fallas en 10 000 semillas no es una prueba de corrección.

### Licencia

Apache-2.0.

---

## EN

### What is inside

| Module | Contents |
|---|---|
| `raft.rs` | Raft node as a pure state machine (no threads, clock or I/O): randomized election timeouts, `AppendEntries` with fast backoff by conflicting term, commit only of current-term entries (§5.4.2), leader no-op on election, log compaction into snapshots and `InstallSnapshot`. |
| `kv.rs` | KV state machine with per-client sessions: a retried command applies at most once. |
| `sim.rs` | Seeded simulator: a network that **delays (1–25 ticks, reorders), drops (5 %), duplicates (2 %) and partitions** messages; **crashes** and restarts that keep only persistent state; concurrent clients with timeouts and retries. |
| `lin.rs` | Linearizability checker (Wing–Gong search with Lowe's memoisation of `(linearized set, state)`), register model, history partitioned per key. Unanswered operations may take effect any time after invocation. |

Invariants checked online at every step: **election safety** (≤ 1 leader per term), **state machine safety** (every apply at index *i* is identical on all nodes), **leader completeness** (a new leader holds every committed entry), snapshot ≤ commit. At the end: **liveness** (with a healthy network every pending operation finishes) and per-key linearizability.

Everything is a function of the seed: same seed ⇒ same history (tested).

### Usage

See the Spanish section; flags: `--seeds`, `--start`, `--seed`, `--nodes`, `--clients`, `--chaos-ticks`, `--drop`, `--crash-rate`, `--down-max`, `--partition-rate`, `--bug stale-reads|forget-vote|short-quorum`, `--expect-failure`, `--history`. Exit 0 if every seed passes (or, with `--expect-failure`, if any fails).

### Measured results

Machine: Linux 6.12 VM, Intel Xeon, 8 vCPU; single thread; `--release`. Default config: 5 nodes, 4 clients, 3 keys, 6000 faulty ticks + 4000 recovery ticks.

| Run | Seeds | Failing | Completed ops | Snapshots installed | Time |
|---|---:|---:|---:|---:|---:|
| Correct Raft, default faults | 10,000 | **0** | 3,675,183 | 206,247 | 23.4 s (427 seeds/s) |
| Correct Raft, 3 nodes, frequent crashes | 5,000 | **0** | 1,257,100 | 48,589 | 5.3 s |

The checker never ran out of budget (`lin_unknown=0`).

**Negative controls** (bugs injected on purpose to show the checks are not vacuous): `stale-reads` (leader serves reads from local state) caught by the linearizability checker in 218/1000 seeds; `forget-vote` (vote not persisted; 3 nodes, frequent crashes) caught by election safety in 12/1000; `short-quorum` (quorum `n/2`) caught in 981/1000.

### Tests

24 tests: 11 unit (PRNG, KV sessions, checker; the checker is compared with brute force on 3000 random histories), 7 Raft-node tests with hand-routed messages (election, replication, overwrite of an uncommitted suffix, `InstallSnapshot` to a lagging follower, restart, stale terms) and 7 simulation tests (150 seeds in debug, 1000 in release per config, determinism, three negative controls). CI: ubuntu + macOS (fmt, clippy `-D warnings`, debug and release tests), a job running 5000 + 3000 seeds plus the negative controls, and an MSRV 1.75 build. There is no `unsafe`, so Miri is not run.

### Limits (honest)

- A simulator, not a server: no real network, disk or fsync. "Persistent" means it survives a simulated crash, assuming fsync before sending.
- No membership changes, no lease/ReadIndex reads (reads go through the log), no chunked snapshot transfer.
- The paper's Figure 8 scenario (committing old-term entries by counting replicas) could **not** be reproduced with this simulator; with the leader no-op the window is tiny, so it is not offered as a negative control.
- A control caught in only 1.2 % of seeds (`forget-vote`) shows some bugs need thousands of seeds; 0 failures in 10,000 seeds is not a proof of correctness.

### License

Apache-2.0.
