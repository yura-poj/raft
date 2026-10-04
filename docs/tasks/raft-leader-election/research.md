# Research: Raft skeleton, leader election, status API and cluster UI

## Status

- Stage: `/research`
- Date: 2026-10-01
- Product code changed: no
- Source document: `/Users/pozhidaev/Downloads/raft.pdf`
- Russian reference copies:
  - `/Users/pozhidaev/Documents/ChatGPT/raft/output/pdf/raft-russian-translation.pdf`
  - `/Users/pozhidaev/Documents/ChatGPT/raft/output/pdf/raft-short-russian.pdf`

## Goal

Create a runnable educational Raft skeleton in Go that implements only leader election and exposes the current cluster state through REST endpoints and a small web UI. Nodes must be runnable as separate local processes from different terminals.

The implementation is intentionally narrower than a complete Raft replicated state machine.

## Confirmed scope

### Included

- Go implementation.
- A fixed cluster configured before startup.
- Independent node processes started manually from separate terminals.
- Raft node roles: follower, candidate and leader.
- Monotonically increasing terms while a process is running.
- Randomized election timeouts.
- Self-vote and majority-based leader election.
- `RequestVote` RPC handling.
- Empty `AppendEntries` RPCs as leader heartbeats.
- Transition to follower when a node observes a greater term.
- Re-election after the current leader becomes unavailable.
- Local status and health REST endpoints.
- Internal Raft REST endpoints for votes and heartbeats.
- A cluster-status REST endpoint suitable for the UI.
- A frontend that shows every configured node and its current state, including an explicit unreachable state when a node cannot be contacted.
- Automated tests for meaningful election behavior, including initial election and failover.
- Instructions for launching a small local cluster in separate terminals.

### Excluded

- Docker and Docker Compose.
- Log replication and client commands.
- A replicated state machine.
- Commit indexes and applying log entries.
- Snapshots and log compaction.
- Dynamic membership changes.
- Persistent storage for `currentTerm`, `votedFor` or any other Raft state.
- Commit, push, merge and deployment.

## Acceptance criteria

1. A user can start at least three nodes from different terminals by supplying a node identifier, its listen address and the fixed peer list.
2. Every node starts as a follower.
3. In a healthy three-node cluster, exactly one node becomes leader after an election timeout; the other nodes remain followers.
4. The elected leader periodically sends heartbeats and stable followers do not start competing elections while those heartbeats arrive.
5. If the leader process is stopped, the remaining majority elects a new leader.
6. A node steps down to follower when it receives a valid leader heartbeat for the same or a greater term, or sees a greater term in any Raft request or response.
7. A candidate wins only with a majority of all configured cluster members, including its own vote.
8. Split votes are retried using newly randomized election timeouts.
9. `GET /status` exposes the local node identity, configured address, role, current term, known leader, current vote and peer information required to understand election state.
10. `GET /health` provides a lightweight process-liveness response.
11. Internal REST endpoints accept `RequestVote` and empty `AppendEntries` heartbeat messages and return the receiver's current term and decision/result.
12. A cluster-level REST response and the frontend show every configured node, its reachability, role, term and known leader without requiring Docker or an external service.
13. The UI is accessible from a running node and remains useful when one of the other nodes is unavailable.
14. Tests cover business-significant behavior: majority calculation, one vote per term while a process is alive, transition on higher terms, initial leader election, heartbeat stability and leader failover.
15. Documentation states clearly that this is an educational partial implementation, not a production-ready Raft system.

## Current repository state

There is no product implementation to extend and therefore no product-code location to cite yet.

- The repository is on branch `main` and has no commits.
- There is no `go.mod`, application entry point, package structure, README or existing test suite.
- Existing files are generated/reference PDF artifacts and temporary PDF-generation scripts under `output/pdf/` and `tmp/pdfs/`.
- No established RFC, ADR or task-plan convention exists. The default task path `docs/tasks/raft-leader-election/` is therefore used.

## Verified Raft requirements from the source

The following facts were checked against the complete relevant pages of the attached English paper, *In Search of an Understandable Consensus Algorithm (Extended Version)* by Diego Ongaro and John Ousterhout.

### State and term rules

Figure 2 on PDF page 4 defines the three roles and the common term rule:

- every server is a follower, candidate or leader;
- a request or response containing a term greater than the local term updates the local term and converts the node to follower;
- stale-term requests are rejected;
- the full Raft algorithm treats `currentTerm`, `votedFor` and the log as persistent state.

Section 5.1 on PDF page 5 explains that terms are monotonically increasing logical-clock values and that at most one leader may be elected in a term.

### Election behavior

Section 5.2 on PDF pages 5-6 specifies:

- nodes start as followers;
- a leader periodically sends empty `AppendEntries` heartbeats;
- a follower starts an election if it receives no valid leader or candidate communication before its election timeout;
- when starting an election, a node increments its term, becomes candidate, votes for itself, resets its election timer and sends `RequestVote` RPCs to the other servers in parallel;
- a candidate becomes leader after receiving votes from a majority of the full cluster in the same term;
- each server grants at most one vote per term on a first-come-first-served basis;
- a candidate receiving a valid `AppendEntries` call from a leader with an equal or greater term becomes follower;
- an election without a winner is retried in a new term;
- randomized election timeouts reduce simultaneous candidacies and repeated split votes; the paper uses 150-300 ms as an example rather than a mandatory production value.

### RPC shape

Figure 2 on PDF page 4 defines `RequestVote` with candidate term and identity plus last-log metadata, and defines `AppendEntries` with leader term and identity plus log metadata. Empty `AppendEntries` requests are heartbeats.

Because log replication is outside this task, the design stage must decide whether to retain the last-log and previous-log fields as zero-valued protocol-compatible placeholders or to define an explicitly reduced educational DTO. Keeping the fields would make the boundary with future log replication clearer; omitting them would make the current exercise smaller.

### Timing and failures

Sections 5.5-5.6 on PDF page 10 state that failed RPCs can be retried and that availability depends on the relationship:

`broadcastTime << electionTimeout << MTBF`

For a local educational cluster, heartbeat and election values should be configurable and should maintain a clear gap between heartbeat frequency and the minimum election timeout. Exact defaults belong in the design stage.

## Interpretation of the REST and frontend requirement

The confirmed external behavior is:

- each node exposes its own status and health;
- each node accepts the Raft election RPCs required by peers;
- opening the frontend on a running node shows the whole configured cluster rather than only the serving node;
- unreachable peers remain visible with an error/unreachable state;
- the UI must not require a separate frontend process.

The simplest compatible direction is a static HTML/CSS/JavaScript page served by the Go process plus a same-origin cluster-status endpoint. Whether the server aggregates peer status or the browser queries peers directly is a design decision. Server-side aggregation is likely safer for a local demo because it avoids browser CORS configuration and gives one consistent timeout/error model, but this is not yet an approved implementation plan.

## Constraints and deliberate deviations

### No durable election state

The paper requires `currentTerm` and `votedFor` to be written to stable storage before responding to RPCs. The user explicitly excluded persistence for this iteration.

Consequences:

- election behavior can be correct while all participating processes remain alive;
- after a process restart, the node forgets its previous term and vote;
- a restarted node can vote more than once for what other nodes consider the same term;
- therefore the implementation cannot claim Raft election safety across process restarts;
- restart behavior must be documented as a demo limitation and must not be presented as production-safe consensus.

This limitation does not prevent demonstrating initial election or failover while a majority of surviving processes stays running.

### Fixed membership

The majority must be computed from the complete configured cluster, not only currently reachable nodes. Otherwise a network partition could allow minority nodes to elect a leader. Membership changes are excluded.

### Concurrency

Election timeout, heartbeat processing, vote requests, role changes and status reads will occur concurrently. Shared Raft state must have one explicit synchronization model and network calls must not be made while holding the state lock. This is a high-risk implementation area to address in `/design` and verify with Go's race detector during `/release`.

### Network behavior

Peer calls require bounded timeouts. A slow or unavailable peer must not block elections, heartbeats, the local status endpoint or the cluster UI indefinitely. Additional generalized retry layers, backoff policies or observability features are outside scope unless included in the approved design.

## Dependencies

- A supported Go toolchain available locally.
- Only standard-library HTTP, JSON, synchronization and timing facilities appear necessary for the confirmed scope.
- Browser support for a simple locally served page; no JavaScript framework is required by the request.
- Fixed node and peer addresses supplied at startup.

No database, message broker, container runtime or external frontend toolchain is required.

## Risks

1. **Restart safety:** no persistence deliberately weakens Raft's election-safety guarantee across restarts.
2. **Timer races:** stale timer callbacks can start an election after a valid heartbeat unless timer ownership/reset rules are precise.
3. **Stale RPC responses:** a vote or heartbeat response arriving after a term/role transition can corrupt state unless its term and election generation are checked.
4. **Split votes:** fixed or synchronized timeouts can cause repeated ties; randomization and deterministic test control are both needed.
5. **Majority mistakes:** counting only successful/reachable peers would violate the fixed-cluster majority rule.
6. **Lock contention or deadlock:** performing HTTP calls while holding the node-state lock could block the entire node.
7. **Misleading UI:** cluster aggregation is an observer's snapshot and may contain statuses captured at slightly different times; the page must not imply atomic global state.
8. **Scope creep:** full `AppendEntries` log semantics, client redirection, persistence and membership changes are explicitly excluded even though the paper describes them.

## Assumptions to carry into design

- A typical demonstration cluster contains three nodes, but the majority calculation must work for any fixed non-empty peer configuration.
- A node's configured member set includes itself exactly once.
- Node identifiers are unique and addresses are valid before the process starts.
- The frontend is functional and intentionally small; visual design is secondary to clear status presentation.
- Tests may use shorter injected timing values than manual runs, while production code defaults remain human-observable and configurable.
- Internal and external endpoints may share one HTTP listener per node.

## Open design decisions

These do not require further user input before `/design`, but the technical plan must resolve them explicitly:

1. Package boundaries and the state-machine/event-loop synchronization model.
2. CLI flag or config-file shape for node ID, listen address, peers and timing values.
3. Exact REST paths, DTO field names, HTTP status codes and request timeouts.
4. Full Figure 2 RPC DTOs with zero-valued log fields versus reduced election-only DTOs.
5. Server-side cluster-status aggregation and its timeout/error representation.
6. Timer abstraction needed for reliable, non-flaky tests.
7. Integration-test process model: in-process HTTP servers or spawned binaries.

## Research conclusion

The requested scope is feasible as a small greenfield Go service with no external runtime dependencies. The source provides a precise election state machine and RPC rules, while the requested REST/UI layer is an application-specific observer around that state.

The most important limitation is explicit: without durable `currentTerm` and `votedFor`, the result can demonstrate leader election and failover but is not a fully safe Raft implementation across restarts. The next stage should turn these findings into a file-by-file implementation plan without changing product code.
