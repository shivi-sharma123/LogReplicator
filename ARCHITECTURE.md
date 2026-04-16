# Rafty — Architecture & Raft Algorithm Guide

## Table of Contents

1. [What is Raft?](#what-is-raft)
2. [Project Overview](#project-overview)
3. [Directory Structure](#directory-structure)
4. [Backend Architecture](#backend-architecture)
   - [Core Raft Node](#1-core-raft-node-raftnodego)
   - [Virtual Transport](#2-virtual-transport-rafttransportgo)
   - [Cluster Controller](#3-cluster-controller-raftcontrollergo)
   - [Data Types](#4-data-types-rafttypesgo)
   - [API Server](#5-api-server-apiservergo)
5. [Frontend Architecture](#frontend-architecture)
   - [App (State Manager)](#1-app-appjsx)
   - [Visualizer](#2-raftyvisualizer-raftyvisualizerjsx)
   - [Control Panel](#3-control-panel-controlpaneljsx)
   - [Replicated Log](#4-replicated-log-replicatedlogjsx)
   - [Log Stream](#5-log-stream-logstreamjsx)
6. [Data Flow](#data-flow)
   - [Startup](#startup)
   - [Leader Election](#leader-election-sequence)
   - [Log Replication](#log-replication-sequence)
   - [Chaos Injection](#chaos-injection)
   - [WebSocket Broadcast](#websocket-state-broadcast)
7. [Raft Algorithm Deep Dive](#raft-algorithm-deep-dive)
   - [Core Problem](#core-problem-distributed-consensus)
   - [Server States](#server-states)
   - [Terms](#terms)
   - [Leader Election](#leader-election)
   - [Log Replication](#log-replication)
   - [Safety Guarantees](#safety-guarantees)
   - [What This Project Implements](#what-this-project-implements)
8. [System Diagram](#system-diagram)
9. [Running the Project](#running-the-project)

---

## What is Raft?

Raft is a **consensus algorithm** designed to be understandable. Its goal: allow a cluster of servers to agree on a sequence of values (a replicated log) even in the presence of failures.

> "Raft is equivalent to Paxos in fault-tolerance and performance. The difference is that it is decomposed into relatively independent sub-problems, and it cleanly addresses all major pieces needed for practical systems."
> — Diego Ongaro & John Ousterhout (2014)

**Why consensus matters:** In a distributed system, if you run the same log of commands on multiple machines in the same order, they stay in sync. Raft ensures all non-faulty servers agree on what that log contains — even when servers crash or messages are lost.

---

## Project Overview

**Rafty** is an interactive Raft consensus algorithm simulator and visualizer. It consists of:

- A **Go backend** that runs a real, in-memory 5-node Raft cluster
- A **React frontend** that visualizes the cluster state in real time
- A **chaos injection UI** to kill/restart nodes and observe how Raft recovers

This is an educational tool — you can watch leader elections happen live, replicate log entries across the cluster, and simulate network partitions.

---

## Directory Structure

```
rafty/
├── backend/
│   ├── raft/
│   │   ├── node.go          # RaftNode state machine
│   │   ├── transport.go     # Virtual in-memory transport layer
│   │   ├── controller.go    # ClusterController — orchestrates all nodes
│   │   └── types.go         # Shared data structures and RPC types
│   ├── api/
│   │   └── server.go        # REST + WebSocket API (Gin framework)
│   ├── main.go              # Entry point — creates 5-node cluster
│   ├── go.mod
│   └── go.sum
├── frontend/
│   ├── src/
│   │   ├── App.jsx                    # Root component, WebSocket client
│   │   ├── components/
│   │   │   ├── RaftyVisualizer.jsx    # Pentagon SVG visualization
│   │   │   ├── ControlPanel.jsx       # Kill/start/submit UI
│   │   │   ├── ReplicatedLog.jsx      # Log state across all nodes
│   │   │   └── LogStream.jsx          # Event stream display
│   │   └── main.jsx
│   ├── vite.config.js
│   └── package.json
├── Makefile
└── README.md
```

---

## Backend Architecture

### 1. Core Raft Node (`raft/node.go`)

The heart of the system. Each `RaftNode` is an independent Raft state machine running in its own goroutine.

#### State fields

```
Persistent (must survive restarts in a real system):
  CurrentTerm   int           — latest term this node has seen
  VotedFor      int           — candidate ID voted for in CurrentTerm (-1 = none)
  Log           []LogEntry    — replicated log: [{term, command}, ...]

Volatile (reset on restart):
  CommitIndex   int           — index of highest committed entry
  LastApplied   int           — index of highest applied entry
  State         Role          — Follower | Candidate | Leader
  IsAlive       bool          — simulation kill switch

Leader-only volatile (reset on becoming leader):
  NextIndex     map[int]int   — next log index to send to each peer
  MatchIndex    map[int]int   — highest log index confirmed replicated on each peer
```

#### Role lifecycle

```
             timeout / no heartbeat
  Follower ─────────────────────────► Candidate
     ▲                                    │
     │   higher term seen                 │  majority votes received
     │◄───────────────────────────────────┘
     │
     │   higher term seen        wins election
  Leader ◄──────────────────── Candidate
```

#### Key methods

| Method | Role | Description |
|---|---|---|
| `run()` | All | Main event loop; dispatches to role-specific handler |
| `runFollower()` | Follower | Waits for heartbeats; starts election on timeout |
| `runCandidate()` | Candidate | Increments term, broadcasts votes, counts results |
| `runLeader()` | Leader | Sends heartbeats and replicates new log entries every 100ms |
| `handleRequestVote()` | All | Grants or rejects vote request from a candidate |
| `handleAppendEntries()` | All | Processes heartbeat/replication RPC from leader |
| `broadcastRequestVote()` | Candidate | Sends `RequestVote` RPC to all peers |
| `broadcastAppendEntries()` | Leader | Sends `AppendEntries` RPC to all peers |
| `SubmitCommand(cmd)` | Leader | Appends a new entry to the leader's log |

#### Timing

| Parameter | Value |
|---|---|
| Election timeout | 300–600ms (randomized per node) |
| Heartbeat interval | 100ms |
| Votes required for majority | `⌊N/2⌋ + 1` = 3 out of 5 |

---

### 2. Virtual Transport (`raft/transport.go`)

Instead of real TCP sockets, nodes communicate via **Go channels**. This makes the simulation fully deterministic and easy to control.

```
VirtualTransport
├── inboxes   map[nodeID] → chan Message   (buffer: 100 messages each)
├── dropRate  float64                      (0.0 = no loss, 1.0 = total loss)
└── latency   time.Duration               (artificial delay per message)
```

**Send flow:**
1. Caller calls `transport.Send(msg)`
2. If `rand.Float64() < dropRate` → drop silently (simulates packet loss)
3. Otherwise, spawn goroutine: sleep `latency`, then write to `inboxes[msg.To]`
4. If inbox is full (100 messages), drop and log a warning

**Chaos control:** `dropRate` and `latency` can be tuned to simulate degraded networks. Currently not exposed in the UI, but accessible in code.

---

### 3. Cluster Controller (`raft/controller.go`)

`ClusterController` owns all 5 nodes and the shared transport. It is the single entry point for the API layer.

```
ClusterController
├── Nodes      map[int]*RaftNode
└── Transport  *VirtualTransport
```

| Method | Description |
|---|---|
| `NewClusterController(n)` | Creates `n` nodes with full-mesh peering |
| `Start()` | Launches goroutines for all nodes |
| `StopNode(id)` | Sets `IsAlive = false` on a node |
| `StartNode(id)` | Sets `IsAlive = true`, resets heartbeat timer |
| `Submit(cmd)` | Finds the current leader and calls `SubmitCommand(cmd)` |
| `GetState()` | Returns a JSON-serializable snapshot of all nodes |

---

### 4. Data Types (`raft/types.go`)

#### RPC messages

```go
// Vote request from candidate to all peers
RequestVoteArgs  { Term, CandidateID, LastLogIndex, LastLogTerm }
RequestVoteReply { Term, VoteGranted }

// Heartbeat + log replication from leader to all peers
AppendEntriesArgs  { Term, LeaderID, PrevLogIndex, PrevLogTerm, Entries[], LeaderCommit }
AppendEntriesReply { Term, Success }
```

#### Wire format

```go
Message {
  From    int         // sender node ID
  To      int         // recipient node ID
  Type    string      // "RequestVote" | "RequestVoteReply" | "AppendEntries" | "AppendEntriesReply"
  Payload interface{} // one of the RPC arg/reply structs above
}
```

---

### 5. API Server (`api/server.go`)

Built with the **Gin** framework. CORS is open for local development.

| Endpoint | Method | Description |
|---|---|---|
| `/control/stop/:id` | POST | Kill node `id` |
| `/control/start/:id` | POST | Revive node `id` |
| `/control/submit` | POST | Submit command to leader; body: `{"command": "..."}` |
| `/ws` | GET | WebSocket — streams full cluster state every 100ms |

#### WebSocket state payload (every 100ms)

```json
{
  "nodes": [
    {
      "id": 1,
      "state": "LEADER",
      "current_term": 3,
      "voted_for": 1,
      "commit_index": 5,
      "is_alive": true,
      "log": [
        { "term": 1, "command": "CMD-abc" },
        { "term": 2, "command": "CMD-def" }
      ]
    }
  ],
  "timestamp": 1713350000123
}
```

---

## Frontend Architecture

### 1. App (`App.jsx`)

Root component. Owns all shared state and communicates with the backend.

- Establishes WebSocket connection on mount; auto-reconnects on disconnect
- Maintains `clusterState` (latest snapshot) and `logs` (event stream, capped at 50)
- **Diffs previous vs. new state** to detect events: leader elections, term increments
- Dispatches HTTP calls (axios) for stop/start/submit actions

### 2. RaftyVisualizer (`RaftyVisualizer.jsx`)

Pentagon topology rendered in SVG. Nodes are placed evenly on a circle.

| Node state | Color | Icon |
|---|---|---|
| Follower | Blue | HeartPulse |
| Candidate | Yellow | Activity |
| Leader | Green + halo | Crown |
| Dead | Red | Skull |

- Full-mesh dashed lines connect all nodes
- Animated green pulses travel from the leader to each alive follower (heartbeat visual)
- Powered by **framer-motion** for smooth transitions

### 3. Control Panel (`ControlPanel.jsx`)

- Global "Send Work Request" button — submits a command to the cluster
- Per-node Kill / Start buttons for chaos injection

### 4. Replicated Log (`ReplicatedLog.jsx`)

Grid view: one row per node. Each row shows that node's committed log entries as color-coded blocks with term numbers. Lets you visually confirm that all alive nodes converge on the same log.

### 5. Log Stream (`LogStream.jsx`)

Scrolling event feed. Shows timestamped entries for: leader elections, term changes, user actions, system events. Auto-scrolls to newest entry.

---

## Data Flow

### Startup

```
main.go
  └─► NewClusterController(5)
        ├─ Creates RaftNode 1..5
        ├─ Each node registers with VirtualTransport (gets inbox channel)
        └─ Start() — launches goroutine per node
  └─► api.StartServer(controller)
        └─ Gin listens on :8080
```

### Leader Election Sequence

```
1. All nodes start as Followers with random election timeouts (300-600ms)
2. First node to time out:
   a. Increments CurrentTerm
   b. Sets VotedFor = self
   c. Transitions to Candidate
   d. Broadcasts RequestVote{term, self, lastLogIndex, lastLogTerm}
3. Each peer receiving RequestVote:
   a. If message term > own term → update term, revert to Follower
   b. Grant vote if: haven't voted this term AND candidate log is ≥ as up-to-date
   c. Send RequestVoteReply{term, voteGranted}
4. Candidate collects replies:
   a. 3 granted votes → transitions to Leader
   b. Higher term in reply → revert to Follower (lost election)
5. Leader immediately broadcasts empty AppendEntries (heartbeat)
   → all Followers reset their election timers
```

### Log Replication Sequence

```
1. Frontend calls POST /control/submit {"command": "work-xyz"}
2. API handler → Controller.Submit("work-xyz")
3. Leader.SubmitCommand("work-xyz") → appends LogEntry{CurrentTerm, "work-xyz"}
4. On next heartbeat (≤100ms later), leader builds AppendEntries RPC:
     { term, leaderID, prevLogIndex, prevLogTerm, entries: [new entry], leaderCommit }
5. Each follower receives AppendEntries:
   a. Validates PrevLogIndex / PrevLogTerm match → ensures no gap in log
   b. On match: appends new entries, updates CommitIndex, replies Success=true
   c. On mismatch: replies Success=false
6. Leader receives replies:
   a. Success=true → updates MatchIndex[peer], NextIndex[peer]
   b. When majority (≥3) have replicated → advance CommitIndex
   c. Success=false → decrement NextIndex[peer], retry with more entries next heartbeat
7. Followers learn of new CommitIndex via next heartbeat's LeaderCommit field
```

### Chaos Injection

```
Kill Node 2:
  POST /control/stop/2
    → node.IsAlive = false
    → node's run loop enters sleep-only mode
    → if node was leader: followers time out, start new election

Start Node 2:
  POST /control/start/2
    → node.IsAlive = true, LastHeartbeat = now
    → node resumes as Follower
    → receives log catch-up via AppendEntries from leader
```

### WebSocket State Broadcast

```
API server goroutine:
  every 100ms:
    state = controller.GetState()   // snapshot of all 5 nodes
    json.Marshal(state) → write to each connected WebSocket client

Frontend:
  ws.onmessage → parse JSON → setClusterState(newState)
  → diff with prevState → append detected events to log stream
  → React re-renders visualization
```

---

## Raft Algorithm Deep Dive

### Core Problem: Distributed Consensus

Given N servers, how do they agree on a sequence of values when:
- Any server can crash at any time
- Messages can be delayed or dropped
- There is no shared clock

Raft guarantees progress as long as a **majority (⌊N/2⌋+1) of servers are alive and reachable**. With 5 nodes, it tolerates 2 simultaneous failures.

### Server States

Every Raft server is always in one of three states:

```
FOLLOWER   — passive; responds to RPCs from leader and candidates
CANDIDATE  — actively seeking to become leader
LEADER     — handles all client requests; replicates log to followers
```

### Terms

Time is divided into **terms** — numbered consecutively starting at 1.

```
Term 1         Term 2        Term 3 ...
│──────────────│─────────────│─────────────►
  Election   Leader  Election  Leader
  (success)          (failure) (success)
```

- Each term begins with an election
- At most one leader per term (zero if the election fails with a split vote)
- **Terms are the Raft clock**: if a node sees a message with a higher term, it immediately steps down to Follower and updates its term

### Leader Election

**Trigger:** A Follower hasn't heard from a leader within its election timeout (300–600ms, randomized to prevent ties).

**Process:**
1. Increment `CurrentTerm`, vote for self, become Candidate
2. Send `RequestVote(term, candidateId, lastLogIndex, lastLogTerm)` to all peers
3. Peer grants vote if and only if:
   - The candidate's term ≥ peer's current term
   - The peer hasn't voted for another candidate this term
   - The candidate's log is **at least as up-to-date** as the peer's log
4. Candidate wins with a **majority** of votes → becomes Leader
5. Leader sends heartbeats immediately to prevent new elections

**Up-to-date log comparison:**
- Higher `LastLogTerm` wins
- If same `LastLogTerm`, higher `LastLogIndex` wins

**Why randomized timeouts?** If all nodes timed out simultaneously, they'd all become candidates and split the vote indefinitely. Randomization ensures one node usually wins the race to start an election.

### Log Replication

The leader is the **single source of truth** for the log. It replicates entries to followers.

**`AppendEntries` RPC (also serves as heartbeat):**
```
Leader → Follower:
  term           — current leader's term
  leaderId       — so followers can redirect clients
  prevLogIndex   — index of log entry immediately before new ones
  prevLogTerm    — term of prevLogIndex entry
  entries[]      — new log entries to append (empty for heartbeat)
  leaderCommit   — leader's current commitIndex
```

**Consistency check:** Before appending, the follower verifies it has an entry at `prevLogIndex` with term `prevLogTerm`. If not, it rejects — the leader will then send earlier entries (backing up `nextIndex`) until the logs converge.

**Commit rule:** An entry is committed once the leader has replicated it to a majority. The leader then advances `commitIndex`, and followers learn of this via the next `AppendEntries`.

**Log conflict resolution:**
```
Leader:   [1][1][2][3][3]
Follower: [1][1][2][4]    ← conflicting entries from an old term

On receiving AppendEntries with prevLogIndex=3, prevLogTerm=3:
  Follower's entry at index 3 has term=4 ≠ 3 → reject
Leader backs up to prevLogIndex=2, retries
  Follower matches → truncates entries from index 3 onward → appends leader's entries
Result: [1][1][2][3][3]  ← follower now matches leader
```

### Safety Guarantees

Raft provides these properties in all non-Byzantine failure scenarios:

| Property | Guarantee |
|---|---|
| **Election Safety** | At most one leader per term |
| **Leader Append-Only** | A leader never overwrites or deletes its own log entries |
| **Log Matching** | If two logs agree at index `i` and term `t`, all preceding entries are identical |
| **Leader Completeness** | All committed entries from previous terms are present on any new leader |
| **State Machine Safety** | All servers apply the same log entry at each index |

### What This Project Implements

| Feature | Status | Notes |
|---|---|---|
| Leader election | Fully implemented | Randomized timeouts, majority voting, term tracking |
| Log replication | Fully implemented | AppendEntries, conflict detection and resolution |
| Heartbeats | Fully implemented | 100ms interval; used to prevent spurious elections |
| Node crash simulation | Fully implemented | Via `IsAlive` flag; leader steps down, new election triggered |
| Network chaos | In code, not in UI | `dropRate` and `latency` on VirtualTransport |
| Log compaction (snapshots) | Not implemented | Intentional — not needed for visualization |
| Persistence to disk | Not implemented | Intentional — state resets on restart are fine for demo |
| Dynamic cluster membership | Not implemented | Fixed 5-node cluster |
| Client session deduplication | Not implemented | Commands are idempotent strings in this simulator |

---

## System Diagram

```
┌────────────────────────────────────────────────────────────────────┐
│                         RAFTY SIMULATOR                            │
└────────────────────────────────────────────────────────────────────┘

  FRONTEND (React + Vite)                 BACKEND (Go)
  ┌─────────────────────────┐             ┌──────────────────────────┐
  │ App.jsx                 │  HTTP/WS    │ api/server.go (Gin)      │
  │  ├─ clusterState        │◄───────────►│  POST /control/stop/:id  │
  │  ├─ logs (event stream) │             │  POST /control/start/:id │
  │  └─ WebSocket client    │             │  POST /control/submit    │
  └────────────┬────────────┘             │  GET  /ws (100ms push)   │
               │                          └──────────────┬───────────┘
    ┌──────────┼──────────┐                              │
    │          │          │                ┌─────────────▼────────────┐
    ▼          ▼          ▼                │ ClusterController        │
 Visualizer  Control  ReplicatedLog        │  ├─ 5 × RaftNode         │
 (SVG)       Panel    (Log grid)           │  └─ VirtualTransport     │
                                           └─────────────┬────────────┘
 LogStream                                               │
 (events)                              ┌────────────────▼─────────────┐
                                       │ VirtualTransport             │
                                       │  ├─ in-memory chan per node  │
                                       │  ├─ drop rate (chaos)        │
                                       │  └─ artificial latency       │
                                       └────────────────┬─────────────┘
                                                        │
                          ┌───────────┬─────────────────┼────────────────┬──────────┐
                          │           │                  │                │          │
                       Node 1      Node 2             Node 3           Node 4    Node 5
                     RaftNode    RaftNode           RaftNode          RaftNode  RaftNode
                    (Follower)  (Follower)          (Leader)         (Follower) (Follower)
                          │           │                  │                │          │
                          └───────────┴──── channels ────┴────────────────┴──────────┘
                                           (full mesh)
```

---

## Running the Project

```bash
# Start everything (backend + frontend)
make run

# Backend only (Go)
make run-backend

# Frontend only (React)
make run-frontend

# Stop everything
make stop
```

| Service | URL |
|---|---|
| Frontend | http://localhost:5173 |
| Backend API | http://localhost:8080 |
| WebSocket | ws://localhost:8080/ws |
