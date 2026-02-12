# Rafty: The Raft Consensus Algorithm Visualizer

**Rafty** is a "Chaos Monkey" style visualizer for the Raft Consensus Algorithm. It simulates a cluster of 5 nodes with a virtual networking layer, allowing you to deterministically inject faults (partition networks, kill nodes) and watch the cluster heal and reach consensus in real-time.

## 🚀 Quick Start

### Prerequisites
- **Go** (1.20+)
- **Node.js** (16+) & **npm**

### Running the Simulator
We have provided a `Makefile` for convenience.

1.  **Clone the repository**.
2.  **Start everything** (Backend + Frontend):
    ```bash
    make run
    ```
    - **Frontend**: http://localhost:5173
    - **Backend API**: http://localhost:8080

### Stopping
To kill all running processes:
```bash
make stop
```

---

## 🎮 How to Use

### The Interface
- **Left Panel (The Pentagon)**: This is your cluster.
  - **Blue Nodes**: Followers.
  - **Yellow Nodes**: Candidates (requesting votes).
  - **Green Node (with Crown)**: The Leader.
  - **Red Skull**: Dead/Stopped node.
- **Middle Panel (Replicated Log)**:
  - Shows the commit log for each node.
  - Verification of consensus: All Alive nodes should essentially have the same log.
- **Right Panel (Chaos Control)**:
  - **Kill/Start Buttons**: Toggle the state of any node.
  - **Send Work Request**: Simulates a client sending a command to the cluster.

### Scenarios to Try
1.  **Leader Election**: Watch the initial election. A leader will emerge naturally.
2.  **Log Replication**: Click "Send Work Request". Watch the green log entries propagate to all followers.
3.  **Chaos - Killing the Leader**:
    - Identify the Leader.
    - Click **Kill**.
    - Watch as followers stop receiving heartbeats (green pulses stop).
    - A new Term will start, and a new Leader will be elected.
4.  **Partition Recovery**:
    - **Start** the old leader back up.
    - Watch it join as a Follower and "catch up" its missing logs.

---

## 🏗️ Architecture

The project is split into a Go backend and a React frontend.

### Backend (`/backend`)
- **Language**: Go
- **Framework**: Standard Library + Gin for API
- **Key Files**:
  - `raft/node.go`: The core Raft state machine (Follower/Candidate/Leader logic).
  - `raft/transport.go`: A **Virtual Transport Layer**. Instead of real TCP/UDP, nodes communicate via Go Channels. This allows us to simulate dropped packets and delays easily.
  - `raft/controller.go`: The Cluster Controller. It acts as the "God Object" that ticks the simulation and broadcasts state to the frontend via WebSockets.

### Frontend (`/frontend`)
- **Language**: JavaScript (React + Vite)
- **Key Libraries**:
  - `framer-motion`: For the smooth animations of nodes and messages.
  - `lucide-react`: For the UI icons.
- **Visuals**:
  - SVG-based rendering for the node graph.
  - Responsive layout utilizing standard CSS Flexbox/Grid.

---

## 🛠️ API Reference

The backend exposes a simple control API:

- `POST /control/stop/:id`: Kills the module with ID `:id`.
- `POST /control/start/:id`: Restarts the module.
- `POST /control/submit`: Submits a new command string to the cluster leader.

**WebSocket**: `ws://localhost:8080/ws` emits the full cluster state JSON every 100ms.

---

## 📜 License
MIT
