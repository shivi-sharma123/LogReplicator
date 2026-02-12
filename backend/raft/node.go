package raft

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type RaftNode struct {
	mu sync.Mutex

	// Identifier
	ID       int
	Peers    []int
	PeersMap map[int]bool

	// Persistent state
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry

	// Volatile state
	CommitIndex int
	LastApplied int

	// Leader Volatile state
	NextIndex  map[int]int
	MatchIndex map[int]int

	// State Integration
	State         Role
	LastHeartbeat time.Time

	// Simulation Controls
	IsAlive bool

	// Transport
	Transport *VirtualTransport
	Inbox     chan Message

	// Channels for signaling logic changes
	stopCh chan struct{}
}

func NewRaftNode(id int, peers []int, transport *VirtualTransport) *RaftNode {
	inbox := transport.Register(id)
	peersMap := make(map[int]bool)
	for _, p := range peers {
		peersMap[p] = true
	}

	return &RaftNode{
		ID:            id,
		Peers:         peers,
		PeersMap:      peersMap,
		State:         Follower,
		VotedFor:      -1, // -1 means null
		Log:           make([]LogEntry, 0),
		Transport:     transport,
		IsAlive:       true,
		CommitIndex:   -1,
		LastApplied:   -1,
		NextIndex:     make(map[int]int),
		MatchIndex:    make(map[int]int),
		stopCh:        make(chan struct{}),
		Inbox:         inbox,
		LastHeartbeat: time.Now(),
	}
}

func (rn *RaftNode) Start() {
	go rn.run()
}

func (rn *RaftNode) Stop() {
	rn.mu.Lock()
	rn.IsAlive = false
	rn.mu.Unlock()
}

func (rn *RaftNode) Resume() {
	rn.mu.Lock()
	rn.IsAlive = true
	rn.LastHeartbeat = time.Now()
	rn.mu.Unlock()
}

// NodeInfo is a snapshot of the node state
type NodeInfo struct {
	ID          int        `json:"id"`
	State       Role       `json:"state"`
	CurrentTerm int        `json:"current_term"`
	VotedFor    int        `json:"voted_for"`
	CommitIndex int        `json:"commit_index"`
	IsAlive     bool       `json:"is_alive"`
	Log         []LogEntry `json:"log"`
}

// Thread-safe state getter for visualization
func (rn *RaftNode) GetState() NodeInfo {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	// Copy log to avoid race conditions during JSON marshaling outside lock
	logCopy := make([]LogEntry, len(rn.Log))
	copy(logCopy, rn.Log)

	return NodeInfo{
		ID:          rn.ID,
		State:       rn.State,
		CurrentTerm: rn.CurrentTerm,
		VotedFor:    rn.VotedFor,
		CommitIndex: rn.CommitIndex,
		IsAlive:     rn.IsAlive,
		Log:         logCopy,
	}
}

func (rn *RaftNode) run() {
	for {
		// Check liveness
		rn.mu.Lock()
		if !rn.IsAlive {
			rn.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		state := rn.State
		rn.mu.Unlock()

		switch state {
		case Follower:
			rn.runFollower()
		case Candidate:
			rn.runCandidate()
		case Leader:
			rn.runLeader()
		}
	}
}

func (rn *RaftNode) runFollower() {
	rn.mu.Lock()
	timeoutDuration := time.Duration(300+rand.Intn(300)) * time.Millisecond
	rn.mu.Unlock()

	timer := time.NewTimer(timeoutDuration)
	defer timer.Stop()

	for {
		rn.mu.Lock()
		if !rn.IsAlive { // Check liveness
			rn.mu.Unlock()
			return
		}
		if rn.State != Follower {
			rn.mu.Unlock()
			return
		}
		rn.mu.Unlock()

		select {
		case <-timer.C:
			rn.mu.Lock()
			// Check if we really timed out or if we just processed a message that reset it
			if time.Since(rn.LastHeartbeat) >= timeoutDuration {
				fmt.Printf("Node %d: Election timeout. Becoming Candidate.\n", rn.ID)
				rn.State = Candidate
				rn.mu.Unlock()
				return
			}
			// Reset timer
			timer.Reset(timeoutDuration) // simplified
			rn.mu.Unlock()

		case msg := <-rn.Inbox:
			rn.handleMessage(msg)
			// Reset timer on valid leader message
			// The handleMessage updates LastHeartbeat if valid
			rn.mu.Lock()
			// Simple reset
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(timeoutDuration)
			rn.mu.Unlock()
		}
	}
}

func (rn *RaftNode) runCandidate() {
	rn.mu.Lock()
	rn.CurrentTerm++
	rn.VotedFor = rn.ID
	rn.State = Candidate
	term := rn.CurrentTerm
	rn.LastHeartbeat = time.Now() // prevent immediate timeout
	rn.mu.Unlock()

	fmt.Printf("Node %d: Starting election for term %d\n", rn.ID, term)

	// Send RequestVote to all peers
	votesReceived := 1 // Vote for self
	votesNeeded := (len(rn.Peers)+1)/2 + 1
	// Note: Peers list doesn't include self. So total nodes = len(Peers) + 1.

	go rn.broadcastRequestVote()

	timeoutDuration := time.Duration(300+rand.Intn(300)) * time.Millisecond
	timer := time.NewTimer(timeoutDuration)
	defer timer.Stop()

	for {
		rn.mu.Lock()
		if !rn.IsAlive { // Check liveness
			rn.mu.Unlock()
			return
		}
		if rn.State != Candidate {
			rn.mu.Unlock()
			return
		}
		if votesReceived >= votesNeeded {
			fmt.Printf("Node %d: Won election for term %d\n", rn.ID, rn.CurrentTerm)
			rn.State = Leader

			// Re-initialize leader state
			for _, peer := range rn.Peers {
				rn.NextIndex[peer] = len(rn.Log) // or +1 based on indexing
				rn.MatchIndex[peer] = -1         // Changed from 0 to -1
			}

			rn.mu.Unlock()
			return
		}
		rn.mu.Unlock()

		select {
		case <-timer.C:
			// Election timeout -> Start new election (loop will restart runCandidate)
			return
		case msg := <-rn.Inbox:
			rn.handleMessage(msg)

			// Process vote reply
			if msg.Type == "RequestVoteReply" {
				// We need to parse payload.
				// Let's assume we use json bytes or struct.
				// Let's standardize on JSON bytes to simulating network better?
				// Or just struct. Struct is easier for now.
				if reply, ok := msg.Payload.(RequestVoteReply); ok {
					if reply.Term == rn.CurrentTerm && reply.VoteGranted {
						votesReceived++
					} else if reply.Term > rn.CurrentTerm {
						rn.State = Follower
						rn.CurrentTerm = reply.Term
						rn.VotedFor = -1
					}
				}
			}
		}
	}
}

func (rn *RaftNode) runLeader() {
	rn.mu.Lock()
	// Re-assert leader state just in case
	rn.State = Leader
	rn.mu.Unlock()

	ticker := time.NewTicker(100 * time.Millisecond) // Heartbeat interval
	defer ticker.Stop()

	// Immediately send first heartbeat
	rn.broadcastAppendEntries() // Changed from broadcastHeartbeat

	for {
		rn.mu.Lock()
		if !rn.IsAlive { // Check liveness
			rn.mu.Unlock()
			return
		}
		if rn.State != Leader {
			rn.mu.Unlock()
			return
		}
		rn.mu.Unlock()

		select {
		case <-ticker.C:
			rn.broadcastAppendEntries() // Changed from broadcastHeartbeat
		case msg := <-rn.Inbox:
			rn.handleMessage(msg)
		}
	}
}

// --- Handlers ---

func (rn *RaftNode) handleMessage(msg Message) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Payload handling
	// We'll perform type assertions.

	switch msg.Type {
	case "RequestVote":
		var args RequestVoteArgs
		if v, ok := msg.Payload.(RequestVoteArgs); ok {
			args = v
		} else {
			// Try converting from map if JSON unmarshalled into map
			b, _ := json.Marshal(msg.Payload)
			json.Unmarshal(b, &args)
		}
		rn.handleRequestVote(msg.From, args)

	case "AppendEntries":
		var args AppendEntriesArgs
		if v, ok := msg.Payload.(AppendEntriesArgs); ok {
			args = v
		} else {
			b, _ := json.Marshal(msg.Payload)
			json.Unmarshal(b, &args)
		}
		rn.handleAppendEntries(msg.From, args)

	case "RequestVoteReply":
		var reply RequestVoteReply
		if v, ok := msg.Payload.(RequestVoteReply); ok {
			reply = v
		} else {
			b, _ := json.Marshal(msg.Payload)
			json.Unmarshal(b, &reply)
		}

		if reply.Term > rn.CurrentTerm {
			rn.CurrentTerm = reply.Term
			rn.State = Follower
			rn.VotedFor = -1
			return
		}

	case "AppendEntriesReply":
		var reply AppendEntriesReply
		if v, ok := msg.Payload.(AppendEntriesReply); ok {
			reply = v
		} else {
			b, _ := json.Marshal(msg.Payload)
			json.Unmarshal(b, &reply)
		}

		if reply.Term > rn.CurrentTerm {
			rn.CurrentTerm = reply.Term
			rn.State = Follower
			rn.VotedFor = -1
			return
		}

		if rn.State == Leader {
			if reply.Success {
				// Update NextIndex and MatchIndex
				// We need to know which entries were sent.
				// Simplified: assume we sent up to len(Log)-1 (unsafe but simple for visualizer with no concurrency on single node log)
				// Better: We track MatchIndex.
				// For this visualizer, let's just forcefully advance NextIndex if successful.
				rn.NextIndex[msg.From] = len(rn.Log)
				rn.MatchIndex[msg.From] = len(rn.Log) - 1
			} else {
				// Back off
				if rn.NextIndex[msg.From] > 0 {
					rn.NextIndex[msg.From]--
				}
			}
		}
	}
}

func (rn *RaftNode) handleRequestVote(from int, args RequestVoteArgs) {
	reply := RequestVoteReply{
		Term:        rn.CurrentTerm,
		VoteGranted: false,
	}

	// 1. Reply false if term < currentTerm
	if args.Term < rn.CurrentTerm {
		rn.Transport.Send(Message{From: rn.ID, To: from, Type: "RequestVoteReply", Payload: reply})
		return
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rn.CurrentTerm {
		rn.CurrentTerm = args.Term
		rn.State = Follower
		rn.VotedFor = -1
	}

	reply.Term = rn.CurrentTerm

	// Check Log Up-to-Date
	lastLogIndex := len(rn.Log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rn.Log[lastLogIndex].Term
	}

	isLogOk := (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if (rn.VotedFor == -1 || rn.VotedFor == args.CandidateID) && isLogOk {
		rn.VotedFor = args.CandidateID
		reply.VoteGranted = true
		rn.LastHeartbeat = time.Now() // Granting vote resets election timeout!
		fmt.Printf("Node %d: Voted for Node %d in term %d\n", rn.ID, args.CandidateID, rn.CurrentTerm)
	}

	rn.Transport.Send(Message{From: rn.ID, To: from, Type: "RequestVoteReply", Payload: reply})
}

func (rn *RaftNode) handleAppendEntries(from int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{
		Term:    rn.CurrentTerm,
		Success: false,
	}

	if args.Term < rn.CurrentTerm {
		rn.Transport.Send(Message{From: rn.ID, To: from, Type: "AppendEntriesReply", Payload: reply})
		return
	}

	if args.Term > rn.CurrentTerm || rn.State != Follower {
		rn.CurrentTerm = args.Term
		rn.State = Follower
		rn.VotedFor = -1
	}

	rn.LastHeartbeat = time.Now()
	reply.Term = rn.CurrentTerm

	// Log Consistency Check
	// If PrevLogIndex points to a non-existent entry or term mismatch, return false
	// Note: PrevLogIndex is 0-indexed in our code relative to Log slice?
	// Or is it absolute index?
	// Let's assume Log slice contains all entries. prevLogIndex is index in that slice.
	// If PrevLogIndex == -1 (initial), it matches.

	// args.PrevLogIndex comes from leader.
	// We check if we have that index.

	// Case 0: Empty log, PrevLogIndex = -1 (or 0 if 1-based, let's say -1 denotes "before beginning")
	// Our Log slice indices are 0..N-1.
	// Let's treat indices as 0-based.
	// Leader sends PrevLogIndex = NextIndex - 1.
	// If NextIndex=0, PrevLogIndex=-1.

	if args.PrevLogIndex >= len(rn.Log) {
		// We don't have this entry
		reply.Success = false
		rn.Transport.Send(Message{From: rn.ID, To: from, Type: "AppendEntriesReply", Payload: reply})
		return
	}

	if args.PrevLogIndex >= 0 {
		if rn.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
			// Conflict
			reply.Success = false
			// Optimization: Delete conflict and all that follow
			rn.Log = rn.Log[:args.PrevLogIndex]
			rn.Transport.Send(Message{From: rn.ID, To: from, Type: "AppendEntriesReply", Payload: reply})
			return
		}
	}

	// Append new entries
	// We need to merge args.Entries into rn.Log starting at args.PrevLogIndex + 1
	insertIndex := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		idx := insertIndex + i
		if idx < len(rn.Log) {
			if rn.Log[idx].Term != entry.Term {
				// Conflict, truncate and replace
				rn.Log = rn.Log[:idx]
				rn.Log = append(rn.Log, entry)
			}
		} else {
			rn.Log = append(rn.Log, entry)
		}
	}

	if args.LeaderCommit > rn.CommitIndex {
		lastNewIndex := insertIndex + len(args.Entries) - 1
		if args.LeaderCommit < lastNewIndex {
			rn.CommitIndex = args.LeaderCommit
		} else {
			rn.CommitIndex = lastNewIndex
		}
	}

	reply.Success = true
	rn.Transport.Send(Message{From: rn.ID, To: from, Type: "AppendEntriesReply", Payload: reply})
}

// --- Broadcasters ---

// SubmitCommand allows external clients to propose a command
func (rn *RaftNode) SubmitCommand(cmd string) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.State != Leader {
		return false
	}

	newEntry := LogEntry{
		Term:    rn.CurrentTerm,
		Command: cmd,
	}
	rn.Log = append(rn.Log, newEntry)
	fmt.Printf("Node %d: Received command '%s' at index %d\n", rn.ID, cmd, len(rn.Log)-1)

	// Trigger immediate broadcast? Or wait for heartbeat ticker?
	// Ticker is simpler, but immediate feels more responsive.
	// We can't call broadcast here because of deadlock risk if broadcast also locks.
	// But broadcast locks. So we should unlock first or refactor.
	// Refactor broadcast to internal unlocked version `broadcastAppendEntriesLocked`?
	// or just let ticker handle it (max 100ms latency). Ticker is fine for sim.

	return true
}

func (rn *RaftNode) broadcastRequestVote() {
	rn.mu.Lock()
	lastLogIndex := len(rn.Log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rn.Log[lastLogIndex].Term
	}

	args := RequestVoteArgs{
		Term:         rn.CurrentTerm,
		CandidateID:  rn.ID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	peers := rn.Peers
	rn.mu.Unlock()

	for _, peer := range peers {
		rn.Transport.Send(Message{
			From:    rn.ID,
			To:      peer,
			Type:    "RequestVote",
			Payload: args,
		})
	}
}

func (rn *RaftNode) broadcastAppendEntries() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	peers := rn.Peers
	currentTerm := rn.CurrentTerm
	leaderID := rn.ID
	leaderCommit := rn.CommitIndex

	for _, peer := range peers {
		// Calculate PrevLogIndex and Entries for this peer
		prevLogIndex := rn.NextIndex[peer] - 1
		prevLogTerm := 0
		if prevLogIndex >= 0 && prevLogIndex < len(rn.Log) {
			prevLogTerm = rn.Log[prevLogIndex].Term
		}

		var entries []LogEntry
		if rn.NextIndex[peer] < len(rn.Log) {
			// Need to send entries
			entries = rn.Log[rn.NextIndex[peer]:]
		}

		args := AppendEntriesArgs{
			Term:         currentTerm,
			LeaderID:     leaderID,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}

		rn.Transport.Send(Message{
			From:    leaderID,
			To:      peer,
			Type:    "AppendEntries",
			Payload: args,
		})
	}
}
