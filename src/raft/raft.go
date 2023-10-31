package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type ServerState int

const (
	Follower  ServerState = 0
	Candidate ServerState = 1
	Leader    ServerState = 2
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	applyCh     chan ApplyMsg       // apply channel
	dead        int32               // set by Kill()
	sstate      ServerState
	leaderAlive chan int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state
	commitIndex int
	lastApplied int

	// Leader state
	nextIndex  []int // index of the next log entry to send
	matchIndex []int // index of highest log entry known to be replicated
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.sstate == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// assume already has the rf lock
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var rflog []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&rflog) != nil {
		//   error...
		log.Fatalf("missing persistent")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = rflog
	}
}

func (rf *Raft) applyCommited() {
	// apply commited logs
	// assume caller holding the lock
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	if len(rf.log) > 0 {
		offset := rf.log[0].Index
		for i := rf.lastApplied - offset + 1; rf.lastApplied < rf.commitIndex; i++ {
			rf.lastApplied += 1

			if rf.log[i].Index != rf.lastApplied {
				log.Fatalf("log index not match!")
			}
			msg := ApplyMsg{}
			msg.CommandValid = true
			msg.Command = rf.log[i].Command
			msg.CommandIndex = rf.lastApplied
			// log.Printf("[ApplyCommand]\t%d applied %v, with last applied %d, term %d, commitId %d\n", rf.me, msg.Command, rf.lastApplied, rf.log[i].Term, rf.commitIndex)
			rf.applyCh <- msg
		}
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	// defer rf.mu.Unlock()
	lastLogIndex, lastLogTerm := 0, 0
	if len(rf.log) > 0 {
		lastLogIndex = rf.log[len(rf.log)-1].Index
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId)) {
		if args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			rf.sstate = Follower
			reply.VoteGranted = true
			// update timer
			// log.Printf("[VoteGranted]\t%d voted for %d in term %d, with last log index %d, log term %d; %d's last log index %d, log term %d\n",
			// rf.me, args.CandidateId, args.Term, lastLogIndex, lastLogTerm, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
			rf.persist()
			rf.mu.Unlock()
			rf.leaderAlive <- 1
			return
		}
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	// log.Printf("[VoteRej]\t%d rejected %d in term %d, with last log index %d, log term %d; %d's last log index %d, log term %d\n",
	// rf.me, args.CandidateId, args.Term, lastLogIndex, lastLogTerm, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// example AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	Term          int
	Success       bool
	NextLogIndex  int
	MatchLogIndex int
}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	// defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.persist()
		rf.mu.Unlock()
		return
	}

	rf.sstate = Follower
	rf.currentTerm = args.Term
	// update log
	lastIndex := 0
	lastTerm := 0
	i := len(rf.log) - 1
	if i >= 0 {
		lastIndex = rf.log[i].Index
		lastTerm = rf.log[i].Term
	}
	// log.Printf("[AppendEntries]\t%d received AppendEntries from %d in term %d, with its lastId %d, lastTerm %d; leader's PrevId %d, PrevTerm %d",
	// rf.me, args.LeaderId, rf.currentTerm, lastIndex, lastTerm, args.PrevLogIndex, args.PrevLogTerm)
	if lastIndex > args.PrevLogIndex {
		// lastIndex != 0
		i -= lastIndex - args.PrevLogIndex
		// safe check
		/*
			k := 0
			for j := i + 1; rf.log[j].Index <= rf.lastApplied; j++ {
				if k >= len(args.Entries) || rf.log[j].Term != args.Entries[k].Term {
					log.Fatalf("overwrite applied log!")
				}
				k++
			} */
		if i >= 0 {
			lastIndex = rf.log[i].Index
			lastTerm = rf.log[i].Term
		} else {
			lastIndex, lastTerm = 0, 0
		}
	}
	if lastIndex == args.PrevLogIndex && lastTerm == args.PrevLogTerm {
		// safe to append
		reply.Success = true
		// rf.log = append(rf.log, args.Entries...)
		k := 0
		for i += 1; i < len(rf.log) && k < len(args.Entries) && args.Entries[k].Term == rf.log[i].Term; i, k = i+1, k+1 {
		}
		if k < len(args.Entries) {
			rf.log = append(rf.log[:i], args.Entries[k:]...)
			i += len(args.Entries) - k
		}

		reply.NextLogIndex = 1
		reply.MatchLogIndex = 0
		if len(rf.log) > 0 {
			reply.NextLogIndex = rf.log[len(rf.log)-1].Index + 1
			// reply.NextLogIndex = rf.log[i-1].Index + 1
			reply.MatchLogIndex = rf.log[i-1].Index
		}

		// already up to date with leader, update commit index
		if rf.commitIndex < args.LeaderCommit {
			if args.LeaderCommit < rf.log[i-1].Index {
				// alway choose this branch ?
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.log[i-1].Index
			}
			// apply commited logs
			rf.applyCommited()
		}
	} else {
		// last index < arg.PrevLogIndex or (last index == arg.PrevLogIndex and last Term < args.PrevLogTerm)
		reply.Success = false
		if lastIndex == args.PrevLogIndex {
			for ; (i >= 0) && (rf.log[i].Term == lastTerm) && (rf.log[i].Index > rf.lastApplied); i-- {
			}
		}
		reply.NextLogIndex = 1
		// reply.MatchLogIndex = 0
		if i >= 0 {
			reply.NextLogIndex = rf.log[i].Index + 1
		}
		// rf.log = rf.log[:i+1]
	}

	rf.persist()
	rf.mu.Unlock()
	// restart timer
	// log.Printf("[AppendHeartBeat] %d sending leaderAlive heartbeat", rf.me)
	rf.leaderAlive <- 1
	// log.Printf("[AppendHeartBeat] %d sent leaderAlive heartbeat", rf.me)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := 1
	if len(rf.log) > 0 {
		index = rf.log[len(rf.log)-1].Index + 1
	}
	term := rf.currentTerm
	isLeader := (rf.sstate == Leader)

	// Your code here (2B).
	if isLeader {
		// log.Printf("[NewCommand]\t%d received cmd %v\n", rf.me, command)
		entry := LogEntry{}
		entry.Index = index
		entry.Term = term
		entry.Command = command
		rf.log = append(rf.log, entry)
		rf.nextIndex[rf.me] += 1
		rf.matchIndex[rf.me] += 1
		rf.persist()
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// log.Printf("[FollowerHeartBeat] %d", rf.me)
		// Your code here (2A)
		// Check if a leader election should be started.
		newElection := make(chan int)
		go func() {
			// pause for a random amount of time between 1300 and 1600
			// milliseconds.
			ms := 100 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)
			newElection <- 1
		}()
		select {
		case <-rf.leaderAlive:
			continue
		case <-newElection:
			if rf.startElection() {
				go rf.leaderOp()
				return
			}
		}
	}
	// log.Printf("[KilledFromTicker] %d killed", rf.me)
}

func (rf *Raft) startElection() bool {
	for !rf.killed() {
		args := RequestVoteArgs{}
		args.CandidateId = rf.me

		rf.mu.Lock()
		rf.sstate = Candidate
		rf.currentTerm += 1
		rf.votedFor = rf.me
		args.Term = rf.currentTerm
		if len(rf.log) > 0 {
			args.LastLogIndex = rf.log[len(rf.log)-1].Index
			args.LastLogTerm = rf.log[len(rf.log)-1].Term
		}
		rf.persist()
		// log.Printf("[CandHeartBeat] %d of term %d", rf.me, rf.currentTerm)
		rf.mu.Unlock()

		voteCollected := 1
		voteCond := make(chan int)
		timeout := make(chan int)
		quit := make(chan int)
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(i int) {
					reply := RequestVoteReply{}
					ok := rf.sendRequestVote(i, &args, &reply)
					if ok {
						vc := -1
						if !reply.VoteGranted {
							vc = reply.Term
						}
						select {
						case <-quit:
							return
						case voteCond <- vc:
							return
						}
					}
				}(i)
			}
		}
		go func() {
			// pause for a random amount of time between 1300 and 1600
			// milliseconds.
			ms := 100 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)
			timeout <- 1
		}()
		for wait := true; wait; {
			select {
			case c := <-voteCond:
				if c == -1 {
					voteCollected += 1
					if voteCollected > len(rf.peers)/2 {
						// win
						close(quit)
						return true
					}
				} else {
					// found updated term
					close(quit)
					rf.mu.Lock()
					if c > rf.currentTerm {
						rf.currentTerm = c + 1
						rf.sstate = Follower
						rf.votedFor = -1
						rf.persist()
					}
					rf.mu.Unlock()
					return false
				}
			case <-rf.leaderAlive:
				close(quit)
				return false
			case <-timeout:
				// new election
				close(quit)
				wait = false
			}
		}
	}
	// log.Printf("[KilledFromCand] %d killed", rf.me)
	return false
}

func (rf *Raft) leaderOp() {
	rf.mu.Lock()
	// log.Printf("[NewLeader]\t%d become leader in term %d\n", rf.me, rf.currentTerm)
	rf.sstate = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	leaderTerm := rf.currentTerm
	termStartIndex := 1
	if len(rf.log) > 0 {
		termStartIndex = rf.log[len(rf.log)-1].Index + 1
	}
	for i := range rf.peers {
		rf.nextIndex[i] = termStartIndex
		// rf.matchIndex[i] = 0
		if rf.matchIndex[i] != 0 {
			log.Fatalf("not default match index!\n")
		}
	}
	rf.matchIndex[rf.me] = termStartIndex - 1
	rf.mu.Unlock()
	sortedMatchPeers := make([]int, len(rf.peers))
	for i := range sortedMatchPeers {
		sortedMatchPeers[i] = i
	}
	sortedMatchPeers[0], sortedMatchPeers[rf.me] = rf.me, 0
	for !rf.killed() {
		// log.Printf("[LeaderHeartBeat] %d", rf.me)
		timeout := make(chan int)
		appCond := make(chan int)
		quit := make(chan int)
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(i int) {
					args := AppendEntriesArgs{}
					args.LeaderId = rf.me
					rf.mu.Lock()
					if rf.sstate != Leader {
						rf.mu.Unlock()
						return
					}
					// term := rf.currentTerm
					args.Term = leaderTerm
					args.LeaderCommit = rf.commitIndex
					args.PrevLogIndex = rf.nextIndex[i] - 1
					args.PrevLogTerm = 0
					if len(rf.log) > 0 {
						offset := rf.log[0].Index
						if args.PrevLogIndex-offset >= 0 {
							args.PrevLogTerm = rf.log[args.PrevLogIndex-offset].Term
						}
						args.Entries = rf.log[args.PrevLogIndex-offset+1:]
					}
					rf.mu.Unlock()
					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntries(i, &args, &reply)
					if ok {
						if reply.Term > leaderTerm {
							select {
							case <-quit:
								return
							case appCond <- reply.Term:
								return
							}
						} else {
							rf.mu.Lock()
							defer rf.mu.Unlock()
							if rf.currentTerm != leaderTerm || reply.NextLogIndex < rf.matchIndex[i] {
								return
							}
							if !reply.Success {
								if reply.NextLogIndex < rf.nextIndex[i] {
									rf.nextIndex[i] = reply.NextLogIndex
								}
							} else {
								if reply.NextLogIndex > rf.nextIndex[i] {
									if reply.NextLogIndex > rf.nextIndex[rf.me] {
										rf.nextIndex[i] = rf.nextIndex[rf.me]
									} else {
										rf.nextIndex[i] = reply.NextLogIndex
									}
								}
								if reply.MatchLogIndex > rf.matchIndex[i] {
									rf.matchIndex[i] = reply.MatchLogIndex
									id := len(rf.peers) - 1
									for ; sortedMatchPeers[id] != i; id-- {
									}
									for id -= 1; reply.MatchLogIndex > rf.matchIndex[sortedMatchPeers[id]]; id-- {
										sortedMatchPeers[id], sortedMatchPeers[id+1] = sortedMatchPeers[id+1], sortedMatchPeers[id]
									}
								}
							}
						}
					}
				}(i)
			}
		}
		go func() {
			// pause for 50 milliseconds.
			//
			ms := 50
			time.Sleep(time.Duration(ms) * time.Millisecond)
			timeout <- 1
		}()
		select {
		case c := <-appCond:
			// found new term
			close(quit)
			rf.mu.Lock()
			if c > rf.currentTerm {
				rf.currentTerm = c
				rf.sstate = Follower
				rf.votedFor = -1
				rf.persist()
			}
			rf.mu.Unlock()
			// log.Printf("[ChangeLeader]\t%d giving up leader, found new term %d", rf.me, c)
			go rf.ticker()
			return
		case <-rf.leaderAlive:
			// already set to follower
			close(quit)
			// log.Printf("[ChangeLeader]\t%d giving up leader, found new leader alive", rf.me)
			go rf.ticker()
			return
		case <-timeout:
			// heartbeat
			close(quit)
		}
		rf.mu.Lock()
		if rf.currentTerm > leaderTerm {
			// has leader alive
			rf.mu.Unlock()
			// log.Printf("[LeaderTermChanged] %d: unexpected change of term", rf.me)
			go rf.ticker()
			return
		}
		// find the n/2+1's largest number
		n := rf.matchIndex[sortedMatchPeers[len(rf.peers)/2]]
		if n > rf.commitIndex && n >= termStartIndex {
			rf.commitIndex = n
			// apply commited logs
			rf.applyCommited()
		}
		rf.mu.Unlock()
	}
	// log.Printf("[LeaderKilled] %d killed", rf.me)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	// rf.commitIndex = 0
	// rf.lastApplied = 0
	// rf.currentTerm = 0

	rf.sstate = Follower
	rf.leaderAlive = make(chan int)

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
