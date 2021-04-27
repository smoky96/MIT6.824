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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Raft state
const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

// LogEntry ...
type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state int32 // Raft state: Follower, Candidate, Leader (2A)

	currentTerm int // latest term server has seen (2A)
	votedFor    int // cnadidateId that received vote in current term (2A)

	timerElectionChan       chan bool // timer for election (2A)
	timerHeartbeatChan      chan bool // timer for heartbeat (2A)
	lastResetElectionTimer  int64
	lastResetHeartbeatTimer int64
	timeoutHeartbeat        int64
	timeoutElection         int64

	log         []LogEntry    // log entries (2B)
	nextIndex   []int         // for each server, index of the next log entry to send to that server (2B)
	matchIndex  []int         // for each server, index of highest log entry known to be replicated on server (2B)
	commitIndex int           // index of highest log entry known to be commited (2B)
	lastApplied int           // index of highest log entry applied to state machine (2B)
	applyCh     chan ApplyMsg // channel to send commit (2B)
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
	isleader = rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // candidate's term (2A)
	CandidateID int // candidate requesting vote (2A)

	LastLogIndex int // index of candidate's last log entry (2B)
	LastLogTerm  int // term of candidate's last log entry (2B)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate recieved vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.convertTo(Follower)
		rf.currentTerm = args.Term
	}

	// term not qualified or already voted for another peer
	if args.Term < rf.currentTerm || (rf.votedFor != -1 && rf.votedFor != args.CandidateID) {
		DPrintf("[RequestVote] raft %d reject vote for %d | current term: %d | current state: %d | recieved term: %d\n",
			rf.me, args.CandidateID, rf.currentTerm, rf.state, args.Term)

		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		// grant vote if candidate's log is at least as up-to-data as me log
		lastLogIndex := len(rf.log) - 1
		if rf.log[lastLogIndex].Term > args.LastLogTerm ||
			(rf.log[lastLogIndex].Term == args.LastLogTerm && len(rf.log)-1 > lastLogIndex) {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}

		DPrintf("[RequestVote] raft %d accept vote for %d | current term: %d | current state: %d | recieved term: %d\n",
			rf.me, args.CandidateID, rf.currentTerm, rf.state, args.Term)

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		// reset timer when you grant a vote to another peer
		rf.resetTimerElection()
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.convertTo(Candidate)
	nVote := 1

	DPrintf("[startElection] raft %d start election | current term: %d | current state: %d \n",
		rf.me, rf.currentTerm, rf.state)

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(id int) {

			rf.mu.Lock()
			lastLogIndex := len(rf.log) - 1
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateID:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  rf.log[lastLogIndex].Term,
			}
			rf.mu.Unlock()

			var reply RequestVoteReply

			if rf.sendRequestVote(id, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.currentTerm != args.Term {
					return
				}

				if reply.VoteGranted {
					nVote += 1

					DPrintf("[requestVoteAsync] raft %d get accept vote from %d | current term: %d | current state: %d | reply term: %d | poll %d\n",
						rf.me, id, rf.currentTerm, rf.state, reply.Term, nVote)

					if nVote > len(rf.peers)/2 && rf.state == Candidate {
						rf.convertTo(Leader)
						DPrintf("[requestVoteAsync] raft %d convert to leader | current term: %d | current state: %d\n",
							rf.me, rf.currentTerm, rf.state)

						// reinitialize after election
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = len(rf.log)
							rf.matchIndex[i] = 0
						}

						rf.mu.Unlock()
						rf.braodcastHeartbeat()
						rf.mu.Lock()
					}
				} else {
					DPrintf("[requestVoteAsync] raft %d get reject vote from %d | current term: %d | current state: %d | reply term: %d | poll %d\n",
						rf.me, id, rf.currentTerm, rf.state, reply.Term, nVote)

					if rf.currentTerm < reply.Term {
						rf.convertTo(Follower)
						rf.currentTerm = reply.Term
					}
				}
			} else {
				DPrintf("[requestVoteAsync] raft %d RPC to %d failed | current term: %d | current state: %d | reply term: %d\n",
					rf.me, id, rf.currentTerm, rf.state, reply.Term)
			}
		}(i)
	}

}

//
type AppendEntriesArgs struct {
	Term int // leader's term (2A)

	PrevLogIndex int        // index of log entry immediately preceding new ones (2B)
	PrevLogTerm  int        // term of PrevLogIndex entry (2B)
	Entries      []LogEntry // log entries to store (2B)
	LeaderCommit int        // leader's commitIndex (2B)
}

//
type AppendEntriesReply struct {
	Term    int // currentTerm, for leader to update itself (2A)
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("[AppendEntries] raft %d reject append entries | current term: %d | current state: %d | recieved term: %d\n",
			rf.me, rf.currentTerm, rf.state, args.Term)
		// do not reset timer here
		return
	}

	rf.resetTimerElection()

	if args.Term > rf.currentTerm {
		rf.convertTo(Follower)
		rf.currentTerm = args.Term
		DPrintf("[AppendEntries] raft %d update term | current term: %d | current state: %d | recieved term: %d\n",
			rf.me, rf.currentTerm, rf.state, args.Term)
	} else if rf.state == Candidate {
		rf.state = Follower
		DPrintf("[AppendEntries] raft %d update state | current term: %d | current state: %d | recieved term: %d\n",
			rf.me, rf.currentTerm, rf.state, args.Term)
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[AppendEntries] raft %d reject append entry | log len: %d | args.PrevLogIndex: %d | args.prevLogTerm %d\n",
			rf.me, len(rf.log), args.PrevLogIndex, args.PrevLogTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		// If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it

		// If the follower has all the entries the leader sent, the follower MUST NOT truncate its log.
		// Any elements following the entries sent by the leader MUST be kept.
		isMatch := true
		nextIndex := args.PrevLogIndex + 1
		conflictIndex := 0
		logLen := len(rf.log)
		entLen := len(args.Entries)
		for i := 0; isMatch && i < entLen; i++ {
			if ((logLen - 1) < (nextIndex + i)) || rf.log[nextIndex+i].Term != args.Entries[i].Term {
				isMatch = false
				conflictIndex = i
				break
			}
		}

		if !isMatch {
			rf.log = append(rf.log[:nextIndex+conflictIndex], args.Entries[conflictIndex:]...)
			DPrintf("[AppendEntries] raft %d appended entries from leader | log length: %d\n", rf.me, len(rf.log))
		}

		lastNewEntryIndex := args.PrevLogIndex + entLen
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, lastNewEntryIndex)
			go rf.applyEntries() // apply entries after update commitIndex
		}

		reply.Term = rf.currentTerm
		reply.Success = true
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) braodcastHeartbeat() {
	rf.mu.Lock()

	if rf.state != Leader {
		DPrintf("[broadcastHeartbeat] raft %d lost leadership | current term: %d | current state: %d\n",
			rf.me, rf.currentTerm, rf.state)
		rf.mu.Unlock()
		return
	}
	rf.resetTimerHeartbeat()

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(id int) {

		retry:

			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				PrevLogIndex: rf.nextIndex[id] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[id]-1].Term,
				Entries:      rf.log[rf.nextIndex[id]:], // send AppendEntries RPC with log entries starting at nextIndex
				LeaderCommit: rf.commitIndex,
			}

			rf.mu.Unlock() // should unlock while waiting reply, otherwise: deadlock

			// check leadership
			if _, isLeader := rf.GetState(); !isLeader {
				return
			}

			var reply AppendEntriesReply
			if rf.sendAppendEntries(id, &args, &reply) {
				rf.mu.Lock()
				// defer rf.mu.Unlock()

				if rf.state != Leader {
					// check state wheather changed during broadcasting
					DPrintf("[appendEntriesAsync] raft %d lost leadership | current term: %d | current state: %d\n",
						rf.me, rf.currentTerm, rf.state)
					rf.mu.Unlock()
					return
				}

				if rf.currentTerm != args.Term {
					DPrintf("[appendEntriesAsync] raft %d term inconsistency | current term: %d | current state: %d | args term %d\n",
						rf.me, rf.currentTerm, rf.state, args.Term)
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					DPrintf("[appendEntriesAsync] raft %d append entries to %d accepted | current term: %d | current state: %d\n",
						rf.me, id, rf.currentTerm, rf.state)
					rf.matchIndex[id] = args.PrevLogIndex + len(args.Entries) // index of highest log entry known to be replicated
					rf.nextIndex[id] = rf.matchIndex[id] + 1                  // index of next log entry to send
					rf.checkN()
				} else {
					DPrintf("[appendEntriesAsync] raft %d append entries to %d rejected | current term: %d | current state: %d | reply term: %d\n",
						rf.me, id, rf.currentTerm, rf.state, reply.Term)
					if reply.Term > rf.currentTerm {
						rf.convertTo(Follower)
						rf.currentTerm = reply.Term
						rf.mu.Unlock()
						return
					}
					rf.nextIndex[id] -= 1
					DPrintf("[appendEntriesAsync] raft %d append entries to %d rejected: decrement nextIndex and retry | nextIndex: %d\n",
						rf.me, id, rf.nextIndex[id])
					rf.mu.Unlock()
					goto retry
				}
				rf.mu.Unlock()
			} else {
				DPrintf("[appendEntriesAysnc] raft %d RPC to %d failed | current term: %d | current state: %d | reply term: %d\n",
					rf.me, id, rf.currentTerm, rf.state, reply.Term)
			}
		}(i)
	}
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
// call with lock
func (rf *Raft) checkN() {
	for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
		nReplicated := 0
		for i := 0; i < len(rf.peers); i++ {
			// DPrintf("----------raft %d: | matchIndex: %d | peer len: %d\n", j, rf.matchIndex[j], len(rf.peers))
			if rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
				nReplicated += 1
			}
			if nReplicated > len(rf.peers)/2 {
				rf.commitIndex = N
				go rf.applyEntries()
				break
			}
		}
		// DPrintf("----------raft %d: | nReplicated: %d | peer len: %d\n", j, nReplicated, len(rf.peers))
	}
}

func (rf *Raft) applyEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
		rf.applyCh <- applyMsg
		rf.lastApplied += 1
		DPrintf("[applyEntries] raft %d applied entry | lastApplied: %d | commitIndex: %d\n",
			rf.me, rf.lastApplied, rf.commitIndex)
	}
}

// call with lock
func (rf *Raft) convertTo(state int32) {
	switch state {
	case Follower:
		// sofar, every time convert to follower comes with term change,
		// which need to set votedFor to -1
		rf.votedFor = -1
		rf.state = Follower
	case Candidate:
		rf.state = Candidate
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.resetTimerElection()
	case Leader:
		rf.state = Leader
		rf.resetTimerHeartbeat()
	}
}

func (rf *Raft) timerElection() {
	for {
		rf.mu.Lock()

		if rf.state != Leader {
			timeElapsed := (time.Now().UnixNano() - rf.lastResetElectionTimer) / time.Hour.Milliseconds()
			if timeElapsed > rf.timeoutElection {
				DPrintf("[timerElection] raft %d election timeout | current term: %d | current state: %d\n",
					rf.me, rf.currentTerm, rf.state)
				rf.timerElectionChan <- true
			}
		}

		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) timerHeartbeat() {
	for {
		rf.mu.Lock()

		if rf.state == Leader {
			timeElapsed := (time.Now().UnixNano() - rf.lastResetHeartbeatTimer) / time.Hour.Milliseconds()
			if timeElapsed > rf.timeoutHeartbeat {
				DPrintf("[timerHeartbeat] raft %d heartbeat timeout | current term: %d | current state: %d\n",
					rf.me, rf.currentTerm, rf.state)
				rf.timerHeartbeatChan <- true
			}
		}

		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) resetTimerElection() {
	rand.Seed(time.Now().UnixNano())
	rf.timeoutElection = rf.timeoutHeartbeat*5 + rand.Int63n(150)
	rf.lastResetElectionTimer = time.Now().UnixNano()
}

func (rf *Raft) resetTimerHeartbeat() {
	rf.lastResetHeartbeatTimer = time.Now().UnixNano()
}

func (rf *Raft) mainLoop() {
	for !rf.killed() {
		select {
		case <-rf.timerElectionChan:
			rf.startElection()
		case <-rf.timerHeartbeatChan:
			rf.braodcastHeartbeat()
		}
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if term, isLeader = rf.GetState(); isLeader {
		rf.mu.Lock()
		rf.log = append(rf.log, LogEntry{command, rf.currentTerm})
		rf.matchIndex[rf.me] = rf.nextIndex[rf.me] + len(rf.log) - 1
		index = len(rf.log) - 1
		DPrintf("[Start] raft %d replicate command to log | current term: %d | current state: %d | log length: %d\n",
			rf.me, rf.currentTerm, rf.state, len(rf.log))
		rf.mu.Unlock()
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.timerHeartbeatChan = make(chan bool)
	rf.timerElectionChan = make(chan bool)
	rf.timeoutHeartbeat = 100 // ms
	rf.resetTimerElection()

	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	DPrintf("Starting raft %d\n", me)
	go rf.mainLoop()
	go rf.timerElection()
	go rf.timerHeartbeat()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
