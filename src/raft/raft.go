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
	"fmt"
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
	poll        int // count the votes (2A)

	timer int64 // last time hear from leader (2A)
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
		rf.currentTerm = args.Term
		if rf.state != Follower {
			rf.state = Follower
			rf.votedFor = -1
		}
	}

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || (rf.votedFor != -1 && rf.votedFor != args.CandidateID) {
		// term not qualified or already voted for another peer
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateID
	// reset timer when you grant a vote to another peer
	rf.timer = time.Now().UnixNano() / 1e6
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

func (rf *Raft) electAfterTimeout() {
	timeout := rand.Int63n(700) + 300
	curTime := time.Now().UnixNano() / 1e6

	if curTime-atomic.LoadInt64(&rf.timer) > timeout {
		rf.election()
	}
}

func (rf *Raft) election() {

	fmt.Printf("Server %d starting election\n", rf.me)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = Candidate

	rf.currentTerm++
	rf.votedFor = rf.me
	rf.poll = 1
	rf.timer = time.Now().UnixNano() / 1e6

	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateID = rf.me

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go rf.goSendRequestVote(i, args)
	}
}

func (rf *Raft) goSendRequestVote(server int, args RequestVoteArgs) {
	var reply RequestVoteReply
	if rf.sendRequestVote(server, &args, &reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			return
		}
		if reply.VoteGranted {
			rf.poll++
		}
		if rf.poll > len(rf.peers)/2 && rf.state == Candidate && rf.currentTerm == args.Term {
			rf.state = Leader
			fmt.Printf("Server %d win the election in term %d\n", rf.me, rf.currentTerm)
		}
		rf.mu.Unlock()
	}
}

//
type AppendEntriesArgs struct {
	Term int // leader's term (2A)
}

//
type AppendEntriesReply struct {
	Term    int // currentTerm, for leader to update itself (2A)
	Success bool
}

//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.timer = time.Now().UnixNano() / 1e6

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.state != Follower {
			rf.state = Follower
			rf.votedFor = -1
		}
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) heartBeatAfterTimeout() {
	timeout := 100 * time.Microsecond
	time.Sleep(timeout)
	rf.heartBeat()
}

func (rf *Raft) heartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var args AppendEntriesArgs
	args.Term = rf.currentTerm

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.goSendAppendEntries(i, args)
	}
}

func (rf *Raft) goSendAppendEntries(server int, args AppendEntriesArgs) {
	var reply AppendEntriesReply
	if atomic.LoadInt32(&rf.state) == Leader {
		rf.sendAppendEntries(server, &args, &reply)
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) mainLoop() {
	for {
		state := atomic.LoadInt32(&rf.state)
		switch state {
		case Follower:
			rf.electAfterTimeout()
		case Candidate:
			rf.electAfterTimeout()
		case Leader:
			rf.heartBeatAfterTimeout()
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
	rf.poll = -1
	rf.timer = time.Now().UnixNano() / 1e6
	fmt.Printf("Starting raft %d\n", me)
	go rf.mainLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
