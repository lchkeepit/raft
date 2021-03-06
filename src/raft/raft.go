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

import "sync"
import (
	"labrpc"
	"math/rand"
	"time"
	//"fmt"
	//"log"
)

// import "bytes"
// import "encoding/gob"


//TIMERANGE
const (
	ELECT_TIMEOUT_MIN int = 150 //ms
	ELECT_TIMEOUT_MAX int = 300 //ms
	HEART_BEAT_TIMEDURATION int = 50
)

func getElectTimeout() int {
	return rand.Intn(ELECT_TIMEOUT_MAX - ELECT_TIMEOUT_MIN) * 2 + ELECT_TIMEOUT_MAX
}
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//server log struct
type Msg struct {
	Index   int
	Term    int
	command interface{}
}

type Identity int

const (
	CANDIDATE Identity = 0
	FOLLOWER  Identity = 1
	LEADER    Identity = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	electTimeout int
	timeDuration int
	voteCnt      int

	status Identity

	currentTerm int
	votedFor    int
	log         []Msg

	commitIndex int
	lastApplied int

	nextIndex  []int //next msg index to send to ith peer server
	matchIndex []int //already sent msg index
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.status == LEADER
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
	Term   int
}

////append entries
type RequestAppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	entries []Msg
	LeaderCommit int
}

type RequestAppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = true

	if (args.Term < rf.currentTerm){
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		rf.currentTerm = args.Term
		rf.becomeFollower()
	}
	DPrintf("[RequestAppendEntries] %s %d (votefor %d) timeDurant %d electTimeout %d\n",
		reply.Success, rf.me, rf.votedFor, rf.timeDuration, rf.electTimeout)

	// not only heartbeat
	if (len(args.entries) != 0){

	}
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = true
	CPrintf("[RequestVote] RequestVoteArgs %+v", args)

	if args.Term < rf.currentTerm {
		//DPrintf("%d refuse voteRequest from %d", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if (args.Term > rf.currentTerm) {
		rf.votedFor = -1
		rf.becomeFollower()
	} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId{
		DPrintf("%d (votefor %d) refuse voteRequest from %d\n", rf.me, rf.votedFor, args.CandidateId)
		reply.VoteGranted = false
	}

	if reply.VoteGranted {
		CPrintf("%d term %d (votefor %d) accept voteRequest from %d term %d \n",
			rf.me, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term)
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.becomeFollower()
		CPrintf("%d become follower with electTimeout %d timeDuration %d", rf.me, rf.electTimeout, rf.timeDuration)
	} else {
		CPrintf("%d term %d (votefor %d) refuse voteRequest from %d term %d \n",
			rf.me, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term)
	}
	DPrintf("%d (votefor %d) timeDurant %d electTimeout %d\n", rf.me, rf.votedFor, rf.timeDuration, rf.electTimeout)
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
func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) broadcaseRequestAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()


	//var w sync.WaitGroup
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		logsSent := rf.getAppendLogs(i)
		lastIndex := rf.getLastIndex(i)
		lastTerm := rf.getLastTerm(i)

		args := &RequestAppendEntriesArgs{
			Term:rf.currentTerm,
			LeaderId:rf.me,
			entries:logsSent,
			PrevLogIndex:lastIndex,
			PrevLogTerm:lastTerm,
			LeaderCommit: rf.commitIndex,
		}
		replyList := [] *RequestAppendEntriesReply{}

		reply := &RequestAppendEntriesReply{}
		replyList = append(replyList, reply)

		go func(i int) {
			rf.sendRequestAppendEntries(i, args, reply)
		}(i)
	}
}

func (rf *Raft) heartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//CPrintf("[heartbeatRequest] from %d\n", rf.me)
	args := &RequestAppendEntriesArgs{Term:rf.currentTerm, LeaderId:rf.me}
	replyList := [] *RequestAppendEntriesReply{}

	//var w sync.WaitGroup
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		reply := &RequestAppendEntriesReply{}
		replyList = append(replyList, reply)
		//w.Add(1)

		go func(i int) {
			//defer w.Done()
			//CPrintf("%d Send heartbeatRequest to %d\n", rf.me, i)
			rf.sendRequestAppendEntries(i, args, reply)
		}(i)
	}
	//w.Wait()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	CPrintf("[startElection] %d timeduration %d\n", rf.me, rf.timeDuration)
	args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
	rf.timeDuration = 0
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.mu.Unlock()

	replyList := [] *RequestVoteReply{}

	rf.voteCnt = 0
	if rf.votedFor == rf.me {
		rf.voteCnt = 1
	}
	//var w sync.WaitGroup
	for i := range rf.peers {
		if i == rf.me || rf.status == FOLLOWER{
			continue
		}
		reply := &RequestVoteReply{}
		replyList = append(replyList, reply)


		//w.Add(1)
		go func(i int) {
			//defer w.Done()
			ok := rf.sendRequestVote(i, args, reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()


				if (reply.Term > rf.currentTerm) {
					rf.currentTerm = reply.Term
					rf.becomeFollower()
				}
				if (reply.VoteGranted) {
					rf.voteCnt ++
				}

				// it vote for itself
				if rf.status == CANDIDATE && rf.voteCnt > len(rf.peers)/2 {
					rf.becomeLeader()
				}
			}
		}(i)
	}
	//w.Wait()
	//CPrintf("[endElection] %d Term %d with %d votes over %d peers\n", rf.me, rf.currentTerm, rf.voteCnt, len(rf.peers))
}


//append log funcs
func (rf *Raft) getAppendLogs(peerId int) []Msg {
	index := rf.nextIndex[peerId]
	return rf.log[index:]
}

func (rf *Raft) getLastLog() Msg {
	length := len(rf.log)
	return rf.log[length - 1]
}

func (rf *Raft) getLastIndex(index int) int {
	if index <= 0 {
		return -1
	}
	return rf.log[index - 1].Index
}

func (rf *Raft) getLastTerm(index int) int {
	if index <= 0 {
		return -1
	}
	return rf.log[index - 1].Term
}

// handle as different role
func (rf *Raft) handleCandidate() {
	if rf.timeDuration >= rf.electTimeout {
		rf.startElection()
	}
}
func (rf *Raft) handleFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.timeDuration >= rf.electTimeout{
		CPrintf("[debug] %d timeDurant %d electTimeout %d \n", rf.me, rf.timeDuration, rf.electTimeout)
		rf.becomeCandidate()
	}
}
func (rf *Raft) handleLeader() {
	if rf.timeDuration >= HEART_BEAT_TIMEDURATION {
		rf.heartBeat()
		//rf.broadcaseRequestAppendEntries()
		rf.timeDuration = 0
	}
}

func (rf *Raft) raftProcess() {
	for true {
		switch rf.status{
			case FOLLOWER:
				rf.handleFollower()
				break
			case CANDIDATE:
				rf.handleCandidate()
				break
			case LEADER:
				rf.handleLeader()
				break
		}
		time.Sleep(time.Millisecond)
		rf.timeDuration += 1;
	}
}



// init as different role
func (rf *Raft) becomeCandidate() {
	rf.status = CANDIDATE
	CPrintf("%d become candidate with term %d", rf.me, rf.currentTerm)
}

func (rf *Raft) becomeFollower() {
	rf.status = FOLLOWER
	rf.timeDuration = 0
	rf.electTimeout = getElectTimeout()
	//CPrintf("%d become follower with electTimeout %d", rf.me, rf.electTimeout)
}

func (rf *Raft) becomeLeader() {
	CPrintf("%d become leader\n", rf.me)
	rf.status = LEADER
	rf.votedFor = rf.me
	//rf.heartBeat()
	rf.timeDuration = 0

	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.matchIndex[i] = -1
		rf.nextIndex[i] = rf.getLastLog().Index
	}

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf.currentTerm = 0
	rf.votedFor = -1
	//fmt.Printf("%d raft start\n", rf.me)
	// Your initialization code here (2A, 2B, 2C).
	rf.becomeFollower()
	DPrintf("%d electTimeout %d\n", rf.me, rf.electTimeout)
	go rf.raftProcess()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
