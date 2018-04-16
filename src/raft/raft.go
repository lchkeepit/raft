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
	"fmt"
	//"math"
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
	//return rand.Intn(ELECT_TIMEOUT_MAX - ELECT_TIMEOUT_MIN) * 2 + ELECT_TIMEOUT_MAX
	return int(rand.Int63() % 333 + 550)
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
	Command interface{}
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

	//message channel
	heartBeatChannel chan bool
	grantVoteChannel chan bool
	leaderChannel chan bool
	applyChannel       chan ApplyMsg
	commitChannel chan bool

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

//get status
func (rf *Raft) GetStateInfo() (int, string) {

	var term int
	var status string
	// Your code here (2A).
	term = rf.currentTerm
	if rf.status == LEADER {
		status = "LEADER"
	} else if rf.status == CANDIDATE {
		status = "CANDIDATE"
	} else {
		status = "FOLLOWER"
	}

	return term, status
}

func (rf *Raft) PrintStateInfo(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply)  {

	Printf_2B("%d term %d log %+v\n", rf.me, rf.currentTerm, rf.log)
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
	Entries []Msg
	LeaderCommit int
}

type RequestAppendEntriesReply struct {
	Term int
	NextIndex int
	Success bool
}

func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = true

	if (args.Term < rf.currentTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NextIndex = rf.getLastLog().Index + 1
		return
	}
	rf.heartBeatChannel <- true
	if (args.Term > rf.currentTerm) {
		rf.currentTerm = args.Term
		rf.becomeFollower()
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm

	if !rf.logExist(args.PrevLogIndex){
		Printf_2B("[RequestAppendEntries] %d reject args %+v\n", rf.me, args)
		reply.Success = false
		reply.NextIndex = rf.getLastLog().Index + 1
		Printf_2B("[RequestAppendEntries] %d reject reply %+v\n", rf.me, reply)
		return
	}

	index := args.PrevLogIndex - rf.log[0].Index
	//Printf_2B("[commit_follower] %d index %d\n", rf.me, index)
	//rf.logExist(index)

	if rf.log[index].Term != args.PrevLogTerm{
		//conflict logs
		tTerm := rf.log[index].Term
		for i := index; i >= 0; i-- {
			if rf.log[i].Term != tTerm {
				reply.NextIndex = i + 1
				break
			}
		}
		// log conflict end and return without other operations
		reply.Success = false
		Printf_2B("[RequestAppendEntries] %d reject reply %+v\n", rf.me, reply)
		return
	} else {
		rf.log = rf.log[:index + 1]
		for i := range args.Entries {
			rf.log = append(rf.log, args.Entries[i])
		}
		reply.NextIndex = rf.getLastLog().Index + 1
		Printf_2B("[RequestAppendEntries] %d append log %+v\n", rf.me, rf.log)
	}

	if reply.Success {
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.getLastLog().Index)
			rf.commitChannel <- true
			Printf_2B("[commit_follower] %d term %d commitIndex %d log %+v\n", rf.me, rf.currentTerm, rf.commitIndex, rf.log)
			Printf_2B("[commit_follower] args %+v\n", args)
		}
		rf.votedFor = args.LeaderId
		rf.becomeFollower()
	} else {
		Printf_2B("[RequestAppendEntries] %d term %d reject args %+v\n", rf.me, rf.currentTerm, args)
		Printf_2B("[RequestAppendEntries] %d log %+v\n", rf.me, rf.log)
	}
	//Printf_2B("[RequestAppendEntries] reply.Success %v %d (votefor %d) timeDurant %d electTimeout %d\n", reply.Success, rf.me, rf.votedFor, rf.timeDuration, rf.electTimeout)
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	CPrintf("%d term %d receive voteRequest %v from %d term %d\n", rf.me, rf.currentTerm, args, args.CandidateId, args.Term)
	reply.VoteGranted = true
	CPrintf("[RequestVote] RequestVoteArgs %+v", args)

	if args.Term < rf.currentTerm {
		//DPrintf("%d refuse voteRequest from %d", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if (args.Term > rf.currentTerm) {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.becomeFollower()
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId{
		CPrintf("%d (votefor %d) refuse voteRequest from %d\n", rf.me, rf.votedFor, args.CandidateId)
		reply.VoteGranted = false
	}

	if args.LastLogTerm < rf.getLastLog().Term || args.LastLogTerm == rf.getLastLog().Term && args.LastLogIndex < rf.getLastLog().Index {
		reply.VoteGranted = false
	}

	if reply.VoteGranted {
		Printf_2B("%d term %d log %+v accept voteRequest from %+v \n",
			rf.me, rf.currentTerm, rf.log, args)
		rf.votedFor = args.CandidateId
		rf.becomeFollower()
		//rf.currentTerm = args.Term

		rf.grantVoteChannel <- true
		CPrintf("%d become follower with electTimeout %d timeDuration %d", rf.me, rf.electTimeout, rf.timeDuration)
	} else {
		Printf_2B("%d term %d lastLog %+v  refuse voteRequest %+v \n", rf.me, rf.currentTerm, rf.getLastLog(), args)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.status != LEADER {
			return ok
		}
		if args.Term != rf.currentTerm {
			return ok
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.becomeFollower()
			return ok
		}

		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			} else {
				rf.nextIndex[server] = reply.NextIndex
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			rf.nextIndex[server] = reply.NextIndex
		}
	}
	return ok
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) updateCommit() {
	lastIndex := rf.getLastLog().Index

	newCommitIndex := rf.commitIndex

	for index := rf.commitIndex + 1; index <= lastIndex; index ++ {
		calcNum := 1
		for j := range(rf.peers) {
			if j != rf.me && rf.matchIndex[j] >= index && rf.log[index].Term == rf.currentTerm{
				calcNum += 1
			}
		}
		if 2 * calcNum > len(rf.peers) {
			newCommitIndex = index
		}
	}

	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		Printf_2B("[commit] %d commitIndex %d log %+v\n", rf.me, rf.commitIndex, rf.log)
		rf.commitChannel <- true
	}
}

func (rf *Raft) broadcaseRequestAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Printf_2B("[broadcaseRequestAppendEntries]%d start\n", rf.me)
	baseIndex := rf.log[0].Index

	rf.updateCommit()
	//var w sync.WaitGroup
	for i := range rf.peers {
		if i == rf.me || rf.status != LEADER {
			continue
		}

		prevIndex := rf.getPrevIndex(i)
		prevTerm := rf.getPrevTerm(i)

		args := &RequestAppendEntriesArgs{
			Term:rf.currentTerm,
			LeaderId:rf.me,
			PrevLogIndex:prevIndex,
			PrevLogTerm:prevTerm,
			LeaderCommit: rf.commitIndex,
		}
		args.Entries = make([]Msg, len(rf.log[prevIndex + 1 - baseIndex:]))
		copy(args.Entries, rf.log[prevIndex + 1 - baseIndex:])

		reply := &RequestAppendEntriesReply{}

		go func(i int) {
			//Printf_2B("[broadcaseRequestAppendEntries]%d send to %d arg %+v\n", rf.me, i, args)
			rf.sendRequestAppendEntries(i, args, reply)
			//Printf_2B("[broadcaseRequestAppendEntries2]%d send to %d arg %+v\n", rf.me, i, args)
			//Printf_2B("[broadcaseRequestAppendEntries]%d term %d status %+v get reply %+v from %d \n", rf.me, rf.currentTerm, rf.status, reply, i)
		}(i)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.voteCnt = 1
	args := &RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex:rf.getLastLog().Index,
		LastLogTerm:rf.getLastLog().Term,
	}
	Printf_2B("[startElection] %d term %d\n", rf.me, rf.currentTerm)
	rf.mu.Unlock()

	//var w sync.WaitGroup
	for i := range rf.peers {
		if i == rf.me || rf.status == FOLLOWER{
			continue
		}
		reply := &RequestVoteReply{}

		//w.Add(1)
		go func(i int) {
			//defer w.Done()
			Printf_2B("[sentVote] %d to %d\n", rf.me, i)
			ok := rf.sendRequestVote(i, args, reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if (reply.Term > rf.currentTerm) {
					rf.currentTerm = reply.Term
					rf.becomeFollower()
					return
				}
				if (reply.VoteGranted) {
					rf.voteCnt ++
				}

				// it vote for itself
				if rf.status == CANDIDATE && rf.voteCnt > len(rf.peers)/2 {
					rf.leaderChannel <- true
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
	if index >= len(rf.log) {
		//empty log, heartbeat
		var msgList []Msg
		return append(msgList, Msg{Term:rf.currentTerm})
	} else {
		return rf.log[index:]
	}
}

func (rf *Raft) logExist(index int) bool {
	baseIndex := rf.log[0].Index
	if index > baseIndex + len(rf.log) - 1 {
		return false
	} else {
		return true
	}
}

func (rf *Raft) getLastLog() Msg {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) getPrevIndex(peerId int) int {
	return rf.nextIndex[peerId] - 1
}

func (rf *Raft) getPrevTerm(peerId int) int {
	index := rf.nextIndex[peerId] - rf.log[0].Index - 1
	return rf.log[index].Term
}

func (rf *Raft) handleFollowerV2() {
	//CPrintf("%d handle follower\n", rf.me)
	select {
		case <- rf.heartBeatChannel:
		case <- rf.grantVoteChannel:
		case <- time.After(time.Duration(getElectTimeout()) * time.Millisecond):
			rf.becomeCandidate()
	}
}
func (rf *Raft) handleCandidateV2() {
	CPrintf("%d handle candidate\n", rf.me)
	go rf.startElection()
	select {
		case <- time.After(time.Duration(getElectTimeout()) * time.Millisecond):
		case <- rf.leaderChannel:
			rf.becomeLeader()
		case <- rf.heartBeatChannel:
			rf.becomeFollower()
	}
}
func (rf *Raft) handleLeaderV2() {
	rf.broadcaseRequestAppendEntries()
	time.Sleep(time.Duration(HEART_BEAT_TIMEDURATION) * time.Millisecond)
}

func (rf *Raft) raftProcessV2() {
	CPrintf("%d raftProcess Start\n", rf.me)
	for true {
		switch rf.status {
		case FOLLOWER:
			rf.handleFollowerV2()
		case CANDIDATE:
			rf.handleCandidateV2()
		case LEADER:
			rf.handleLeaderV2()
		}
	}
}

func (rf *Raft) commitProcess() {
	for {
		select {
		case <-rf.commitChannel:
			rf.mu.Lock()
			commitIndex := rf.commitIndex
			baseIndex := rf.log[0].Index
			for i := rf.lastApplied+1; i <= commitIndex; i++ {
				msg := ApplyMsg{Index: i, Command: rf.log[i-baseIndex].Command}
				rf.applyChannel <- msg
				rf.lastApplied = i
			}
			rf.mu.Unlock()
		}
	}
}

// init as different role
func (rf *Raft) becomeCandidate() {
	rf.status = CANDIDATE
	Printf_2B("%d become candidate with term %d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) becomeFollower() {
	rf.status = FOLLOWER

	//CPrintf("%d become follower with electTimeout %d", rf.me, rf.electTimeout)
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Printf_2B("%d become leader\n", rf.me)
	rf.status = LEADER
	rf.votedFor = rf.me

	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.matchIndex[i] = -1
		rf.nextIndex[i] = rf.getLastLog().Index + 1
		//Printf_2B("[becomeLeader] rf.nextIndex[%d] = %d\n", i, rf.nextIndex[i])
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.status == LEADER

	// Your code here (2B).
	if isLeader {
		Printf_2B("[Start] id %d command %+v log %+v\n", rf.me, command, rf.log)
		index = rf.getLastLog().Index + 1
		rf.log = append(rf.log, Msg{Term:term, Index:index, Command:command})
	}
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
func (rf *Raft) init() {
	rf.log = append(rf.log, Msg{Term:0, Index:0})
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.grantVoteChannel = make(chan bool, 100)
	rf.leaderChannel = make(chan bool, 100)
	rf.heartBeatChannel = make(chan bool, 100)
	rf.commitChannel = make(chan bool, 100)
}



func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyChannel = applyCh
	rf.init()
	fmt.Printf("%d raft start\n", rf.me)
	// Your initialization code here (2A, 2B, 2C).
	rf.becomeFollower()
	DPrintf("%d electTimeout %d\n", rf.me, rf.electTimeout)
	go rf.raftProcessV2()
	go rf.commitProcess()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
