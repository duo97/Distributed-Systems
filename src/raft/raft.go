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
	//	"bytes"
<<<<<<< HEAD

	"bytes"
	"log"
	"math/rand"
=======
>>>>>>> d94ea1678082307cb5bb6a36f6cd8eb0c117614a
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
<<<<<<< HEAD
	"6.824/labgob"
=======
>>>>>>> d94ea1678082307cb5bb6a36f6cd8eb0c117614a
	"6.824/labrpc"
)

const (
	HeartbeatInterval = time.Millisecond * 150
	Electionwindow    = time.Second * 2
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//persistent state
	currentTerm int
	votedFor    int
	logs        []logEntry
	// Volatile state
	commitIndex int
	lastApplied int
	lastLogTerm int
	//leader volatile state
	nextIndex  []int
	matchIndex []int
	//other 2A
	//for all servers
	currentRole   string //follower,leader or candidate
	lastHeartbeat time.Time
	hbTimeout     time.Duration
	msgChan       chan ApplyMsg
	//for candidate
	Npeers        int
	NVote         int
	electionStart time.Time
	eTimeout      time.Duration
	//
	leaderId int
}

type logEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	if rf.currentRole == "leader" {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm
	// fmt.Printf("getstate server %v, isleader %v interm %v\n", rf.me, isleader, term)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// var currentTerm int
	// var votedFor int
	// var logs []logEntry
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil || d.Decode(&rf.logs) != nil {
		log.Fatal("Persistor decoding failed!")
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//2A
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Voted bool
	Term  int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	defer rf.persist()
	// Your code here (2A, 2B).
	t := args.Term
	// fmt.Printf("server %v recieved in term %v a vote request for term%v\n", rf.me, rf.currentTerm, t)
	rf.updateTerm(t)
	reply.Term = rf.currentTerm
	reply.Voted = false
	if rf.votedFor == -1 && rf.currentTerm <= t {
		if (args.LastLogIndex >= len(rf.logs) && args.LastLogTerm == rf.lastLogTerm) || args.LastLogTerm > rf.lastLogTerm {
			rf.votedFor = args.CandidateId
			reply.Voted = true

			// fmt.Printf("s%v voting for %v,in term%v cand's lastlogterm is %v folLastLogTerm:%v\ncandLastLogindex:%v follower's LastlogIndex %v\n", rf.me, args.CandidateId, t, args.LastLogTerm, rf.lastLogTerm, args.LastLogIndex, len(rf.logs))

		}
	}
	return
}

func (rf *Raft) updateTerm(t int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if t > rf.currentTerm {

		rf.currentTerm = t
		rf.currentRole = "follower"
		rf.votedFor = -1

		// fmt.Printf("server%v(%v) updating its current term to%v\n", rf.me, rf.currentRole, t)

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
	defer rf.persist()
	// fmt.Printf("Inside start function before locking \n")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := false
	index := len(rf.logs) + 1
	term := rf.currentTerm
	if rf.currentRole == "leader" {
		rf.startAgreement(command, index)
		// fmt.Printf("Trying to start an agreement in term %v for log %v \n", term, index)
		isLeader = true
	}
	return index, term, isLeader

}

// Your code here (2B).

func (rf *Raft) startAgreement(command interface{}, idx int) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	newlog := rf.initLogEntry(command)
	rf.logs = append(rf.logs, newlog)
	rf.lastLogTerm = rf.logs[len(rf.logs)-1].Term
	//2C

	// fmt.Printf("started an agreement in term %v for log %v,server:%v \n", rf.currentTerm, idx, rf.me)
	go rf.checkForAgreement(idx - 1)
}

func (rf *Raft) checkForAgreement(idx int) {

	for !rf.killed() && rf.currentRole == "leader" && idx > rf.commitIndex {

		counter := 1 //count itself
		for _, mIndex := range rf.matchIndex {
			// fmt.Printf("leader's matchindex%v,  counting vote for idx:%v\n", rf.matchIndex, idx)
			if mIndex >= idx {
				// fmt.Printf("recieved a confirm on log %v \n", idx)
				counter += 1
			}

		}
		// fmt.Printf("checking agreement, recieved %v confirm out of %v \n", counter, len(rf.peers))

		if counter > (len(rf.peers) / 2) {
			// fmt.Printf("old commitIdex is %v\n", rf.commitIndex)
			// fmt.Printf("new commitIdex is %v\n", idx)
			// fmt.Printf("reached agreement on log % v leader is server %v\n", idx, rf.me)
			// rf.mu.Lock()
			if idx > rf.commitIndex {
				rf.commitIndex = idx
			}
			// rf.mu.Unlock()

			return
		}

		time.Sleep(HeartbeatInterval)

	}

}

func (rf *Raft) initLogEntry(c interface{}) logEntry {
	newLog := logEntry{
		Term:    rf.currentTerm,
		Command: c,
	}
	return newLog

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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// func getSleepTime() time.Duration {
// 	t := rand.Intn(400)
// 	t += 400
// 	return time.Duration(t) * time.Millisecond

// }
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//check if heartbeat has times out
		if time.Since(rf.lastHeartbeat) > rf.hbTimeout && rf.currentRole == "follower" {

			// fmt.Printf("server %v 's ticker started an election\n", rf.me)
			go rf.startElection()

		}
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		t := time.Duration(200) * time.Millisecond
		time.Sleep(t)

	}
}
func getSleepTime() time.Duration {
	t := rand.Intn(400)
	t += 400
	return time.Duration(t) * time.Millisecond

}

func (rf *Raft) startElection() {

	rf.currentRole = "candidate"
	rf.eTimeout = getElectionTime()
	//increment term
	rf.currentTerm += 1
	//vote for itself
	rf.NVote = 1
	rf.votedFor = rf.me
	rf.persist()
	//reset election Timer
	rf.electionStart = time.Now()
	//send RequestVoteRPCs to all other servers
	for server := range rf.peers {
		args := &RequestVoteArgs{}
		args.CandidateId = rf.me
		args.Term = rf.currentTerm
		//2B
		args.LastLogIndex = len(rf.logs)
		if len(rf.logs) == 0 {
			args.LastLogTerm = 0
		} else {
			args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
		}
		//2B
		reply := &RequestVoteReply{}
		go rf.requestVotes(server, args, reply, rf.currentTerm)
	}
	// fmt.Printf("Server%v started an election in term %v \n", rf.me, rf.currentTerm)
	rf.runElection()

}

//keep on request vote until success
func (rf *Raft) requestVotes(server int, args *RequestVoteArgs, reply *RequestVoteReply, term int) {
	for !rf.killed() && rf.currentRole == "candidate" && rf.currentTerm == term {
		success := rf.sendRequestVote(server, args, reply)
		if success {
			if reply.Voted {
				rf.mu.Lock()
				if term == rf.currentTerm {
					rf.NVote += 1
					// fmt.Printf("Server %v recieved a vote,current vote %v out of %v in term %v\n", rf.me, rf.NVote, len(rf.peers), rf.currentTerm)
				}

				rf.mu.Unlock()
			} else {

				//check term
				t := reply.Term
				rf.updateTerm(t)
				rf.persist()
			}
			return
		}
		time.Sleep(HeartbeatInterval)
	}
}

func getElectionTime() time.Duration {
	t := rand.Intn(500)
	t += 1000
	return time.Duration(t) * time.Millisecond

}

func (rf *Raft) runElection() {
	for !rf.killed() {
		if rf.currentRole != "candidate" {
			return
		}
		if time.Since(rf.electionStart) > rf.eTimeout {
			// time.Sleep(getSleepTime())
			// fmt.Printf("Server %v election timed out,restarted election\n", rf.me)
			rf.currentRole = "follower"
			return
		}
		if rf.NVote > (len(rf.peers) / 2) {
			// fmt.Printf("Server %v recieved the majority of votes\n", rf.me)
			go rf.leaderStart()
			return
		}
		time.Sleep(HeartbeatInterval)

	}
}

func (rf *Raft) leaderStart() {
	rf.initLeader()
	go rf.Heartbeat()

}

func (rf *Raft) initLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("%v initializing it self to leader of term %v\n", rf.me, rf.currentTerm)
	rf.currentRole = "leader"
	//2B initializing match and next index
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, len(rf.logs))
		rf.matchIndex = append(rf.matchIndex, -1)
	}
	// fmt.Printf("initialization finished, nextIndex %v, logs:%v\n", rf.nextIndex, rf.logs)

}

type AppendEntriesArgs struct {
	//2A
	Term     int
	LeaderId int
	//2B
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
	LastLogIndex int
}

type AppendEntriesReply struct {
	//2A
	Term       int
	Success    bool
	FirstIndex int
	MatchIndex int
}

func (rf *Raft) Heartbeat() {
	for !rf.killed() && rf.currentRole == "leader" {
		for server := range rf.peers {
			if server != rf.me {
				args := &AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				//2B
				args.LastLogIndex = len(rf.logs)
				args.PrevLogIndex = rf.nextIndex[server] - 1
				// fmt.Printf("leader's logs %v \n", rf.logs)
				if args.PrevLogIndex == -1 {
					args.PrevLogTerm = 0
				} else {
					args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
				}
				args.Entries = rf.getNewEntries(server)
				args.LeaderCommit = rf.commitIndex
				//2B
				reply := &AppendEntriesReply{}
				go rf.tryAppendEntries(server, args, reply)
			}
		}

		if rf.lastApplied < rf.commitIndex {
			start := rf.lastApplied
			end := rf.commitIndex
			commits := rf.logs[start+1 : end+1]
			for num, commit := range commits {
				index := start + 1 + num
				msg := rf.makeApplyMsg(commit, index+1)
				rf.msgChan <- msg
				rf.lastApplied = index
				// fmt.Printf("server %v(leader) sending through channel log%v ,currentlogs num:%v,lastlogtermis:%v\n", rf.me, index+1, len(rf.logs), rf.lastLogTerm)

			}

		}
		time.Sleep(HeartbeatInterval)

	}

}

func (rf *Raft) getNewEntries(server int) []logEntry {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	idx := rf.nextIndex[server]
	// fmt.Printf("server %v next index is %v, while rf.logs length are %v\n", server, idx, len(rf.logs))
	if len(rf.logs) > idx {
		// fmt.Printf("returning non empty logs")
		return rf.logs[idx:]
	}
	return []logEntry{}
}

func (rf *Raft) tryAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	for !rf.killed() && rf.currentRole == "leader" {
		success := rf.sendAppendEntries(server, args, reply)
		if success {
			//check term

			t := reply.Term

			rf.updateTerm(t)
			rf.persist()
			if !reply.Success && rf.currentRole == "leader" {
				rf.updateNextIndex(server, reply.FirstIndex)
			} else {
				rf.updateMatchIndex(server, reply.MatchIndex)
			}
			return
		}
		time.Sleep(HeartbeatInterval)
	}
}

func (rf *Raft) updateNextIndex(server int, idx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[server] = idx
	// fmt.Printf("updating next index for server %v to %v", server, idx)
}

func (rf *Raft) updateMatchIndex(server int, idx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if idx > rf.matchIndex[server] {
		if idx > len(rf.logs)-1 {
			rf.matchIndex[server] = 0
			rf.nextIndex[server] = len(rf.logs)
		} else {
			rf.matchIndex[server] = idx
			rf.nextIndex[server] = idx + 1

		}
		// fmt.Printf("updating match index for server %v to %v\n", server, rf.matchIndex[server])
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	defer rf.persist()
	t := args.Term
	l := args.LeaderId
	reply.Term = rf.currentTerm

	if t == rf.currentTerm && rf.currentRole != "leader" {
		rf.receiveHeartbeat(t, l)

	}
	if t > rf.currentTerm {
		rf.receiveHeartbeat(t, l)

	}
	//2B
	if t < rf.currentTerm {
		return
	}
	// fmt.Printf("Prevlog index is %v\n", args.PrevLogIndex)
	// fmt.Printf("Prevlog term is %v\n", args.PrevLogTerm)
	if rf.currentRole == "follower" {
		// if len(rf.logs) > args.LastLogIndex {
		// 	rf.logs = rf.logs[:args.LastLogIndex]

		// }

		// if len(args.Entries) == 0 {

		// } else
		if args.PrevLogIndex >= len(rf.logs) {
			reply.Success = false
			//by pass empty slot
			reply.FirstIndex = len(rf.logs)
			return
		} else if args.PrevLogIndex != -1 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			//bypass all logs from conflicting term
			term := rf.logs[args.PrevLogIndex].Term
			current := args.PrevLogIndex
			for current >= 0 && rf.logs[current].Term == term {
				current -= 1
			}
			reply.FirstIndex = current + 1
			rf.logs = rf.logs[:current+1]
			// fmt.Printf("deleting logs from term %v of server:%v\n", term, rf.me)

			return

		} else if len(args.Entries) != 0 {
			reply.MatchIndex = args.PrevLogIndex
			reply.Success = true

			rf.mu.Lock()
			// fmt.Printf("appending %v\n", args.Entries)

			// rf.logs = rf.logs[:args.PrevLogIndex+1]

			// fmt.Printf("sliced log %v\n", rf.logs)
			for idx, entry := range args.Entries {
				current := idx + 1 + args.PrevLogIndex
				if current < len(rf.logs) && rf.logs[current].Term != entry.Term {
					rf.logs = append(rf.logs[:current], entry)
					rf.lastLogTerm = entry.Term
				} else if current < len(rf.logs) {
					continue
				} else {
					rf.logs = append(rf.logs, entry)
					rf.lastLogTerm = entry.Term
				}

				// rf.persist()

				// fmt.Printf("appended log %v\n", rf.logs)
				// fmt.Printf("appending entry %v to server:%v\n", currentIdx, rf.me)

			}
			reply.MatchIndex = len(rf.logs) - 1

			// fmt.Printf("appended logs from %v to %v of server:%v,eader is%v\n", args.PrevLogIndex+1, args.PrevLogIndex+
			// 	len(args.Entries), rf.me, args.LeaderId)
			rf.mu.Unlock()
		}

		//update commit
		rf.mu.Lock()
		oldCommit := rf.commitIndex
		lstIdx := len(rf.logs) - 1
		rf.mu.Unlock()
		newCommit := 0
		//check if commit is updated
		if args.LeaderCommit > oldCommit && lstIdx > oldCommit {

			if lstIdx < args.LeaderCommit {
				newCommit = len(rf.logs) - 1
			} else {
				newCommit = args.LeaderCommit
			}
			rf.mu.Lock()
			rf.commitIndex = newCommit
			rf.mu.Unlock()
			// if oldCommit < newCommit {

			// }
			// fmt.Printf("Updating server %v commitindx to %v\n", rf.me, rf.commitIndex)
		}

		//check if commited logs are applied
		if rf.commitIndex > rf.lastApplied {
			start := rf.lastApplied
			end := rf.commitIndex
			newCommits := rf.logs[start+1 : end+1]
			currentIndex := start + 1
			for _, commit := range newCommits {

				msg := rf.makeApplyMsg(commit, currentIndex+1)
				// fmt.Printf("server %v sending through channel as log %v log:%v,lastlogindex is %v\n logs:%v\n", rf.me, currentIndex+1, commit, len(rf.logs), rf.logs)
				// fmt.Printf("server %v sending through channel as log %v ,lastlogindex is %v\n", rf.me, currentIndex+1, len(rf.logs))
				rf.msgChan <- msg
				rf.lastApplied = currentIndex
				currentIndex++

			}
			// fmt.Printf("server %v sending through channel as log %v to %v ,lastlogindex is %v,current last log term is%v\n", rf.me, start+2, end+1, len(rf.logs), rf.lastLogTerm)

		}

		// fmt.Printf("append successful, server %v current match index is%v,logs:%v\n", rf.me, reply.MatchIndex, rf.logs)
		return
	}

}

func (rf *Raft) makeApplyMsg(commit logEntry, idx int) ApplyMsg {
	msg := ApplyMsg{}
	msg.CommandValid = true
	msg.Command = commit.Command
	msg.CommandIndex = idx
	return msg
}

func (rf *Raft) receiveHeartbeat(t int, id int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < t {
		rf.votedFor = -1
	}
	rf.currentTerm = t
	rf.currentRole = "follower"
	rf.lastHeartbeat = time.Now()
	rf.leaderId = id
	// fmt.Printf("server %v changing it's role to follower, leaderid:%v", rf.me, id)

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
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.msgChan = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.start()
	// fmt.Printf("created server%v role:%v\n", rf.me, rf.currentRole)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	if len(rf.logs) > 0 {
		rf.lastLogTerm = rf.logs[len(rf.logs)-1].Term
	}
	rf.mu.Unlock()
	// fmt.Printf("Started server %v with %v logs\n", rf.me, len(rf.logs))
	go rf.ticker()

	return rf
}
func gethbTime() time.Duration {
	t := rand.Intn(1000)
	t += 600
	return time.Duration(t) * time.Millisecond

}
func (rf *Raft) start() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	//2A
	rf.currentRole = "follower"
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastHeartbeat = time.Now()
	rf.hbTimeout = gethbTime()
	rf.lastLogTerm = 0
	rf.logs = []logEntry{}
	rf.commitIndex = -1
	rf.lastApplied = -1
	// rf.persist()
	// //a dummy log to prevent index out of range, term 0 and fake command
	// rf.logs = append(rf.logs, rf.initLogEntry("dummy entry"))
}
