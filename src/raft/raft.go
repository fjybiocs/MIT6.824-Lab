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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	minHeartBeatTimeOut = 500 // 单位毫秒
	maxHeartBeatTimeOut = 800
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type logEntry struct {
	Term  int
	Index int
	Val   interface{}
}

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// role
	role       int       // 0-Follower; 1-Leader; 2-Candidates
	roleSwitch chan bool // 当状态发生切换时，进行通知

	// persistent
	currentTerm int
	votedFor    int
	logs        []logEntry

	// volatile
	commitIndex int // commit, then apply to state machine.
	lastApplied int

	lastReceiveHeartbeat time.Time
	timeOutOfThisTerm    time.Duration

	// 记录Leader的ID
	leaderIndex int

	// only on leaders
	nextIndex  []int // next log entry to send to that server
	matchIndex []int // for every server, the highest log entry known to be REPLICATED on server
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.leaderIndex == rf.me {
		// 再次检查更新
		rf.sendHeartBeatToCluster()
	}

	term = rf.currentTerm
	if rf.leaderIndex == rf.me {
		isLeader = true
	} else {
		isLeader = false
	}

	DPrintf("----------Get State, term: %d, me: %d, leader: %d", term, rf.me, rf.leaderIndex)
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).

	// to identify the peer
	CandidateID int

	// to make sure only candidates with BIGGER TERM to win
	Term int

	// to make sure only candidates with NEWER LOGS to win
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).

	// to UPDATE TERM
	Term int

	// to make sure the vote process SUCCEED
	VoteGranted bool
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("%d -> %d VoteRequest Received", args.CandidateID, rf.me)

	// 如果对方任期更小，直接拒绝
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("%d 到 %d 的投票请求被拒绝，因为 任期小", args.CandidateID, rf.me)
		return
	}

	// 如果对方任期更大，立刻升级自己，如果自己是Leader，还要退位
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term, args.CandidateID)
	}

	// 判断对方Log新旧
	//lastEntry := rf.getLastLogEntry()
	//if args.LastLogTerm < lastEntry.Term || args.LastLogIndex < lastEntry.Index {
	//	reply.VoteGranted = false
	//	return
	//}

	if rf.votedFor != -1 {
		reply.VoteGranted = false
		DPrintf("%d 到 %d 的投票请求被拒绝，因为 已投票", args.CandidateID, rf.me)
		return
	}

	// 投票
	reply.VoteGranted = true

	// 确保本任期不再投票
	rf.votedFor = args.CandidateID

	return
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
// within a timeout interval, Call() RETURNS TRUE; otherwise
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

type AppendEntriesArgs struct {
	// to make sure the leader is still leader
	Term     int
	LeaderID int

	// to make sure SAFETY ( Log Matching )
	PrevLogIndex int
	PrevLogTerm  int

	// commands
	Entries []logEntry

	// followers see this and commit their local entries
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 这里是3A的简略实现，没有对LogMatching做保证，只是无脑发心跳
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("%d -> %d AppendEntries, 内容:%v, 发送任期：%d，接收任期：%d，当前Leader:%d",
	//	args.LeaderID, rf.me, args.Entries, args.Term, rf.currentTerm, rf.leaderIndex,
	//)

	// 如果对方任期更小，则拒绝
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// todo: LogMatching检测

	// 如果对方任期更大，则需更新自己的任期
	rf.updateTerm(args.Term, args.LeaderID)

	// 同步LeaderID
	rf.leaderIndex = args.LeaderID

	//for i := range rf.peers {
	//	// Log Matching
	//	// todo
	//}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start the service using Raft (e.g. a k/v server) wants to start
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// Kill
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

// start election
// 调用时，必须已经在上级锁住了该peer
func (rf *Raft) election() {
	// todo: 注意，要实现选举超时，要么超时退出，要么当选退出

	win := make(chan bool)
	timeOut := time.After(rf.lastReceiveHeartbeat.Add(rf.timeOutOfThisTerm).Sub(time.Now()))
	DPrintf("%d", rf.lastReceiveHeartbeat.Add(rf.timeOutOfThisTerm).Sub(time.Now()).Milliseconds())
	vote := 0
	voteMutex := sync.Mutex{}

	// 开始选举之前，先清空状态变更通道
	select {
	case <-rf.roleSwitch:
	default:
	}

	// 自增Term
	rf.currentTerm++
	// 坑：给自己投票
	vote++
	rf.votedFor = rf.me

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// 构建参数和返回值
		lastLogEntry := rf.getLastLogEntry()
		args := RequestVoteArgs{
			CandidateID:  rf.me,
			Term:         rf.currentTerm,
			LastLogTerm:  lastLogEntry.Term,
			LastLogIndex: lastLogEntry.Index,
		}
		reply := RequestVoteReply{}

		// 并发地请求投票
		go func(peerIndex int) {
			success := rf.sendRequestVote(peerIndex, &args, &reply)
			//DPrintf("%d -> %d VoteRequest Sent", rf.me, peerIndex)
			if success {
				DPrintf("%d 让 %d 投票的结果: %t，发起者任期：%d", rf.me, peerIndex, reply.VoteGranted, rf.currentTerm)
				if reply.VoteGranted {
					// 如果获得选票，则判断是否成功
					voteMutex.Lock()
					defer voteMutex.Unlock()

					vote++
					if vote > len(rf.peers)/2 {
						// 获胜，发信号
						win <- true
					}
				}
			} else {
				DPrintf("%d 让 %d 投票丢包", rf.me, peerIndex)
			}
		}(i)
	}

	select {
	case <-win:
		// 获胜，切换为Leader
		DPrintf("%d 当选Leader，任期：%d", rf.me, rf.currentTerm)
		rf.leaderIndex = rf.me
		// 将nextIndex和matchIndex切换为当前自己的进度
		for i := range rf.peers {
			// todo:这里的初始化方式应该有问题
			rf.nextIndex[i] = rf.getLastLogEntry().Index
			rf.matchIndex[i] = rf.getLastLogEntry().Index
		}
	case <-timeOut:
		// 超时，重置超时时间
		DPrintf("%d 发起的选举超时，无Leader产生, term:%d，选票：%d，参选人数：%d", rf.me, rf.currentTerm, vote, len(rf.peers))
		rf.randomRefreshHeartbeatAndTimeOut()
	case <-rf.roleSwitch:
		// 发生状态切换，只能是往Follower切了，立刻结束选举流程
		DPrintf("%d 's vote failed, term:%d", rf.me, rf.currentTerm)
		return
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		// 这里之前没有注意到的一个坑：如果自己是Leader，那就不用判断心跳是否过期了
		if rf.leaderIndex != rf.me && time.Now().Sub(rf.lastReceiveHeartbeat) > rf.timeOutOfThisTerm {
			DPrintf("%d 开始了一次选举, 上次心跳: %s, 当前任期：%d",
				rf.me, rf.lastReceiveHeartbeat.Format("04:05.000"), rf.currentTerm)
			// 刷新
			rf.randomRefreshHeartbeatAndTimeOut()
			rf.election()
		}

		// 如果自己是Leader，则需要发心跳
		if rf.leaderIndex == rf.me {
			rf.sendHeartBeatToCluster()
		}

		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// HAVE THE SAME ORDER. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it SHOULD START GOROUTINES
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers // PEERS INIT!
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	// ---
	// 初始化系统状态
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.randomRefreshHeartbeatAndTimeOut()
	rf.leaderIndex = -1
	rf.votedFor = -1
	//DPrintf("%d 's time out: %dms", rf.me, rf.timeOutOfThisTerm.Milliseconds())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// 随机重置超时时间
// 不保证并发安全，请在上层加锁
func (rf *Raft) randomRefreshHeartbeatAndTimeOut() {
	rand.Seed(time.Now().UnixNano())
	timeout := time.Duration(rand.Intn(maxHeartBeatTimeOut-minHeartBeatTimeOut)+minHeartBeatTimeOut) * time.Millisecond

	rf.timeOutOfThisTerm = timeout
	rf.lastReceiveHeartbeat = time.Now()
}

// 获取当前状态机最后一个Log
// 如果当前为空，则会返回一个index为-1，Term为-1的logEntry
// 不保证并发安全，请在上层加锁
func (rf *Raft) getLastLogEntry() logEntry {
	lastLogIndex := len(rf.logs) - 1 // 存疑，是最后一个还是最后一个已提交
	if lastLogIndex >= 0 {
		return rf.logs[lastLogIndex]
	}

	return logEntry{
		Index: -1,
		Term:  -1,
	}
}

// 发送心跳，可以发现当前任期是否已经过期
// 不保证并发安全，请在上层加锁
func (rf *Raft) sendHeartBeatToCluster() {
	// 构建参数和返回值
	request := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: rf.getLastLogEntry().Index,
		PrevLogTerm:  rf.getLastLogEntry().Term,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peerIndex int) {
			success := rf.sendAppendEntries(peerIndex, &request, &reply)
			if success {
				// 如果任期已经落后，则退位，并更新自己的任期
				rf.updateTerm(reply.Term, peerIndex)
			}
		}(i)
	}
}

// 修改任期
// 不保证并发安全，请在上层加锁
func (rf *Raft) updateTerm(newTerm, fromPeer int) {
	if newTerm <= rf.currentTerm {
		// 无需更新
		return
	}

	DPrintf("%d 被 %d 更新任期为 %d", rf.me, fromPeer, newTerm)

	// 更新任期
	rf.currentTerm = newTerm

	// 清空投票
	rf.votedFor = -1

	// 判断是否是Candidate

	// 判断是否需要Leader退位
	if rf.leaderIndex == rf.me {
		rf.leaderIndex = fromPeer
		// 尝试通知角色转变
		select {
		case rf.roleSwitch <- true:
		default:
		}
	}
}
