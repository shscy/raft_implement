package raft

// rpc module
//


// requestVoteArgs for vote with rpc
type requestVoteArgs struct {
	Term int
	CandicateId int
	LastLogIndex int
	LastLogoTerm int
}

// responseVoteArgs for vote responsew with rpc
type responseVoteArgs struct {
	Term int
	VoteGranted bool
}


type requestAppendEntries struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries  []EntryLog
	LeaderCommit int
}

type responseAppendEntries struct {
	Term int
	Success bool
}


type message struct {
	args interface{}
	reply interface{}
	err chan error
}
func (m *message) Response(err error) {
	// response to rpc client
}


// RequestAppendEntry append log entry request
func (rf *Raft) RequestAppendEntry(args *requestAppendEntries)(responseAppendEntries, bool) {
	if args.Term < rf.currentTerm {
		return responseAppendEntries{rf.currentTerm, false}, true
	} else if args.Term > rf.currentTerm {
		rf.updateCurrentTerm(args.Term)
	}
	// voted for leader, assume self has vote leader
	// see the example of a,b,c,d,e five nodes
	rf.voteCandicate(args.LeaderId)

	rf.log.mutex.Lock()
	defer rf.log.mutex.Unlock()

	logIndex, logTerm := rf.log.lastInfo()
	if logIndex < args.PrevLogIndex || (logIndex == args.PrevLogIndex && logTerm == args.PrevLogTerm){
		return responseAppendEntries{rf.currentTerm, false}, true
	}
	isMatch, matchIndex := false, 0

	for i := rf.log.length-1; i >= 0; i-- {
		entry := rf.log.entries[i]
		if entry.Index == args.PrevLogIndex && entry.Term == args.Term{
			isMatch = true
			matchIndex  = i
			break
		}
	}
	if !isMatch{
		return responseAppendEntries{rf.currentTerm, false}, true
	}
	rf.log.entries = append(rf.log.entries[:matchIndex+1], args.Entries...)
	rf.log.length = len(rf.log.entries)

	_, logIndex = rf.log.lastInfo()

	if args.LeaderCommit > rf.commitIndex {
		idx := min(args.LeaderCommit, logIndex)
		//only commit current term log, befor logs will be commit
		for i:= rf.log.length-1; i >=0; i -- {
			if rf.log.entries[i].Index == idx && rf.currentTerm == rf.log.entries[i].Term{
				rf.commitIndex = idx
				rf.applyNotice <- struct{}{}
			}
		}
	}
	return responseAppendEntries{rf.currentTerm, true}, true

}

// RequestVoteResponse vote request
func(rf *Raft) RequestVoteResponse(args *requestVoteArgs) (responseVoteArgs, bool) {
	res := responseVoteArgs{}
	if args.Term < rf.currentTerm {
		res.Term = rf.currentTerm
		res.VoteGranted = false
		return res, false
	} else if args.Term > rf.currentTerm { // update term
		rf.updateCurrentTerm(args.Term)
	}

	if rf.votedFor != votedInitValue && rf.votedFor != args.CandicateId{
		// has voted other,
		res.Term = rf.currentTerm
		res.VoteGranted = false
		return res, false
	}
	rf.log.mutex.Lock()
	lastLogIndex, lastLogTerm:= rf.log.lastInfo()
	rf.log.mutex.Unlock()

	if args.LastLogoTerm > lastLogTerm || (args.LastLogIndex >= lastLogIndex) {
		res.Term = rf.currentTerm
		res.VoteGranted = true
		// update votedFor pointer
		rf.voteCandicate(args.CandicateId)
		return res, true
	}
	return res, false
}


func (rf *Raft) updateCurrentTerm(curTerm int) {
	rf.currentTerm = curTerm
}

func (rf *Raft) voteCandicate(candicateId int) {
	rf.votedFor = candicateId
}

// RpcEntry which is the entry of all rpc requests
// all request go into buffered blockEventQ channel
// and start a goroutine to wait for response
func (rf *Raft) RpcEntry(m *message) {

	// while server state changed, stop response
	go func (){
		select {
		case err :=  <- m.err:
			m.Response(err)
		case <- rf.exitFlag:
			return
		}

	}()

	select {
	case <-rf.exitFlag:
		return
	case rf.blockEventQ <- m: // should block
	}
}
