package raft

import "context"

// rpc module
//

// requestVoteArgs for vote with rpc
type requestVoteArgs struct {
	Term         int
	CandicateId  int
	LastLogIndex int
	LastLogoTerm int
}

// responseVoteArgs for vote responsew with rpc
type responseVoteArgs struct {
	Term        int
	VoteGranted bool
}

type message struct {
	args  interface{}
	reply interface{}
	err   chan error
}

// RequestVoteResponse vote request
func (rf *Raft) RequestVoteResponse(args *requestVoteArgs) (responseVoteArgs, bool) {

	res := responseVoteArgs{}
	if args.Term < rf.currentTerm {
		res.Term = rf.currentTerm
		res.VoteGranted = false

		return res, false
	} else if args.Term > rf.currentTerm { // update term
		rf.updateCurrentTerm(args.Term)
	}

	if rf.votedFor != votedInitValue && rf.votedFor != args.CandicateId {
		// has voted other,
		res.Term = rf.currentTerm
		res.VoteGranted = false
		//fmt.Println("vote erro1", rf.votedFor, args.CandicateId, rf.me, rf.currentTerm)
		return res, false
	}

	rf.log.mutex.Lock()
	lastLogIndex, lastLogTerm := rf.log.lastInfo()
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
	//rf.mutex.Lock()
	rf.currentTerm = curTerm
	//rf.mutex.Unlock()
}

func (rf *Raft) voteCandicate(candicateId int) {
	rf.votedFor = candicateId
}

// deliver which is the entry of all rpc requests
// all request go into buffered blockEventQ channel
// and start a goroutine to wait for response
func (rf *Raft) deliver(m *message) {
	// m := &message{args:args, reply:reply, err:make(chan error)}
	// while server state changed, stop response
	select {
	case <-rf.exitFlag:
		return
	case rf.blockEventQ <- m: // should block
	}
}

// sendVoteRequest send a vote request
func (rf *Raft) sendVoteRequest(ctx context.Context, server int, args requestVoteArgs, reply *responseVoteArgs) bool {
	//fmt.Println("errrrrrr: ", server)
	ok := rf.peers[server].Call(ctx, "Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args requestVoteArgs, reply *responseVoteArgs) {
	message := &message{args: args, reply: reply, err: make(chan error)}
	rf.deliver(message)
	select {
	case <-message.err:
	case <-rf.exitFlag:
		return
	}
}
