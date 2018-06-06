package raft

import (
	"context"
	"sort"

)

type requestAppendEntries struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []EntryLog
	LeaderCommit int
}

type responseAppendEntries struct {
	Term    int
	Success bool
}

// RequestAppendEntry append log entry request
func (rf *Raft) RequestAppendEntry(args *requestAppendEntries) (responseAppendEntries, bool) {
	if args.Term < rf.currentTerm {
		return responseAppendEntries{rf.currentTerm, false}, true
	} else if args.Term > rf.currentTerm {
		rf.updateCurrentTerm(args.Term)
	}
	// voted for leader, assume self has vote leader
	// see the example of a,b,c,d,e five nodes
	rf.voteCandicate(args.LeaderId)

	//rf.log.mutex.Lock()
	//defer rf.log.mutex.Unlock()

	logIndex, logTerm := rf.log.lastInfo()

	if logIndex < args.PrevLogIndex || (logIndex == args.PrevLogIndex && logTerm != args.PrevLogTerm) {
		return responseAppendEntries{rf.currentTerm, false}, true
	}
	if len(rf.log.entries) == 0 {
		return responseAppendEntries{rf.currentTerm, true}, true
	}

	// get the maxest match , and followers delete the logs after the matLogIndex
	// and append the leader's logs
	isMatch, matchIndex := false, 0

	for i := rf.log.length - 1; i >= 0; i-- {
		entry := rf.log.entries[i]
		if entry.Index == args.PrevLogIndex && entry.Term == args.Term {
			isMatch = true
			matchIndex = i
			break
		}
	}
	if !isMatch {
		return responseAppendEntries{rf.currentTerm, false}, true
	}
	rf.log.entries = append(rf.log.entries[:matchIndex+1], args.Entries...)
	rf.log.length = len(rf.log.entries)

	_, logIndex = rf.log.lastInfo()

	if args.LeaderCommit > rf.commitIndex {
		idx := min(args.LeaderCommit, logIndex)
		//only commit current term log, befor logs will be commit
		for i := rf.log.length - 1; i >= 0; i-- {
			if rf.log.entries[i].Index == idx && rf.currentTerm == rf.log.entries[i].Term {
				rf.commitIndex = idx
				rf.applyNotice <- struct{}{}
			}
		}
	}
	return responseAppendEntries{rf.currentTerm, true}, true

}

// sendAppendRequest send a vote request
func (rf *Raft) sendAppendRequest(ctx context.Context, server int, args requestAppendEntries, reply *responseAppendEntries) bool {
	ok := rf.peers[server].Call(ctx, "Raft.RequestAppend", args, reply)
	return ok
}

func (rf *Raft) RequestAppend(args requestAppendEntries, reply *responseAppendEntries) {
	message := &message{args: args, reply: reply, err: make(chan error)}
	rf.deliver(message)
	select {
	case <-message.err:
	case <-rf.exitFlag:
		return
	}
}

func (rf *Raft) makeAppendRequestArgs(server int) *requestAppendEntries {
	args := requestAppendEntries{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	rf.mutex.Lock()
	defer rf.mutex.Unlock()

	if rf.nextIndex[server] - 1 > 0 {
		args.PrevLogIndex = rf.log.entries[rf.nextIndex[server]-1].Index
	} else {
		args.PrevLogIndex = 0
	}
	args.LeaderCommit = rf.commitIndex
	rf.log.mutex.Lock()
	defer rf.log.mutex.Unlock() // protect the rf.log.length
	if rf.nextIndex[server] < rf.log.length {
		args.Entries = rf.log.entries[rf.nextIndex[server]:]
		//fmt.Println("raft log info ", rf.me, rf.log.entries, len(rf.log.entries), rf.nextIndex[server]-1)
		if rf.nextIndex[server] - 1 > 0 {
			args.PrevLogTerm = rf.log.entries[rf.nextIndex[server]-1].Term
		}
	}
	if len(args.Entries) > 0 {
		//fmt.Printf("raft[%d] append logs args %d, %v, %d, %d", rf.me, args.LeaderCommit, args.Entries, args.PrevLogIndex, args.PrevLogTerm)
	}
	return &args
}

type appendResp struct {
	args  *requestAppendEntries
	resp  *responseAppendEntries
	index int
}

func (rf *Raft) makeAppendRequest(ctx context.Context, server int, resp chan appendResp) {
	argsPtr := rf.makeAppendRequestArgs(server)
	go func(index int) {
		replay := &responseAppendEntries{}
		ok := rf.sendAppendRequest(ctx, index, *argsPtr, replay)
		if !ok {
			//fmt.Println("send log errororororororo *&*************")
			return
		}
		resp <- appendResp{argsPtr, replay, index}
	}(server)
}

func (rf *Raft) updateNextAndMatch(resp appendResp) {
	args := resp.args
	index := rf.log.logIndex(args.PrevLogIndex, len(args.Entries))
	rf.matchIndex[resp.index] = index
	rf.nextIndex[resp.index] = index + 1
	//fmt.Println("\n\nupdate next data", index, rf.me)
}

func (rf *Raft) handleAppendRequest(ctx context.Context, commit map[int]bool, resp appendResp, respChan chan appendResp) {
	if resp.resp.Success {
		//fmt.Println("leader handle append log response success111")
		commit[resp.index] = true
		rf.mutex.Lock()
		rf.updateNextAndMatch(resp)
		rf.mutex.Unlock()
		if len(commit) >= rf.QuorumSize() {
			ret := []int{}
			for k := range commit {
				ret = append(ret, rf.matchIndex[k])
			}
			sort.Slice(ret, func(i, j int) bool {
				return i > j
			})
			entryIndex := ret[rf.QuorumSize()-1]

			if len(rf.log.entries) > 0 && rf.log.entries[entryIndex].Term == rf.currentTerm {
				rf.commitIndex = entryIndex
			}
		}

	} else {
		//fmt.Println("\n\nit should not run -----------------")
		rf.mutex.Lock()
		if rf.nextIndex[resp.index] > 0 {
			rf.nextIndex[resp.index]--
		}
		rf.mutex.Unlock()
		//retry send append request
		//rf.makeAppendRequest(ctx, resp.index, respChan)
	}
}
