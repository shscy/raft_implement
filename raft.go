package raft

import (
	"context"
	"errors"
	"labrpc"
	"sync"
	"time"
)

// raft 节点的3种状态的管理
// 日志状态机的管理
//
//

const (
	LeaderState = iota
	FollowerState
	CandicateState
)

const (
	votedInitValue = -1
)

type RPCConn struct {
	Id    int
	conns map[int]RPCConn
}

type Raft struct {
	me int

	peers       []*labrpc.ClientEnd
	currentTerm int
	votedFor    int
	log         *log

	commitIndex int
	lastApplied int

	//follow two fields for leader
	nextIndex  []int
	matchIndex []int

	// buffered channel
	// explain why using blockEventQ channel
	// server is only in a single state, each state Concurrently handle different rpc request
	// use channel we can decrease using the lock to protect state,logs etc
	group           sync.WaitGroup
	blockEventQ     chan *message
	electionTimeout time.Duration // electionTimeout election timeout
	mutex           sync.Mutex
	state           int // state of the
	applyNotice     chan struct{}
	exitFlag        chan struct{} // stop signal channel, while stop, close(exitFlag)

	updateStateFunc []func()

	//debug
	testIndex int
}

func init() {

}

func (rf *Raft) updateState(state int, callback ...bool) {
	if state == rf.state {
		return
	}
	rf.state = state
	if state == LeaderState {
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.log.length
			rf.matchIndex[i] = 0
		}
	} else if state == FollowerState {
		rf.votedFor = -1
	} else if state == CandicateState {
		rf.votedFor = -1
	}
	if true {
		if rf.state != LeaderState {
			return
		}
		// todo 仅仅作为调试
		// update state hook functions for debug
		for _, f := range rf.updateStateFunc {
			f()
		}
	}
}

func (rf *Raft) QuorumSize() int {
	return len(rf.peers)/2 + 1
}

const (
	HeartSt  = int64(80)
	HeartEnd = int64(100)

	VoteSt  = int64(200)
	VoteEnd = int64(300)
)

func (rf *Raft) LeaderLoop() {
	// todo leader state loop
	timeout := randTime(HeartSt, HeartEnd)
	heartbeat := true
	//var num = 0
	appResp := make(chan appendResp)
	logCommit := make(map[int]bool)

	ctx, _ := context.WithCancel(context.Background())

	//go func() {
	//	// this goroutine mock the client sned log
	//	t := randTime(int64(700), int64(1500))
	//	for {
	//		select {
	//		case <-rf.exitFlag:
	//			return
	//		// operate log
	//		case <-t:
	//			index, _ := rf.log.lastInfo()
	//			rf.log.addEntry(EntryLog{
	//				Index: index + 1,
	//				Term: rf.currentTerm,
	//			})
	//
	//			logMockChan <- struct{}{}
	//			return
	//			//t = randTime(int64(700), int64(1500))
	//		}
	//	}
	//}()
	for rf.state == LeaderState {
		select {
		case <-rf.exitFlag:
			return
		default:
		}

		if heartbeat {
			heartbeat = false
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.makeAppendRequest(ctx, i, appResp)
			}
		}
		select {
		case reply := <-appResp:
			if reply.resp.Term > rf.currentTerm {
				rf.updateState(FollowerState)
				rf.votedFor = -1
				return
			}
			rf.handleAppendRequest(ctx, logCommit, reply, appResp)

		case <-timeout:
			heartbeat = true
			timeout = randTime(HeartSt, HeartEnd)
		case event := <-rf.blockEventQ:
			switch event.args.(type) {
			case *LogClientMessage:
				index, _ := rf.log.lastInfo()
				rf.log.addEntry(EntryLog{
					Index: index + 1,
					Term:  rf.currentTerm,
				})
				rf.updateNextByClientLog()
				// no network delay or any Network partition, leader will not receive
				// vote and append log info
			}
		}
	}
}

func (rf *Raft) State() int {
	return rf.state
}

func (rf *Raft) FollowerLoop() {
	// todo
	var timeoutChan <-chan time.Time
	if rf.me == 5 {
		timeoutChan = randTime(VoteSt, VoteEnd)
	} else {
		timeoutChan = randTime(VoteSt, VoteEnd)
	}

	for rf.state == FollowerState {
		select {
		case <-rf.exitFlag:
			return
		default:
		}
		select {
		case event := <-rf.blockEventQ:
			var (
				err    error
				update bool
			)
			switch args := event.args.(type) {
			case requestVoteArgs:
				var rp responseVoteArgs
				rp, update = rf.RequestVoteResponse(&args)
				//event.reply = rp
				rpPtr := event.reply.(*responseVoteArgs)
				*rpPtr = rp
			case requestAppendEntries:
				// todo handle append Entries
				var resp responseAppendEntries
				resp, update = rf.RequestAppendEntry(&args)
				rpPtr := event.reply.(*responseAppendEntries)
				*rpPtr = resp
			default:
				err = errors.New("follower receve unsupport message")
			}
			// rpc select the err chan to judge whether response over
			event.err <- err
			if update {
				timeoutChan = randTime(int64(200), int64(300))
			}
		case <-timeoutChan:
			rf.updateState(CandicateState)
			return
		}
	}
}

func (rf *Raft) makeVote(ctx context.Context, in22 int, resp chan *responseVoteArgs) {

	lastIndex, lastTerm := rf.log.lastInfo()
	args := requestVoteArgs{
		Term:         rf.currentTerm,
		CandicateId:  rf.me,
		LastLogoTerm: lastTerm,
		LastLogIndex: lastIndex,
	}
	replay2 := &responseVoteArgs{}
	if ok := rf.sendVoteRequest(ctx, in22, args, replay2); !ok {
		return
	}


	select {
	case <-ctx.Done():
		return
	default:
	}
	//if replay.VoteGranted {
	//	lock2.Lock()
	//	*voteMap = append(*voteMap, debug{me:in22, term:replay.Term})
	//	lock2.Unlock()
	//}
	resp <- replay2

}

func (rf *Raft) CandicateLoop() {
	var (
		ctx context.Context
	)
	timeChan := randTime(VoteSt, VoteEnd)
	voteFor := true
	var number int32 = 0
	var resp2 chan *responseVoteArgs

	for rf.state == CandicateState {

		if voteFor {
			//rf.blockEventQ = make(chan *message, 100)

			resp2 = make(chan *responseVoteArgs, 100)
			number = 1

			voteFor = false
			rf.currentTerm++
			rf.votedFor = rf.me
			ctx, _ = context.WithCancel(context.Background())

			//lastIndex, lastTerm := rf.log.lastInfo()
			for index := 0; index < len(rf.peers); index++ {
				if index != rf.me {
					go rf.makeVote(ctx, index, resp2)
				}
			}

		}
		if number >= int32(rf.QuorumSize()) {
			rf.updateState(LeaderState)
			//cancel()
			return
		}

		select {
		case <-rf.exitFlag:
			return

		case <-timeChan:
			voteFor = true
			timeChan = randTime(VoteSt, VoteEnd)

		case res := <-resp2:
			if res.Term > rf.currentTerm {
				// rules for all servers
				rf.updateCurrentTerm(res.Term)
				// todo common => InitState(FollowerState)
				rf.updateState(FollowerState)
				rf.votedFor = -1
				return
			}
			if res.VoteGranted && res.Term == rf.currentTerm {
				number += 1
			}

		case event := <-rf.blockEventQ:
			// just like followers, but never update timeChan
			var err error = nil
			switch args := event.args.(type) {
			case requestVoteArgs:
				//var rp responseVoteArgs
				// candicate handle the vote args never success, because it vote it self
				// and ignore the update field
				rp, _ := rf.RequestVoteResponse(&args)
				rpPtr := event.reply.(*responseVoteArgs)
				*rpPtr = rp
			case requestAppendEntries:
				// receive the append log request, candicate => follower
				// whill leader break down, costs less time to vote a leader
				resp, _ := rf.RequestAppendEntry(&args)
				rpPtr := event.reply.(*responseAppendEntries)
				*rpPtr = resp
				rf.updateState(FollowerState)
				return
			default:
				err = errors.New("follower receve unsupport message")
			}
			event.err <- err

		}
	}

}

func (rf *Raft) Wait() {
	select {
	case <-rf.exitFlag:
	}
}

// StateLoop
func (rf *Raft) StateLoop() {
	for {
		// exit the server
		select {
		case <-rf.exitFlag:
			return
		default:
		}
		// any time, server has only state
		switch rf.state {
		case LeaderState:
			rf.LeaderLoop()
		case FollowerState:
			rf.FollowerLoop()
		case CandicateState:
			rf.CandicateLoop()
		}
	}
}
