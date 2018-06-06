package raft

import (
	"context"
	"errors"
	"fmt"
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
}

func init() {

}

func (rf *Raft) updateState(state int) {
	oldState := rf.state
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
	} else if state == CandicateState{
		rf.votedFor = -1
	}
	if rf.state != LeaderState {
		return
	}
	// todo 仅仅作为调试

	// update state hook functions for debug
	for _,f := range rf.updateStateFunc{
		if oldState == LeaderState {
			fmt.Println("old state", oldState)
		}
		//fmt.Println("leader has vote ***************************************", rf.state, rf.me)
		f()
		//fmt.Println("leader has vote ****************************endendendenden", rf.state, rf.me)
	}
}

func (rf *Raft) QuorumSize() int {
	return len(rf.peers)/2 + 1
}

const (
	HeartSt  = int64(90)
	HeartEnd = int64(100)
)

func (rf *Raft) LeaderLoop() {
	// todo leader state loop
	timeout := randTime(HeartSt, HeartEnd)
	heartbeat := true
	//var num = 0
	appResp := make(chan appendResp)
	logCommit := make(map[int]bool)

	ctx, _ := context.WithCancel(context.Background())

	logMockChan := make(chan struct{})
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
	fmt.Printf("\n\n\n\n---------------------------------------")
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
				//fmt.Printf("[raft:%d] send append log to [raft:%d] %v\n", rf.me, i, rf.log.length)
				rf.makeAppendRequest(ctx, i, appResp)
			}
		}
		select {
		case reply := <-appResp:
			//fmt.Println("\n\nleader handler --------------------------")
			if reply.resp.Term > rf.currentTerm {
				rf.updateState(FollowerState)
				rf.votedFor = -1
				return
			}
			rf.handleAppendRequest(ctx, logCommit, reply, appResp)

		//case <-rf.applyNotice:
		// append request
		//	timeout = randTime(HeartSt, HeartEnd)
		case <-timeout:
			heartbeat = true
			timeout = randTime(HeartSt, HeartEnd)
		//case <-rf.blockEventQ:
			// no network delay or any Network partition, leader will not receive
			// vote and append log info
		case <-logMockChan:
			// todo just for test
			// receive a client log
			//heartbeat = true
			//timeout = randTime(HeartSt, HeartEnd)
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
		timeoutChan = randTime(int64(300), int64(500))
	}else{
		timeoutChan = randTime(int64(500), int64(800))
	}

	for rf.state == FollowerState {
		//fmt.Println("follower loop: ", rf.me)
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
			//fmt.Println("rececv emssage mesage message message, ", rf.me, event.args)
			switch args := event.args.(type) {
			case requestVoteArgs:
				//fmt.Println("recevt vote args", rf.me)
				var rp responseVoteArgs
				rp, update = rf.RequestVoteResponse(&args)
				//event.reply = rp
				rpPtr := event.reply.(*responseVoteArgs)
				//fmt.Printf("raft[%d] handle vote req %v", rf.me, rp.VoteGranted)
				*rpPtr = rp
			case requestAppendEntries:
				// todo handle append Entries
				//fmt.Printf("reecevet [raft:%d] rececv append log  form [raft: %d]\n",rf.me,args.LeaderId)
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
				//fmt.Printf("\nraft[%d] start a vote\n", rf.me)
				timeoutChan = randTime(int64(500), int64(800))
			}
		case <-timeoutChan:
			rf.updateState(CandicateState)
			//fmt.Printf("\nraft[%d] follower => candicate\n", rf.me)
			return
		}
	}
}

// voteSelf start to
func (rf *Raft) voteSelf() {
	rf.updateCurrentTerm(rf.currentTerm + 1)
	rf.votedFor = rf.me
}

type debug struct {
	me int
	term int
}
func (rf *Raft) makeVote(ctx context.Context, in22 int, resp chan *responseVoteArgs, lock2 *sync.Mutex, voteMap *[]debug) {
		lastIndex, lastTerm := rf.log.lastInfo()
		args := requestVoteArgs{
			Term:         rf.currentTerm,
			CandicateId:  rf.me,
			LastLogoTerm: lastTerm,
			LastLogIndex: lastIndex,
		}
		//fmt.Println("send vote requets")
		replay := &responseVoteArgs{}
		if ok := rf.sendVoteRequest(ctx, in22, args, replay);!ok {
			return
		}


		//fmt.Printf("%d send vote rquest %d ret : %v\n", rf.me, index, ret,)
		//fmt.Println("\nterm ", rf.me, rf.currentTerm, in, *replay)

		select {
		case <-ctx.Done():
			return
		default:
		}
		if replay.VoteGranted {
			lock2.Lock()
			*voteMap = append(*voteMap, debug{me:in22, term:replay.Term})
			lock2.Unlock()
		}
		resp <- replay

	}

func (rf *Raft) CandicateLoop() {
	var (
		ctx context.Context
	)
	timeChan := randTime(int64(500), int64(800))
	voteFor := true
	var number int32 = 0
	resp := make(chan *responseVoteArgs, 100)
	var voteMap  []debug
	var lock2 sync.Mutex
	count:= 0
	cc := make([]int, 0)

	for rf.state == CandicateState {
		if voteFor {
			//rf.blockEventQ = make(chan *message, 100)
			voteMap = make([]debug,0)
			resp = make(chan *responseVoteArgs, 100)
			number = 0
			count = 0
			cc = make([]int, 0)
			voteFor = false
			rf.voteSelf()
			ctx, _ = context.WithCancel(context.Background())
			for index := 0; index < len(rf.peers); index ++ {

				select {
				case <-rf.exitFlag:
					return
				default:
				}
				if index == rf.me {
					continue
				}

				go rf.makeVote(ctx, index, resp, &lock2, &voteMap)
			}
		}
		if number >= int32(rf.QuorumSize()) {
			if rf.me == 2 || rf.me == 3 || rf.me == 5 {
				fmt.Printf("\nraft[%d] candicate => leder %v %d %d %s %d %v\n", rf.me, voteMap, number,rf.currentTerm," :: count", count, cc)
			}
			rf.updateState(LeaderState)
			//cancel()
			number = 0
			return
		}

		select {
		case <-rf.exitFlag:
			return
		case <-timeChan:
			voteFor = true
			timeChan = randTime(int64(500), int64(800))
		case res := <-resp:
			if res.Term > rf.currentTerm {
				// rules for all servers
				rf.updateCurrentTerm(res.Term)
				// todo common => InitState(FollowerState)
				rf.updateState(FollowerState)
				rf.votedFor = -1
				return
			}
			if res.VoteGranted && res.Term == rf.currentTerm{
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
				//fmt.Println("response vote:****************** ", rf.me)
				rpPtr := event.reply.(*responseVoteArgs)
				*rpPtr = rp
			case requestAppendEntries:
				// todo handle append Entries
				// receive the append log request, candicate => follower
				// whill leader break down, costs less time to vote a leader
				//fmt.Printf("[raft:%d] get the append log from [raft:%d]", rf.me, args.LeaderId)
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
			//fmt.Printf("raft[%d] catch exitFlag stop signal, shutdown\n", rf.me)
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


