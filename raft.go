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
}

func init() {

}

func (rf *Raft) Connect(in string, out *int) {
	fmt.Println("in ", in)
	*out = 888

}
func (rf *Raft) updateState(state int) {
	rf.state = state
	if state == LeaderState {
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers);i++{
			rf.nextIndex[i] = rf.log.length
			rf.matchIndex[i] = 0
		}
	} else if state == FollowerState {
		rf.votedFor = -1
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
				fmt.Printf("[raft:%d] sen append log to [raft:%d]\n", rf.me, i)
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

		//case <-rf.applyNotice:
			// append request
		//	timeout = randTime(HeartSt, HeartEnd)
		case <-timeout:
			heartbeat = true
			timeout = randTime(HeartSt, HeartEnd)
		case <-rf.blockEventQ:

		}
	}
}

func (rf *Raft) State() int {
	return rf.state
}

func (rf *Raft) FollowerLoop() {
	// todo
	timeoutChan := randTime(int64(500), int64(800))

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

			switch args := event.args.(type) {
			case requestVoteArgs:
				var rp responseVoteArgs
				rp, update = rf.RequestVoteResponse(&args)
				//event.reply = rp
				rpPtr := event.reply.(*responseVoteArgs)
				*rpPtr = rp
			case requestAppendEntries:
				// todo handle append Entries
				//fmt.Printf("reecevet [raft:%d] rececv append log  form [raft: %d]\n",rf.me,args.LeaderId)
				var resp responseAppendEntries
				resp, update = rf.RequestAppendEntry(&args)
				rpPtr := event.reply.(*responseAppendEntries)
				fmt.Printf("[raft:%d] resposne append log :%v \t args : %v\n", rf.me, resp, args)
				*rpPtr = resp
			default:
				err = errors.New("follower receve unsupport message")
			}
			// rpc select the err chan to judge whether response over
			event.err <- err
			if update {
				timeoutChan = randTime(int64(500), int64(800))
			}
		case <-timeoutChan:
			rf.updateState(CandicateState)
			return
		}
	}
}

// voteSelf start to
func (rf *Raft) voteSelf() {
	rf.updateCurrentTerm(rf.currentTerm + 1)
	rf.votedFor = rf.me
}

func (rf *Raft) CandicateLoop() {
	var (
		ctx context.Context
		//cancel func()
	)
	timeChan := randTime(int64(500), int64(800))
	voteFor := true
	// count the vote self num
	var number int32 = 0
	resp := make(chan *responseVoteArgs, 100)
	//var wait sync.WaitGroup
	//wait.Add(2)
	for rf.state == CandicateState {
		if voteFor {
			resp = make(chan *responseVoteArgs, 100)
			number = 0
			voteFor = false
			//fmt.Println("start to vote leader", rf.peers)
			rf.voteSelf()
			ctx, _ = context.WithCancel(context.Background())
			for index, peer := range rf.peers {
				//fmt.Println("get in ")
				select {
				case <-rf.exitFlag:
					return
				default:
				}
				if index == rf.me {
					continue
				}
				go func(peer labrpc.ClientEnd, in int) {

					rf.log.mutex.Lock()
					lastIndex, lastTerm := rf.log.lastInfo()
					rf.log.mutex.Unlock()
					args := requestVoteArgs{
						Term:         rf.currentTerm,
						CandicateId:  rf.me,
						LastLogoTerm: lastTerm,
						LastLogIndex: lastIndex,
					}
					replay := &responseVoteArgs{}
					if !rf.sendVoteRequest(ctx, in, args, replay) {
						// rpc false, return directly
						return
					}

					//fmt.Printf("%d send vote rquest %d ret : %v\n", rf.me, index, ret,)
					fmt.Println("\nterm ", rf.me, rf.currentTerm, in, *replay)

					select {
					case <-ctx.Done():
						return
					default:
					}
					resp <- replay

				}(*peer, index)
			}
		}
		if number >= int32(rf.QuorumSize()) {
			rf.updateState(LeaderState)
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
			if res.VoteGranted {
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
			fmt.Println("errr loop out", rf.me)
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
