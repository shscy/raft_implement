package raft

import (
	"context"
	"errors"
	"fmt"
	"labrpc"
	"sync"
	"sync/atomic"
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
}

func (rf *Raft) QuorumSize() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) LeaderLoop() {
	// todo leader state loop
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
			default:
				err = errors.New("follower receve unsupport message")
			}
			// rpc select the err chan to judge whether response over
			event.err <- err
			if update {
				timeoutChan = randTime(int64(500), int64(800))
			}
		case <-timeoutChan:
			//fmt.Println("time out ----->>>>>>>>")
			// become to candicate
			if rf.me == 2 {
				//time.Sleep(time.Duration(10000) * time.Millisecond)
				//rf.updateState(CandicateState)
				//ft.Println("become candicate", rf.me)
				rf.updateState(CandicateState)
			} else {

			}

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
		ctx    context.Context
		//cancel func()
	)
	timeChan := randTime(int64(500), int64(800))
	voteFor := true
	// lock the update Term
	var lock sync.Mutex
	var lockNum sync.Mutex
	// count the vote self num
	var number int32 = 0

	//var wait sync.WaitGroup
	//wait.Add(2)
	for rf.state == CandicateState {
		if voteFor {
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
				go func(peer labrpc.ClientEnd, in int ) {

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
					_ = rf.sendVoteRequest(ctx, in, args, replay)
					//fmt.Printf("%d send vote rquest %d ret : %v\n", rf.me, index, ret,)
					fmt.Println("\nterm ",  rf.me, rf.currentTerm,  in, *replay)

					// response  handler
					select {
					case <-ctx.Done():
						return
					default:
					}
					// todo  should use respChan := make(chan *response, bufferLength)
					// to handle the response pipeline to avoid using lock and lockNum
					// variables.

					// update self currentTerm
					lock.Lock()
					if replay.Term > rf.currentTerm {
						//return
						rf.updateCurrentTerm(replay.Term)
						rf.updateState(FollowerState)
						rf.votedFor = -1
						//fmt.Println("*******************************")
					}

					lock.Unlock()

					lockNum.Lock()
					if replay.VoteGranted {
						atomic.AddInt32(&number, int32(1))
						fmt.Println("numebr: ", number)
					}
					lockNum.Unlock()


				}(*peer, index)
			}
		}
		// todo first I do not want to use lock, assume number get the old value,
		// select condition may block always, if add `default` case for select
		// it will solve the above problem, but select may just case default and -<timeChan
		// Whether it will bring Performance impact.
		// to make programer more beautifual, I decide to use default rather than lock
		lockNum.Lock()
		fmt.Println("num", number)
		if number >= int32(rf.QuorumSize()) {
			rf.updateState(LeaderState)
			lockNum.Unlock()
			return
		}
		lockNum.Unlock()

		select {
		case <-timeChan:
			//cancel()
			//fmt.Println("cancel func *(****************", "sl;eep : ", rf.me)
			voteFor = true
			timeChan = randTime(int64(500), int64(800))
			//time.Sleep(time.Duration(1000) * time.Millisecond)
		default:
		}
		select {
		default:
		case <-rf.exitFlag:
			return
		case event := <-rf.blockEventQ:
			//fmt.Println("888343535353636363636363636363636")
			// just like followers, but never update timeChan
			var (
				err error = nil
			)

			switch args := event.args.(type) {
			case requestVoteArgs:
				//var rp responseVoteArgs
				// candicate handle the vote args never success, because it vote it self
				// and ignore the update field
				rp, _ := rf.RequestVoteResponse(&args)
				fmt.Println("response vote:****************** ", rf.me)
				rpPtr := event.reply.(*responseVoteArgs)
				*rpPtr = rp
			default:
				err = errors.New("follower receve unsupport message")
			}
			// rpc select the err chan to judge whether response over
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
