package raft

import (
	"time"
	"errors"
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

type Raft struct {

	me int

	currentTerm int
	votedFor int
	log *log

	commitIndex int
	lastApplied int

	//follow two fields for leader
	nextIndex []int
	matchIndex []int

	// buffered channel
	// explain why using blockEventQ channel
	// server is only in a single state, each state Concurrently handle different rpc request
	// use channel we can decrease using the lock to protect state,logs etc
	blockEventQ chan *message
	electionTimeout time.Duration // electionTimeout election timeout
	state int  // state of the
	applyNotice chan struct{}
	exitFlag chan struct{} // stop signal channel, while stop, close(exitFlag)
}

func (rf *Raft) updateState(state int) {
	rf.state = state
}


func (rf *Raft) LeaderLoop() {
	// todo leader state loop
}

func (rf *Raft) FollowerLoop() {
	// todo
	timeoutChan := randTime(int64(150), int64(300))

	for rf.state == FollowerState {
		select {
		case <-rf.exitFlag:
			return
		case event := <-rf.blockEventQ:
			var (
				err error
				update bool
			)

			switch args := event.args.(type){
			case *requestVoteArgs:
				var rp responseVoteArgs
				rp, update = rf.RequestVoteResponse(args)
				event.reply = rp
			default:
				err = errors.New("follower receve unsupport message")
			}
			// rpc select the err chan to judge whether response over
			event.err <- err
			if update {
				timeoutChan = randTime(int64(150), int64(300))
			}
		case <-timeoutChan:
				// become to candicate
				return
		}

	}
}

func (rf *Raft) CandicateLoop() {
	// todo
}

// StateLoop
func(rf *Raft) StateLoop() {

	for{
		// exit the server
		select{
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
