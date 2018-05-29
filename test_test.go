package raft

import (
	"testing"
	//"time"
	"fmt"
	"runtime"
)

func TestNetwork(t *testing.T) {
	runtime.GOMAXPROCS(4)
	n := 3
	//in := requestVoteArgs{}
	//res := &responseVoteArgs{}
	conf := make_config(n)
	//conf.net.Servers()
	//fmt.Println(conf.endnames)
	//conf.net.Connects()
	//for i := 0; i < n; i++ {
	//	for j := 0; j < n; j++ {
	//		if i != j {
	//			rf := conf.rafts[i]
	//
	//
	//			//fmt.Println("rf", rf, rf.peers)
	//			time.Sleep(time.Duration(200) * time.Millisecond)
	//			ret := rf.peers[j].Call(context.Background(), "Raft.RequestVote", in, res)
	//			fmt.Println("ret : ", ret, res)
	//		}
	//	}
	//}

	timeChan := randTime(int64(300), int64(350))

	for {
		select {
		case <-timeChan:
			for i := 0; i < n; i++ {
				fmt.Printf("%d state : %d   ||  ", i, conf.rafts[i].State())
			}
			fmt.Println()
			timeChan = randTime(int64(100), int64(1500))
		case <-conf.exit:
			fmt.Println("get ctrl + C signal ----------------------")
			return
		}
	}
}
