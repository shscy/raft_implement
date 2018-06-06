package raft

import (
	"labrpc"
	"os"
	"sync"

	"fmt"
	"os/signal"
	"syscall"
)

type testConfig struct {
	n         int
	net       *labrpc.Network
	rafts     []*Raft
	endnames  map[int][]string // client end <=> server
	group     sync.WaitGroup
	exit      chan struct{}
	startFunc []func()
}

func (t *testConfig) startWithGoroutine() {
	for _, f := range t.startFunc {
		go f()
	}
}

func make_config(n int) *testConfig {
	//fmt.Println("ranutmn :", )
	conf := &testConfig{n: n}
	conf.endnames = make(map[int][]string)
	conf.net = labrpc.MakeNetwork()
	conf.rafts = make([]*Raft, n)
	conf.exit = make(chan struct{}, 1)

	for i := 0; i < n; i++ {
		conf.rafts[i] = conf.startServer(i)
	}

	//for name := range conf.endnames {
	for j := 0; j < n; j++ {
		names := conf.endnames[j]
		for index, name := range names {
			//for k := 0; k < n; k ++ {
			//fmt.Println("Name", name, index)
			conf.net.Connect(name, index)
			//}
		}
	}
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGQUIT)
		s := <-c
		//conf.exit <- struct{}{}
		close(conf.exit)
		for i := 0; i < n; i++ {
			close(conf.rafts[i].exitFlag)
		}
		fmt.Println("get signal ", s)
	}()
	return conf
}

func (c *testConfig) startServer(i int) *Raft {
	//raft := &Raft{}
	clients := make([]*labrpc.ClientEnd, c.n)
	for j := 0; j < c.n; j++ {
		name := randomString(10)
		c.endnames[i] = append(c.endnames[i], name)
		clients[j] = c.net.MakeEnd(name)
		c.net.Enable(name, true)
	}
	raft := &Raft{}
	raft.state = FollowerState
	raft.me = i
	raft.peers = clients
	raft.blockEventQ = make(chan *message, 1000)
	raft.log = &log{}
	//raft.log.entries = make([]EntryLog)
	raft.log.length = 0
	raft.log.startIndex = 0
	raft.log.startTerm = 0
	raft.currentTerm = 0

	raft.applyNotice = make(chan struct{})
	raft.exitFlag = make(chan struct{})
	raft.votedFor = -1

	server := labrpc.MakeServer()
	server.AddService(labrpc.MakeService(raft))
	c.net.AddServer(i, server)

	c.startFunc = append(c.startFunc, func() {
		raft.StateLoop()
		// loop exit
		c.net.DeleteServer(i)
		// wait exit flag chan
		raft.Wait()
	})
	return raft
}
