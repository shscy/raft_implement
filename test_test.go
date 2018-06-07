package raft

import (
	"testing"
	//"time"
	"fmt"
	"runtime"
	"sync"
	"time"
)

func TestVoteLeader(t *testing.T) {
	//num := 0
	runtime.GOMAXPROCS(runtime.NumCPU())
	var wait sync.WaitGroup
	var num int
	for num = 0; num < 6000; num++ {
		wait.Add(1)
		var once sync.Once
		k := num
		go func() {
			n := 7
			// make_config default have
			conf := make_config(n)

			// before start server , patch the server state
			conf.rafts[0].log.addEntry(EntryLog{Index: 1, Term: 1})
			conf.rafts[0].currentTerm = 1

			conf.rafts[1].log.addEntry(EntryLog{Index: 1, Term: 1})
			conf.rafts[1].currentTerm = 1

			conf.rafts[4].log.addEntry(EntryLog{Index: 1, Term: 1})
			conf.rafts[4].currentTerm = 1

			conf.rafts[6].log.addEntry(EntryLog{Index: 1, Term: 1})
			conf.rafts[6].currentTerm = 1

			for _, v := range conf.rafts {
				v.testIndex = k
			}

			//fmt.Println("num :: ", k)
			store := make(map[int][]int)
			for i, _ := range conf.rafts {
				// here range with clojure will result the bug
				j := i

				conf.rafts[j].updateStateFunc = []func(){
					func() {
						//watch the leader state
						//fmt.Println("leader hoook before ", conf.rafts[j].me, conf.rafts[j].state, j)
						if conf.rafts[j].state == LeaderState {
							//fmt.Println("leader hook---------------------------", k,conf.rafts[j].me, conf.rafts[j].currentTerm)
							key := conf.rafts[j].me
							//if value, ok:= store[key]; ok{
							//	fmt.Println("debug ----------------------: before", value[0], value[1], value[2])
							//	fmt.Println("Debug ----------------------: after ", k, conf.rafts[j].me, conf.rafts[j].state)
							//} else {
							store[key] = make([]int, 3)
							store[key][0] = conf.rafts[j].me
							store[key][1] = conf.rafts[j].state
							store[key][2] = conf.rafts[j].currentTerm
							//
							//if len(store) > 1 {
							//	for ks, vs := range store{
							//		fmt.Println("length", len(store), ks, " :: ", vs)
							//	}
							//}
							once.Do(func() {
								close(conf.exit)
								time.Sleep(time.Duration(1000) * time.Second)
								for _, v := range conf.rafts {
									close(v.exitFlag)
								}
							})

						}
					},
				}
			}

			conf.startWithGoroutine()
			<-conf.exit
			var leaders []int
			count := 0
			for _, v := range conf.rafts {
				if v.state == LeaderState {
					leaders = append(leaders, v.me)
					count += 1
				}
			}
			if count != 1 {
				t.Error("two leader", leaders, k)
			}
			leader := leaders[0]
			if !(leader == 0 || leader == 1 || leader == 4 || leader == 6) {
				t.Error("vote leader,it should be 0|1|4|6, but is", leader, k)
			}
			wait.Done()

		}()
	}
	wait.Wait()

}
func Network() {
	runtime.GOMAXPROCS(4)
	n := 3
	//in := requestVoteArgs{}
	//res := &responsseVoteArgs{}
	conf := make_config(n)

	timeChan := randTime(int64(300), int64(350))

	for {
		select {
		case <-timeChan:
			for i := 0; i < n; i++ {
				fmt.Printf("\n[Debug]:  %d state : %d   ||  ", i, conf.rafts[i].State())
				if conf.rafts[i].state == LeaderState {
					fmt.Println("Leader -------->>>>>>>>>>>>>leader is []", i)
				}

				fmt.Printf("\n[Debug]: %d log length %d\n", i, len(conf.rafts[i].log.entries))
			}
			fmt.Println()
			timeChan = randTime(int64(100), int64(1500))
		case <-conf.exit:
			fmt.Println("get ctrl + C signal ----------------------")
			return
		}
	}
}
