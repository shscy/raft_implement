package raft

import (
	"sync"
//	"fmt"
)

// raft log module

// explain the significance of the existence of the index field
// while we do not use snapshot, that is ok,
// the value of the Index field of the structure EntryLog is coincides with the index of the log.entries

type EntryLog struct {
	Index   int
	Term    int
	//command interface{}
}

// log record the logs of the server
// explain the the meaning of the existence of the mutex field of the structure log
// assume there 4 servers(a b c d) in the raft, the four nodes b,c,d and e communicate with
// each othe and a is the leader; d,e tow nodes communicate but e is not connected to b, c, a nodes
// so e always send requestVote rpc and leader a send AppendEntryRequest rpc request to d
type log struct {
	mutex sync.Mutex // protect following fields

	entries    []EntryLog
	length     int
	startIndex int
	startTerm  int
}

func (l *log) addEntry(entry EntryLog){
	l.mutex.Lock()
	l.length += 1
	l.entries = append(l.entries, entry)
	l.mutex.Unlock()
}

func (l *log) lastInfo() (index int, term int) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if l.length == 0 {
		return l.startIndex, l.startTerm
	}
	entry := l.entries[l.length-1]
	index, term = entry.Index, entry.Term
	return
}

func (l *log) logIndex(curLogIndex, delay int) int{
	for i, v := range l.entries {
		if v.Index == curLogIndex {
			return i+delay
		}
	}
	//fmt.Println("[Raft:Error] logindex 0")
	return 0

}




type LogClientMessage struct {
	commandLog interface{}
}
