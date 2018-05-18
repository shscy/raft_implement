package raft

import (
	"time"
	"math/rand"
)

func randTime(min, max int64) <-chan time.Time {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	d, delta := min, (max - min)
	if delta > 0 {
		d += int64(time.Duration(r.Int63n(int64(delta))))
	}
	return time.After(time.Duration(d) * time.Millisecond)
}

func min(x,y int) int {
	if x < y {
		return x
	}
	return y
}