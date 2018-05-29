package raft

import (
	crand "crypto/rand"
	"encoding/base64"
	"math/rand"
	"time"
)

func randTime(min, max int64) <-chan time.Time {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	d, delta := min, (max - min)
	if delta > 0 {
		d += int64(time.Duration(r.Int63n(int64(delta))))
	}
	return time.After(time.Duration(d) * time.Millisecond)
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

var randomString func(n int) string

func init() {
	randomString = __randomString()
}

func __randomString() func(n int) string {
	d := make(map[string]struct{})
	v := struct{}{}
	return func(n int) string {
		for {
			b := make([]byte, 2*n)
			crand.Read(b)
			s := base64.URLEncoding.EncodeToString(b)[:n]
			if _, ok := d[s]; !ok {
				d[s] = v
				return s
			}
		}
	}
}
