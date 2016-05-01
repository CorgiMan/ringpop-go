// Copyright (c) 2015 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Package hashring provides a hashring implementation that uses a red-black
// Tree.
package hashring

import (
	"fmt"
	"sync"

	// "github.com/CorgiMan/ringpop-go/events"
	// "github.com/CorgiMan/ringpop-go/logging"

	// "github.com/uber-common/bark"
)

// add, remove, addremove, checksum

type Server struct {
	hostport string
}

type HashRing struct {
	sync.Mutex
	hashes, swap  []uint64
	serversByHash map[uint64][]*Server
	servers       []*Server
	hashFunc      func(string) uint64
}

// Config for hashring package.
var Config struct {
	ReplicaPoints uint8 `default:"3"`
}

func init() {
	// cfg.Fill(&Config)
	Config.ReplicaPoints = 100
}

func New(hashFunc func([]byte) uint64) *HashRing {
	r := &HashRing{}
	r.hashFunc = func(s string) uint64 { return hashFunc([]byte(s)) }
	r.serversByHash = make(map[uint64][]*Server)
	return r
}

func (r *HashRing) Lookup(key string) *Server {
	ss := r.LookupNUnique(key, 1)
	if len(ss) == 0 {
		return nil
	}
	return ss[0]
}

func (r *HashRing) LookupNUnique(key string, n int) []*Server {
	// don't defer Unlock for performance
	r.Lock()

	S := make(map[*Server]struct{})

	hashFunc := r.hashFunc(key)
	ix := indexOf(r.hashes, hashFunc)
	for i := 0; i < len(r.hashes) || len(S) >= n; i++ {
		hash := r.hashes[(ix+i)%len(r.hashes)]
		servers := r.serversByHash[hash]
		for _, s := range servers {
			S[s] = struct{}{}
		}
	}

	var servers []*Server
	for s := range S {
		servers = append(servers, s)
	}

	r.Unlock()
	return servers
}

func (r *HashRing) AddRemove(add, rm []*Server) {
	r.Lock()
	defer r.Unlock()

	for _, s := range add {
		r.addNoSort(s)
	}

	for _, s := range rm {
		r.remove(s)
	}

	radixSort(r.hashes, r.swap)
}

func (r *HashRing) addNoSort(s *Server) {
	r.servers = append(r.servers, s)

	hs := hashes(s, int(Config.ReplicaPoints), r.hashFunc)
	r.hashes = append(r.hashes, hs...)
	r.swap = append(r.swap, hs...)

	for _, h := range hs {
		r.serversByHash[h] = append(r.serversByHash[h], s)
	}
}

func (r *HashRing) remove(s *Server) {
	r.servers = removeServer(r.servers, s)

	hs := hashes(s, int(Config.ReplicaPoints), r.hashFunc)
	r.hashes = remove(r.hashes, hs)
	r.swap = r.swap[:len(r.hashes)]

	for _, h := range hs {
		r.serversByHash[h] = removeServer(r.serversByHash[h], s)
	}
}

func (r *HashRing) Checksum() (c uint64) {
	r.Lock()
	defer r.Unlock()

	for _, h := range r.hashes {
		c ^= h
	}
	return
}

func hashes(s *Server, n int, hash func(string) uint64) []uint64 {
	var r []uint64
	for i := 0; i < n; i++ {
		h := hash(fmt.Sprintf("%s:%d", s.hostport, i))
		r = append(r, h)
	}
	return r
}

func removeServer(slice []*Server, s *Server) []*Server {
	for i := 0; i < len(slice); i++ {
		if slice[i] == s {
			slice[i] = slice[len(slice)-1]
			slice = slice[:len(slice)-1]
			i--
		}
	}
	return slice
}
