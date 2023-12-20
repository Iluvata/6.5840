package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm *shardctrler.Clerk
	// config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	mu             sync.Mutex
	me             int64
	shardLastIndex [shardctrler.NShards]int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.me = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	shard := key2shard(key)
	config := ck.sm.Query(-1)

	for {
		args.ConfigNum = config.Num
		res := make(chan GetReply)
		quit := make(chan bool)
		go func(config shardctrler.Config, args GetArgs, res chan GetReply, quit chan bool) {
			gid := config.Shards[shard]
			if servers, ok := config.Groups[gid]; ok {
				DPrintf("[CkGet]\t%d to group %d, key=%v", ck.me%23, gid, key)
				// try each server for the shard.
				for si := 0; si < len(servers); si++ {
					srv := ck.make_end(servers[si])
					var reply GetReply
					ok := srv.Call("ShardKV.Get", &args, &reply)
					if ok && (reply.Err == OK || reply.Err == ErrNoKey || reply.Err == ErrWrongGroup) {
						select {
						case res <- reply:
							return
						case <-quit:
							return
						}
					} // else ErrWrongLeader or ErrTryAgain or not ok
				}
			}
			var reply GetReply
			reply.Err = ErrTryAgain
			select {
			case res <- reply:
				return
			case <-quit:
				return
			}
		}(config, args, res, quit)
		select {
		case reply := <-res:
			if reply.Err == OK || reply.Err == ErrNoKey {
				return reply.Value
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		case <-time.After(100 * time.Millisecond):
			close(quit)
		}
		// ask controler for the latest configuration.
		config = ck.sm.Query(-1)
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.Ck = ck.me
	shard := key2shard(key)
	ck.mu.Lock()
	ck.shardLastIndex[shard]++
	args.Index = ck.shardLastIndex[shard]
	ck.mu.Unlock()
	config := ck.sm.Query(-1)

	for {
		args.ConfigNum = config.Num
		done := make(chan bool)
		quit := make(chan bool)
		go func(config shardctrler.Config, args PutAppendArgs, done chan bool, quit chan bool) {
			gid := config.Shards[shard]
			if servers, ok := config.Groups[gid]; ok {
				DPrintf("[CkPutAppend]\t%d %v to group %d, ckid=%d, key=%v, value=%v", ck.me%23, op, gid, args.Index, key, value)
				for si := 0; si < len(servers); si++ {
					srv := ck.make_end(servers[si])
					var reply PutAppendReply
					ok := srv.Call("ShardKV.PutAppend", &args, &reply)
					if ok && reply.Err == OK {
						select {
						case done <- true:
							return
						case <-quit:
							return
						}
					}
					if ok && reply.Err == ErrWrongGroup {
						break
					}
					// ... not ok, or ErrWrongLeader
				}
			}
			select {
			case done <- false:
				return
			case <-quit:
				return
			}
		}(config, args, done, quit)
		select {
		case ok := <-done:
			if ok {
				return
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		case <-time.After(100 * time.Millisecond):
			close(quit)
		}
		// ask controler for the latest configuration.
		config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
