package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu         sync.Mutex
	me         int64
	lastLeader int
	lastIndex  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.me = nrand()
	DPrintf("[NewCk]\t%d, %d", ck.me, ck.me%23)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	value := ""
	args := GetArgs{}
	// args.Ck = ck.me
	args.Key = key
	ck.mu.Lock()
	// ck.lastIndex++
	// args.Index = ck.lastIndex
	lastLeader := ck.lastLeader
	ck.mu.Unlock()
	for {
		reply := GetReply{}
		ok := ck.servers[lastLeader].Call("KVServer.Get", &args, &reply)
		DPrintf("[CkGet]\tck %d to server %d, ok=%v, key=%v, Err=%v", ck.me%23, reply.ServerId, ok, key, reply.Err)
		if !ok || reply.Err == ErrWrongLeader {
			lastLeader = (lastLeader + 1) % len(ck.servers)
		} else if reply.Err == ErrNoKey {
			break
		} else if reply.Err == OK {
			value = reply.Value
			break
		} // else ErrTryAgain
	}
	ck.mu.Lock()
	ck.lastLeader = lastLeader
	ck.mu.Unlock()
	return value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.Ck = ck.me
	args.Key = key
	args.Value = value
	args.Op = op
	ck.mu.Lock()
	ck.lastIndex++
	args.Index = ck.lastIndex
	lastLeader := ck.lastLeader
	ck.mu.Unlock()
	for {
		reply := PutAppendReply{}
		ok := ck.servers[lastLeader].Call("KVServer.PutAppend", &args, &reply)
		DPrintf("[CkPutAppend]\tck %d to server %d, ok=%v, Err=%v, key=%v, val=%v, op=%v",
			ck.me%23, reply.ServerId, ok, reply.Err, key, value, op)
		if !ok || reply.Err == ErrWrongLeader {
			lastLeader = (lastLeader + 1) % len(ck.servers)
		} else if reply.Err == OK {
			break
		} // else ErrTryAgain
	}
	ck.mu.Lock()
	ck.lastLeader = lastLeader
	ck.mu.Unlock()
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
