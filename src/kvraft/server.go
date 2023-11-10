package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cmd   string // "Get" or "Put" or "Append"
	Key   string
	Value string
}

type OpRes struct {
	index     int
	term      int
	replyCond chan string
	err       Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	resQueue []*OpRes
	KVdata   map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{}
	op.Cmd = "Get"
	op.Key = args.Key
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	opReply := OpRes{}
	opReply.index = index
	opReply.term = term
	opReply.replyCond = make(chan string)
	kv.resQueue = append(kv.resQueue, &opReply)
	DPrintf("[ServerGet]\t%d received Get id %d, term %d; for key %v", index, term, kv.me, args.Key)
	kv.mu.Unlock()

	reply.Value = <-opReply.replyCond
	reply.Err = opReply.err
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{}
	op.Cmd = args.Op
	op.Key = args.Key
	op.Value = args.Value
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	opReply := OpRes{}
	opReply.index = index
	opReply.term = term
	opReply.replyCond = make(chan string)
	kv.resQueue = append(kv.resQueue, &opReply)
	DPrintf("[ServerPutAppend]\t%d received PutAppend id %d, term %d; for %v key %v, value %v",
		kv.me, index, term, args.Op, args.Key, args.Value)
	kv.mu.Unlock()

	<-opReply.replyCond
	reply.Err = opReply.err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func replyRes(reply *OpRes, res string) {
	reply.replyCond <- res
}

func (kv *KVServer) coordinator() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.SnapshotValid {
			// TODO
		}
		if msg.CommandValid {
			kv.mu.Lock()
			DPrintf("[MsgApply]\t%d applying id %d, term %d", kv.me, msg.CommandIndex, msg.CommandTerm)
			op := msg.Command.(Op)
			value, ok := "", false
			if op.Cmd == "Get" {
				value, ok = kv.KVdata[op.Key]
			} else if op.Cmd == "Put" {
				kv.KVdata[op.Key] = op.Value
			} else if op.Cmd == "Append" {
				kv.KVdata[op.Key] += op.Value
			}
			for len(kv.resQueue) > 0 {
				kv.resQueue[0].err = ErrTryAgain
				if msg.CommandIndex < kv.resQueue[0].index {
					break
				} else if msg.CommandIndex == kv.resQueue[0].index && msg.CommandTerm == kv.resQueue[0].term {
					kv.resQueue[0].err = OK
					if op.Cmd == "Get" && !ok {
						kv.resQueue[0].err = ErrNoKey
					}
				}
				go replyRes(kv.resQueue[0], value)
				kv.resQueue = kv.resQueue[1:]
			}
			kv.mu.Unlock()
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.KVdata = make(map[string]string)

	// You may need initialization code here.
	go kv.coordinator()

	return kv
}
