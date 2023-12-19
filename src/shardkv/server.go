package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
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
	Op           string
	Key          string
	Value        string
	Ck           int64
	CkIndex      int
	ConfigNum    int
	Shard        ShardData
	ConfigShards [shardctrler.NShards]int
	ConfigGroups map[int][]string
}

type OpRes struct {
	index     int
	term      int
	replyCond chan string
	err       Err
}

type ShardData struct {
	Alive     bool // only UpdateShard can set Alive to true, NewConfig can set it to false
	ShardNum  int
	ConfigNum int
	CkLastId  map[int64]int
	CkIdStat  map[int64]map[int]bool
	KVdata    map[string]string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	persister    *raft.Persister
	applyCh      chan raft.ApplyMsg
	dead         int32
	appliedId    int
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	config       shardctrler.Config
	legacyConfig shardctrler.Config
	configMu     sync.RWMutex
	ctrlers      []*labrpc.ClientEnd
	mck          *shardctrler.Clerk
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	resQueue    []*OpRes
	shardData   [shardctrler.NShards]ShardData
	legacyData  [shardctrler.NShards]ShardData    // last shardData may be acquired by others
	shardRWLock [shardctrler.NShards]sync.RWMutex // only protect RW for configNum and legacyData
}

func (kv *ShardKV) TransferShard(args *TransferShardArgs, reply *TransferShardReply) {
	kv.shardRWLock[args.ShardNum].RLock()
	if kv.legacyData[args.ShardNum].ConfigNum != args.ConfigNum {
		kv.shardRWLock[args.ShardNum].RUnlock()
		reply.Err = ErrTryAgain
		return
	}

	reply.Err = OK
	reply.Shard.ShardNum = args.ShardNum
	reply.Shard.ConfigNum = args.ConfigNum
	reply.Shard.CkLastId = make(map[int64]int)
	reply.Shard.CkIdStat = make(map[int64]map[int]bool)
	reply.Shard.KVdata = make(map[string]string)
	for ck, id := range kv.legacyData[args.ShardNum].CkLastId {
		reply.Shard.CkLastId[ck] = id
	}
	for ck, ckStat := range kv.legacyData[args.ShardNum].CkIdStat {
		reply.Shard.CkIdStat[ck] = make(map[int]bool)
		for id, stat := range ckStat {
			reply.Shard.CkIdStat[ck][id] = stat
		}
	}
	for key, value := range kv.legacyData[args.ShardNum].KVdata {
		reply.Shard.KVdata[key] = value
	}
	kv.shardRWLock[args.ShardNum].RUnlock()
}

func (kv *ShardKV) requestTransferShard(servers []string, shardNum int, configNum int) {
	args := TransferShardArgs{}
	args.ConfigNum = configNum
	args.ShardNum = shardNum
	for !kv.killed() {
		// check whether done applying
		kv.shardRWLock[shardNum].RLock()
		if kv.shardData[shardNum].ConfigNum > configNum {
			kv.shardRWLock[shardNum].RUnlock()
			return
		}
		kv.shardRWLock[shardNum].RUnlock()

		if len(servers) == 0 {
			// start a new shard
			op := Op{}
			op.Op = "UpdateShard"
			op.Shard = ShardData{}
			op.Shard.ConfigNum = configNum
			op.Shard.ShardNum = shardNum
			op.Shard.CkLastId = make(map[int64]int)
			op.Shard.CkIdStat = make(map[int64]map[int]bool)
			op.Shard.KVdata = make(map[string]string)
			kv.rf.Start(op)
		}
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply TransferShardReply
			ok := srv.Call("ShardKV.TransferShard", &args, &reply)
			if ok && reply.Err == OK {
				// put shardData into raft, then wait for raft reply (?)
				op := Op{}
				op.Op = "UpdateShard"
				op.Shard = reply.Shard
				kv.rf.Start(op)
				break
			}
			// ... not ok, or ErrWrongLeader
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	shard := key2shard(args.Key)
	kv.configMu.RLock()
	// only read config.Num and config.Shards, left config.Groups not touched
	config := kv.config
	kv.configMu.RUnlock()

	// check group before raft. Will check again after raft
	if kv.gid != config.Shards[shard] {
		reply.Err = ErrWrongGroup
		return
	}

	op := Op{}
	op.Op = "Get"
	op.Key = args.Key
	op.ConfigNum = config.Num
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
	kv.mu.Unlock()

	DPrintf("[ServerGet]\t(%d, %d) received Get id=%d, term=%d; for key=%v", kv.gid, kv.me, index, term, args.Key)
	reply.Value = <-opReply.replyCond
	reply.Err = opReply.err
	reply.ServerId = kv.me
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	shard := key2shard(args.Key)
	kv.configMu.RLock()
	config := kv.config
	kv.configMu.RUnlock()
	if kv.gid != config.Shards[shard] {
		reply.Err = ErrWrongGroup
		return
	}

	op := Op{}
	op.Op = args.Op
	op.Key = args.Key
	op.Value = args.Value
	op.Ck = args.Ck
	op.CkIndex = args.Index
	op.ConfigNum = config.Num
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
	kv.mu.Unlock()
	// DPrintf("[ServerPutAppend]\t(%d, %d) received PutAppend from %d, ckid=%d, id=%d, term=%d; for %v key=%v, value=%v",
	// kv.gid, kv.me, args.Ck%23, args.Index, index, term, args.Op, args.Key, args.Value)

	<-opReply.replyCond
	reply.Err = opReply.err
	reply.ServerId = kv.me
	// DPrintf("[ServerPutAppendDone]\t(%d, %d) received from %d, ckid=%d, id=%d, term=%d; for %v key=%v, value=%v",
	// kv.gid, kv.me, args.Ck%23, args.Index, index, term, args.Op, args.Key, args.Value)
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) coordinator() {
	for msg := range kv.applyCh {
		kv.mu.Lock()
		if msg.SnapshotValid && msg.Snapshot != nil && len(msg.Snapshot) > 0 {
			snapshot := msg.Snapshot
			r := bytes.NewBuffer(snapshot)
			d := labgob.NewDecoder(r)
			var shardData [shardctrler.NShards]ShardData
			var legacyData [shardctrler.NShards]ShardData
			var config shardctrler.Config
			var legacyConfig shardctrler.Config
			if d.Decode(&shardData) != nil ||
				d.Decode(&legacyData) != nil ||
				d.Decode(&config) != nil ||
				d.Decode(&legacyConfig) != nil {
				//   error...
				log.Fatalf("kv missing snapshot")
			} else {
				for i := range kv.shardRWLock {
					kv.shardRWLock[i].Lock()
				}
				kv.shardData = shardData
				kv.legacyData = legacyData
				for i := range kv.shardRWLock {
					kv.shardRWLock[i].Unlock()
				}

				kv.configMu.Lock()
				kv.config = config
				kv.legacyConfig = legacyConfig
				kv.configMu.Unlock()
			}
			DPrintf("[ApplySnapshot]\t(%d, %d) applying snapshot id=%d, term=%d", kv.gid, kv.me, msg.SnapshotIndex, msg.SnapshotTerm)
			kv.appliedId = msg.SnapshotIndex
			for len(kv.resQueue) > 0 {
				kv.resQueue[0].err = ErrTryAgain
				if msg.SnapshotIndex < kv.resQueue[0].index {
					break
				}
				go func(reply *OpRes, res string) {
					reply.replyCond <- res
					// DPrintf("[ServerDropReply]\t(%d, %d) dropped reply index=%d, term=%d", kv.gid, kv.me, reply.index, reply.term)
				}(kv.resQueue[0], "")
				kv.resQueue = kv.resQueue[1:]
			}
		}
		if msg.CommandValid {
			var value string
			var err Err = OK
			// DPrintf("[MsgApply]\t(%d, %d) applying id=%d, term=%d", kv.gid, kv.me, msg.CommandIndex, msg.CommandTerm)
			kv.appliedId++
			if msg.CommandIndex != kv.appliedId {
				log.Fatalf("server %d should apply %d, but it was applying %d", kv.me, kv.appliedId, msg.CommandIndex)
			}
			cmd := msg.Command.(Op)
			op := cmd.Op
			if op == "NewConfig" {
				// first check whether every shard is up to date
				kv.configMu.RLock()
				configNum := kv.config.Num
				shardsReady := true

				for i := range cmd.ConfigShards {
					// visit ConfigNum needs lock
					kv.shardRWLock[i].RLock()
					if kv.shardData[i].ConfigNum < configNum {
						shardsReady = false
						servers := kv.legacyConfig.Groups[kv.legacyConfig.Shards[i]]
						go kv.requestTransferShard(servers, i, kv.shardData[i].ConfigNum)
					}
					kv.shardRWLock[i].RUnlock()
				}
				kv.configMu.RUnlock()

				if shardsReady && cmd.ConfigNum == configNum+1 {
					kv.configMu.Lock()
					for i, gid := range cmd.ConfigShards {
						kv.shardRWLock[i].Lock()
						if kv.shardData[i].ConfigNum != configNum {
							log.Fatalf("Unexpected config num %d on shard %d group %d, while waiting for %d",
								kv.shardData[i].ConfigNum, i, gid, configNum)
						}
						if kv.shardData[i].Alive {
							// shardsReady suggests kv.config.Shards[i] == gid iff kv.shardData[i].Alive
							// else kv.shardData[i].ConfigNum would be less then kv.config.Num
							if gid != kv.gid {
								// the last version stop serving, turns into legacyData
								kv.shardData[i].Alive = false
								kv.legacyData[i].ShardNum = kv.shardData[i].ShardNum
								kv.legacyData[i].ConfigNum = kv.shardData[i].ConfigNum
								kv.legacyData[i].CkLastId = make(map[int64]int)
								kv.legacyData[i].CkIdStat = make(map[int64]map[int]bool)
								kv.legacyData[i].KVdata = make(map[string]string)
								for ck, id := range kv.shardData[i].CkLastId {
									kv.legacyData[i].CkLastId[ck] = id
								}
								for ck, ckStat := range kv.shardData[i].CkIdStat {
									kv.legacyData[i].CkIdStat[ck] = make(map[int]bool)
									for id, stat := range ckStat {
										kv.legacyData[i].CkIdStat[ck][id] = stat
									}
								}
								for key, value := range kv.shardData[i].KVdata {
									kv.legacyData[i].KVdata[key] = value
								}
							}
							kv.shardData[i].ConfigNum++
						} else if gid == kv.gid {
							// request for shard from other groups
							servers := kv.config.Groups[kv.config.Shards[i]]
							go kv.requestTransferShard(servers, i, kv.shardData[i].ConfigNum)
							DPrintf("[RequestShard]\t(%d, %d) requesting from (%d: %v) for shard %d, config %d",
								kv.gid, kv.me, kv.config.Shards[i], servers, i, configNum)
						} else {
							// shard move between other groups
							kv.shardData[i].ConfigNum++
						}
						kv.shardRWLock[i].Unlock()
					}
					kv.legacyConfig.Num++
					kv.legacyConfig.Shards = kv.config.Shards
					kv.legacyConfig.Groups = kv.config.Groups

					kv.config.Num++
					kv.config.Shards = cmd.ConfigShards
					kv.config.Groups = make(map[int][]string)
					for gid, servers := range cmd.ConfigGroups {
						kv.config.Groups[gid] = servers
					}
					kv.configMu.Unlock()
					DPrintf("[NewConfig]\t(%d, %d) accepted NewConfig %d, Shards: %v", kv.gid, kv.me, cmd.ConfigNum, cmd.ConfigShards)
				} else {
					DPrintf("[ConfigAbort]\t(%d, %d) aborted config %d, Shards: %v, using config %d", kv.gid, kv.me, cmd.ConfigNum, cmd.ConfigShards, configNum)
				}
			} else if op == "UpdateShard" {
				DPrintf("[UpdateShard]\t(%d, %d) received shard %d with config %d", kv.gid, kv.me, cmd.Shard.ShardNum, cmd.Shard.ConfigNum)
				// should only read cmd.Shard
				shardNum := cmd.Shard.ShardNum
				kv.shardRWLock[shardNum].Lock()
				if cmd.Shard.ConfigNum == kv.shardData[shardNum].ConfigNum {
					kv.shardData[shardNum].ConfigNum++
					kv.shardRWLock[shardNum].Unlock()
					kv.shardData[shardNum].Alive = true
					// kv.shardData[shardNum].ShardNum = cmd.Shard.ShardNum
					kv.shardData[shardNum].CkLastId = make(map[int64]int)
					kv.shardData[shardNum].CkIdStat = make(map[int64]map[int]bool)
					kv.shardData[shardNum].KVdata = make(map[string]string)
					for ck, id := range cmd.Shard.CkLastId {
						kv.shardData[shardNum].CkLastId[ck] = id
					}
					for ck, ckStat := range cmd.Shard.CkIdStat {
						kv.shardData[shardNum].CkIdStat[ck] = make(map[int]bool)
						for id, stat := range ckStat {
							kv.shardData[shardNum].CkIdStat[ck][id] = stat
						}
					}
					for key, value := range cmd.Shard.KVdata {
						kv.shardData[shardNum].KVdata[key] = value
					}
				} else {
					kv.shardRWLock[shardNum].Unlock()
				}
			} else {
				// Get, Put or Append
				shard := key2shard(cmd.Key)
				// check ErrWrongGroup
				kv.shardRWLock[shard].RLock()
				configReady := kv.shardData[shard].Alive && (kv.shardData[shard].ConfigNum == cmd.ConfigNum)
				kv.shardRWLock[shard].RUnlock()

				if configReady {
					if op == "Get" {
						val, ok := kv.shardData[shard].KVdata[cmd.Key]
						value = val
						if !ok {
							err = ErrNoKey
						}
					} else {
						// check whether PutAppend are repeated
						repeated := false
						if cmd.CkIndex <= kv.shardData[shard].CkLastId[cmd.Ck] {
							if !kv.shardData[shard].CkIdStat[cmd.Ck][cmd.CkIndex] {
								repeated = true
							} else {
								delete(kv.shardData[shard].CkIdStat[cmd.Ck], cmd.CkIndex)
							}
						} else {
							if _, ok := kv.shardData[shard].CkIdStat[cmd.Ck]; !ok {
								kv.shardData[shard].CkIdStat[cmd.Ck] = make(map[int]bool)
							}
							for i := kv.shardData[shard].CkLastId[cmd.Ck] + 1; i < cmd.CkIndex; i++ {
								kv.shardData[shard].CkIdStat[cmd.Ck][i] = true
							}
							kv.shardData[shard].CkLastId[cmd.Ck] = cmd.CkIndex
						}
						if !repeated {
							switch op {
							case "Put":
								kv.shardData[shard].KVdata[cmd.Key] = cmd.Value
							case "Append":
								kv.shardData[shard].KVdata[cmd.Key] += cmd.Value
							}
						}
					}
				} else {
					err = ErrWrongGroup
				}
			}
			for len(kv.resQueue) > 0 {
				kv.resQueue[0].err = ErrTryAgain
				if msg.CommandIndex < kv.resQueue[0].index {
					break
				} else if msg.CommandIndex == kv.resQueue[0].index && msg.CommandTerm == kv.resQueue[0].term {
					kv.resQueue[0].err = err
				} // else not reply in this server
				go func(reply *OpRes, res string) {
					reply.replyCond <- res
					// DPrintf("[ServerReplyDone]\t(%d, %d) replied index=%d, term=%d, Err=%v", kv.gid, kv.me, reply.index, reply.term, reply.err)
				}(kv.resQueue[0], value)
				kv.resQueue = kv.resQueue[1:]
			}
			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
				// take snapshot
				index := msg.CommandIndex
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				for i := range kv.shardRWLock {
					kv.shardRWLock[i].Lock()
				}
				e.Encode(kv.shardData)
				e.Encode(kv.legacyData)
				for i := range kv.shardRWLock {
					kv.shardRWLock[i].Unlock()
				}
				kv.configMu.Lock()
				e.Encode(kv.config)
				e.Encode(kv.legacyConfig)
				kv.configMu.Unlock()
				snapshot := w.Bytes()
				kv.rf.Snapshot(index, snapshot)
			}
		}
		kv.mu.Unlock()
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.persister = persister
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// polling config
	go func() {
		for !kv.killed() {
			kv.configMu.RLock()
			configNum := kv.config.Num
			kv.configMu.RUnlock()
			config := kv.mck.Query(configNum + 1)
			if config.Num > configNum {
				op := Op{}
				op.Op = "NewConfig"
				op.ConfigNum = config.Num
				op.ConfigShards = config.Shards
				op.ConfigGroups = config.Groups
				kv.rf.Start(op) // if this server is the leader, it may success
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	go kv.coordinator()

	return kv
}
