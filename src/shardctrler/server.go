package shardctrler

import (
	"bytes"
	"log"
	"sync"

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

type OpRes struct {
	index  int
	term   int
	done   chan bool
	config Config
	err    Err
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	appliedId int
	resQueue  []*OpRes
	ckLastId  map[int64]int
	ckIdStat  map[int64]map[int]bool // state of PutAppend RPC. for those index less then lastId, true for haven't received, false (missing) in the map for process done
	persister *raft.Persister

	maxraftstate int // snapshot if log grows this big

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Op      string
	Ck      int64
	CkIndex int
	// query args
	Num int
	// join args
	Servers map[int][]string
	// leave args
	GIDs []int
	// move args
	Shard int
	GID   int
}

func (sc *ShardCtrler) JoinLeaveMove(op string, args interface{}, reply interface{}) {
	cmd := Op{}
	cmd.Op = op
	switch op {
	case "Join":
		joinArgs := args.(*JoinArgs)
		cmd.Ck = joinArgs.Ck
		cmd.CkIndex = joinArgs.Index
		cmd.Servers = joinArgs.Servers
	case "Leave":
		leaveArgs := args.(*LeaveArgs)
		cmd.Ck = leaveArgs.Ck
		cmd.CkIndex = leaveArgs.Index
		cmd.GIDs = leaveArgs.GIDs
	case "Move":
		moveArgs := args.(*MoveArgs)
		cmd.Ck = moveArgs.Ck
		cmd.CkIndex = moveArgs.Index
		cmd.Shard = moveArgs.Shard
		cmd.GID = moveArgs.GID
	}
	sc.mu.Lock()
	index, term, isLeader := sc.rf.Start(cmd)
	if !isLeader {
		sc.mu.Unlock()
		switch op {
		case "Join":
			reply.(*JoinReply).WrongLeader = true
		case "Leave":
			reply.(*LeaveReply).WrongLeader = true
		case "Move":
			reply.(*MoveReply).WrongLeader = true
		}
		return
	}
	opReply := OpRes{}
	opReply.index = index
	opReply.term = term
	opReply.done = make(chan bool)
	sc.resQueue = append(sc.resQueue, &opReply)
	sc.mu.Unlock()
	DPrintf("[ServerJoinLeaveMove]\t%d received %v, id=%d, term=%d",
		sc.me, op, index, term)

	<-opReply.done
	switch op {
	case "Join":
		reply.(*JoinReply).Err = opReply.err
		reply.(*JoinReply).ServerId = sc.me
	case "Leave":
		reply.(*LeaveReply).Err = opReply.err
		reply.(*LeaveReply).ServerId = sc.me
	case "Move":
		reply.(*MoveReply).Err = opReply.err
		reply.(*MoveReply).ServerId = sc.me
	}
	DPrintf("[ServerJoinLeaveMoveDone]\t%d received %v, id=%d, term=%d",
		sc.me, op, index, term)
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.JoinLeaveMove("Join", args, reply)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.JoinLeaveMove("Leave", args, reply)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.JoinLeaveMove("Move", args, reply)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{}
	op.Op = "Query"
	op.Num = args.Num
	sc.mu.Lock()
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	opReply := OpRes{}
	opReply.index = index
	opReply.term = term
	opReply.done = make(chan bool)
	sc.resQueue = append(sc.resQueue, &opReply)
	sc.mu.Unlock()

	DPrintf("[ServerQuery]\t%d received Query id=%d, term=%d; for num=%v", sc.me, index, term, args.Num)
	<-opReply.done
	reply.Config = opReply.config
	reply.Err = opReply.err
	reply.ServerId = sc.me
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// on leaving, gid of shards belong to left gids should be set to zero
func balanceLoad(shards [NShards]int, gids []int) [NShards]int {
	// all in group zero
	if len(gids) == 0 {
		return [NShards]int{}
	}
	// move one shard from the most busy group to least busy group every term
	groupLoad := make(map[int]int)
	// init groupLoad
	groupLoad[0] = 0
	for _, gid := range gids {
		if gid == 0 {
			log.Fatalf("gid zero shouldn't appear in new groups")
		}
		groupLoad[gid] = 0
	}
	sortedGids := []int{} // sort gid by load, if load even then sort by gid
	for _, gid := range shards {
		if _, ok := groupLoad[gid]; !ok {
			log.Fatalf("Uninitialized gid in groupLoad! Shards in leaving groups should be put to group zero.")
		}
		groupLoad[gid]++
	}
	for gid, load := range groupLoad {
		if gid != 0 {
			sortedGids = append(sortedGids, gid)
			for i := len(sortedGids) - 1; i > 0 && (load > groupLoad[sortedGids[i-1]] || (load == groupLoad[sortedGids[i-1]] && gid < sortedGids[i-1])); i-- {
				sortedGids[i], sortedGids[i-1] = sortedGids[i-1], sortedGids[i]
			}
		}
	}
	// move one shard from the group with maxload to group with minload
	// if the group 0 exists, move shard from group 0 to minload group first
	// loadDelta: delta of maxload - minload. len(sortedGid) > 0
	for loadDelta := groupLoad[sortedGids[0]] - groupLoad[sortedGids[len(sortedGids)-1]]; loadDelta > 1 || groupLoad[0] > 0; loadDelta = groupLoad[sortedGids[0]] - groupLoad[sortedGids[len(sortedGids)-1]] {
		maxP, minP := 0, len(sortedGids)-1
		for maxLoad := groupLoad[sortedGids[0]]; maxP < len(sortedGids) && groupLoad[sortedGids[maxP]] == maxLoad; maxP++ {
		}
		for minLoad := groupLoad[sortedGids[len(sortedGids)-1]]; minP >= 0 && groupLoad[sortedGids[minP]] == minLoad; minP-- {
		}
		maxP, minP = maxP-1, minP+1
		maxGid, minGid := sortedGids[maxP], sortedGids[minP]
		// find the first shard in shards that's in gid 0 or maxGid, put it into minGid
		if groupLoad[0] > 0 {
			for i, gid := range shards {
				if gid == 0 {
					shards[i] = minGid
					break
				}
			}
			groupLoad[0]--
			groupLoad[minGid]++
		} else {
			if maxP >= minP {
				log.Fatalf("maxP should be smaller than minP")
			}
			for i, gid := range shards {
				if gid == maxGid {
					shards[i] = minGid
					break
				}
			}
			groupLoad[maxGid]--
			groupLoad[minGid]++
			// deterministicly adjust the position of maxGid in sortedGids
			// move forward till the right position, or meet minGid
			// this may break the sort order of gids with the same load, but won't break the order of gids with different load.
			// because even after the adjustment, load of maxGid is still greater or equal to load of minGid
			for load := groupLoad[maxGid]; maxP < minP-1 && (load < groupLoad[sortedGids[maxP+1]] || (load == groupLoad[sortedGids[maxP+1]] && maxGid > sortedGids[maxP+1])); maxP++ {
				sortedGids[maxP], sortedGids[maxP+1] = sortedGids[maxP+1], sortedGids[maxP]
			}
		}
		// adjust position of minGid
		for load := groupLoad[minGid]; minP > 0 && (load > groupLoad[sortedGids[minP-1]] || (load == groupLoad[sortedGids[minP-1]] && minGid < sortedGids[minP-1])); minP-- {
			sortedGids[minP], sortedGids[minP-1] = sortedGids[minP-1], sortedGids[minP]
		}
	}
	return shards
}

func (sc *ShardCtrler) coordinator() {
	for msg := range sc.applyCh {
		sc.mu.Lock()
		if msg.SnapshotValid && msg.Snapshot != nil && len(msg.Snapshot) > 0 {
			snapshot := msg.Snapshot
			r := bytes.NewBuffer(snapshot)
			d := labgob.NewDecoder(r)
			var ckLastId map[int64]int
			var ckIdStat map[int64]map[int]bool
			var configs []Config
			if d.Decode(&ckLastId) != nil ||
				d.Decode(&ckIdStat) != nil ||
				d.Decode(&configs) != nil {
				//   error...
				log.Fatalf("shardctrler missing snapshot")
			} else {
				sc.ckLastId = ckLastId
				sc.ckIdStat = ckIdStat
				sc.configs = configs
			}
			DPrintf("[ApplySnapshot]\t%d applying snapshot id=%d, term=%d", sc.me, msg.SnapshotIndex, msg.SnapshotTerm)
			sc.appliedId = msg.SnapshotIndex
			for len(sc.resQueue) > 0 {
				sc.resQueue[0].err = ErrTryAgain
				if msg.SnapshotIndex < sc.resQueue[0].index {
					break
				}
				go func(reply *OpRes) {
					// DPrintf("[ServerReply]\t%d replying index=%d, term=%d", sc.me, reply.index, reply.term)
					reply.done <- true
					DPrintf("[ServerDropReply]\t%d dropped reply index=%d, term=%d", sc.me, reply.index, reply.term)
				}(sc.resQueue[0])
				sc.resQueue = sc.resQueue[1:]
			}
		}
		if msg.CommandValid {
			DPrintf("[MsgApply]\t%d applying id=%d, term=%d", sc.me, msg.CommandIndex, msg.CommandTerm)
			sc.appliedId++
			if msg.CommandIndex != sc.appliedId {
				log.Fatalf("server %d should apply %d, but it was applying %d", sc.me, sc.appliedId, msg.CommandIndex)
			}
			cmd := msg.Command.(Op)
			op := cmd.Op

			if op != "Query" {
				// duplicate detect
				repeated := false
				ck, ckIndex := cmd.Ck, cmd.CkIndex
				if ckIndex <= sc.ckLastId[ck] {
					if !sc.ckIdStat[ck][ckIndex] {
						repeated = true
					} else {
						delete(sc.ckIdStat[ck], ckIndex)
					}
				} else {
					if _, ok := sc.ckIdStat[ck]; !ok {
						sc.ckIdStat[ck] = make(map[int]bool)
					}
					for i := sc.ckLastId[ck] + 1; i < ckIndex; i++ {
						sc.ckIdStat[ck][i] = true
					}
					sc.ckLastId[ck] = ckIndex
				}
				if !repeated {
					oldConfig := sc.configs[len(sc.configs)-1]
					newConfig := Config{}
					newConfig.Num = len(sc.configs)
					newConfig.Groups = make(map[int][]string)
					switch op {
					case "Join":
						newConfig.Shards = oldConfig.Shards
						// gids
						gids := []int{}
						for gid, servers := range oldConfig.Groups {
							newConfig.Groups[gid] = servers
							if gid != 0 {
								gids = append(gids, gid)
							}
						}
						for gid, servers := range cmd.Servers {
							if gid == 0 {
								log.Fatalf("group zero joining")
							}
							if _, ok := oldConfig.Groups[gid]; ok {
								log.Fatalf("duplicated join")
							}
							newConfig.Groups[gid] = servers
							gids = append(gids, gid)
						}
						DPrintf("[JoinBalance] server %d, gids: %v, shards %v, groups: %v", sc.me, gids, newConfig.Shards, newConfig.Groups)
						newConfig.Shards = balanceLoad(newConfig.Shards, gids)
						DPrintf("[JoinBalanceDone] server %d, gids: %v, shards %v, groups: %v", sc.me, gids, newConfig.Shards, newConfig.Groups)
					case "Leave":
						newConfig.Shards = oldConfig.Shards
						gids := []int{}
						for gid, servers := range oldConfig.Groups {
							newConfig.Groups[gid] = servers
						}
						for _, gid := range cmd.GIDs {
							if _, ok := newConfig.Groups[gid]; !ok {
								log.Fatalf("non-exist leave")
							}
							// delete gid and move shards belong to this group to group zero
							delete(newConfig.Groups, gid)
							for shard, group := range newConfig.Shards {
								if group == gid {
									newConfig.Shards[shard] = 0
								}
							}
						}
						for gid := range newConfig.Groups {
							if gid != 0 {
								gids = append(gids, gid)
							}
						}
						DPrintf("[LeaveBalance] server %d, gids: %v, shards: %v, groups: %v", sc.me, gids, newConfig.Shards, newConfig.Groups)
						newConfig.Shards = balanceLoad(newConfig.Shards, gids)
						DPrintf("[LeaveBalanceDone] server %d, gids: %v, shards: %v, groups: %v", sc.me, gids, newConfig.Shards, newConfig.Groups)
					case "Move":
						newConfig.Shards = oldConfig.Shards
						for gid, servers := range oldConfig.Groups {
							newConfig.Groups[gid] = servers
						}
						if _, ok := newConfig.Groups[cmd.GID]; !ok {
							log.Fatalf("move to non-exist group")
						}
						newConfig.Shards[cmd.Shard] = cmd.GID
					}
					sc.configs = append(sc.configs, newConfig)
				}
			}
			for len(sc.resQueue) > 0 {
				sc.resQueue[0].err = ErrTryAgain
				if msg.CommandIndex < sc.resQueue[0].index {
					break
				} else if msg.CommandIndex == sc.resQueue[0].index && msg.CommandTerm == sc.resQueue[0].term {
					sc.resQueue[0].err = OK
					if op == "Query" {
						num := cmd.Num
						if num == -1 || num >= len(sc.configs) {
							num = len(sc.configs) - 1
						}
						sc.resQueue[0].config = sc.configs[num]
					}
				}
				go func(reply *OpRes) {
					// DPrintf("[ServerReply]\t%d replying index=%d, term=%d", sc.me, reply.index, reply.term)
					reply.done <- true
					DPrintf("[ServerReplyDone]\t%d replyed index=%d, term=%d", sc.me, reply.index, reply.term)
				}(sc.resQueue[0])
				sc.resQueue = sc.resQueue[1:]
			}
			if sc.maxraftstate > 0 && sc.persister.RaftStateSize() > sc.maxraftstate {
				// take snapshot
				index := msg.CommandIndex
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(sc.ckLastId)
				e.Encode(sc.ckIdStat)
				e.Encode(sc.configs)
				snapshot := w.Bytes()
				sc.rf.Snapshot(index, snapshot)
			}
		}
		sc.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.ckLastId = make(map[int64]int)
	sc.ckIdStat = make(map[int64]map[int]bool)
	sc.persister = persister

	// Your code here.
	go sc.coordinator()

	return sc
}
