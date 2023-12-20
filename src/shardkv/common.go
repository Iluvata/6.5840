package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTryAgain    = "ErrTryAgain"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Ck        int64
	Index     int
	ConfigNum int
}

type PutAppendReply struct {
	Err      Err
	ServerId int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ConfigNum int
}

type GetReply struct {
	Err      Err
	Value    string
	ServerId int
}

type TransferShardArgs struct {
	ShardNum  int
	ConfigNum int
}

type TransferShardReply struct {
	Err   Err
	Shard ShardData
}

type DeleteLegacyShardArgs struct {
	ShardNum  int
	ConfigNum int
}

type DeleteLegacyShardReply struct {
}
