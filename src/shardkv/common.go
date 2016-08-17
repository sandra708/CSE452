package shardkv

import "hash/fnv"

import "crypto/rand"
import "bytes"
import "encoding/binary"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNoConfig   = "ErrNoConfig"
	Nil 		  = ""
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq    int64
  	Client int64

}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key    string
	// You'll have to add definitions here.
	Seq    int64
  	Client int64
}

type GetReply struct {
	Err   Err
	Value string
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

//
// Generates a 64-bit UUID
//
func uuid() int64 {
    // get 8 random bytes
    rbytes := make([]byte, 8)
    n, err := rand.Read(rbytes)
    for n < 8 || err != nil {
        n, err = rand.Read(rbytes)
    }

    // read those bytes into a variable
    var randid int64
    binary.Read(bytes.NewReader(rbytes), binary.LittleEndian, &randid)
    if randid < 0 {
      randid = -randid
    }

    return randid
}
