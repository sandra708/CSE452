package pbservice

import "hash/fnv"

const (
    OK             = "OK"
    ErrNoKey       = "ErrNoKey"
    ErrWrongServer = "ErrWrongServer"
    ErrBackup      = "ErrBackup"
)

type Err string

type PutArgs struct {
    Key    string
    Value  string
    DoHash bool // For PutHash
    // You'll have to add definitions here.
    Id     uint64
    Client string
}

type PutReply struct {
    Err           Err
    PreviousValue string // For PutHash
}

type GetArgs struct {
    Key    string
    // You'll have to add definitions here.
    Id     uint64
    Client string
}

type GetReply struct {
    Err   Err
    Value string
}

type PutEntry struct {
    Reply  PutReply
    Client string
}

type GetEntry struct {
    Reply  GetReply
    Client string
}

// Your RPC definitions here.
type ForwardPutArgs struct {
    Args  *PutArgs
    Reply *PutReply
}

type ForwardGetArgs struct {
    Args  *GetArgs
    Reply *GetReply
}

type ForwardStateArgs struct {
    Primary string
    Store   map[string]string
    Gets    map[uint64]GetEntry
    Puts    map[uint64]PutEntry
}

type ForwardReply struct {
    Err Err
}

func hash(s string) uint32 {
    h := fnv.New32a()
    h.Write([]byte(s))
    return h.Sum32()
}
