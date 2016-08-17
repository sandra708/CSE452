package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug {
        fmt.Printf(format, a...)
    }
    return
}

const (
    Get         = "Get"
    Put         = "Put"
    Inquire     = "Inquire"
    Reconfigure = "Reconfigure"
)

type Op struct {
	// Your definitions here.
    Name string
    Id   int64
    Args interface{}
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid        int64 // my replica group ID

	// Your definitions here.
    store      map[string] string   // key-value store
    ret        map[int64] string    // client => lastest_reply in case of crash
    ids        map[int64] int64     // client => highest_seq for at-most-once semantics
    seq        int                  // highest operation made on the store
    config     shardmaster.Config   // our current configuration
}

type InquireArgs struct {
  Shard  int
  Config shardmaster.Config
}

type InquireReply struct {
  Err     Err
  Store   map[string]string
  Seen    map[int64]int64
  Replies map[int64]string
}

type ReconfigArgs struct {
    Config  shardmaster.Config
    Inquiry InquireReply
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    // execute and reply
    op := Op{Get, uuid(), *args}
    reply.Err, reply.Value = kv.execute(op)

    return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    // execute and reply
    op := Op{Put, uuid(), *args}
    reply.Err, reply.PreviousValue = kv.execute(op)

    return nil
}

// Get shard configuration information from another group
func (kv *ShardKV) Inquire(args *InquireArgs, reply *InquireReply) error {
    // Do not lock this check or deadlock will occur in Concurrent Put/Get/Move
    if kv.config.Num < args.Config.Num {
        reply.Err = ErrNoConfig
        return nil
    }

    kv.mu.Lock()
    defer kv.mu.Unlock()

    // mark this op in the log
    op := Op{Inquire, uuid(), nil}
    kv.execute(op)

    reply.Store = map[string]string{}
    reply.Seen = map[int64]int64{}
    reply.Replies = map[int64]string{}

    // return for the specified shard
    for key := range kv.store {
        if key2shard(key) == args.Shard {
            reply.Store[key] = kv.store[key]
        }
    }
    for client := range kv.ids {
        reply.Seen[client] = kv.ids[client]
        reply.Replies[client] = kv.ret[client]
    }

    reply.Err = OK

    return nil
}

func (kv *ShardKV) Reconfigure(config shardmaster.Config) bool {
    state := InquireReply{OK, map[string]string{}, map[int64]int64{}, map[int64]string{}}

    // get latest info from each group for each shard
    for i, gid := range kv.config.Shards {
        // only if the shard should be in our group but currently is not
        if config.Shards[i] == kv.gid && gid != kv.gid {
            args := InquireArgs{i, kv.config}
            var reply InquireReply
            for _, srv := range kv.config.Groups[gid] {
                if ok := call(srv, "ShardKV.Inquire", &args, &reply); ok {
                    if reply.Err == OK {
                        // copy reply information into our field
                        for key := range reply.Store {
                            state.Store[key] = reply.Store[key]
                        }
                        for client := range reply.Seen {
                            if id, exists := state.Seen[client]; !exists || id < reply.Seen[client] {
                                state.Seen[client] = reply.Seen[client]
                                state.Replies[client] = reply.Replies[client]
                            }
                        }
                        break
                    } else if reply.Err == ErrNoConfig {
                        // that server isn't at the necessary configuration yet
                        // so wait for the next tick
                        return false
                    }
                }
            }
        }
    }

    args := ReconfigArgs{config, state}
    op := Op{Reconfigure, uuid(), args}
    kv.execute(op)

    return true
}

// wait for Paxos to decide on a sequence number for this op
func (kv *ShardKV) waitForPaxos(seq int) Op {
    for to := 10 * time.Millisecond;; {
        if decided, val := kv.px.Status(seq); decided {
            return val.(Op)
        }
        if time.Sleep(to); to < 10 * time.Second {
            to *= 2
        }
    }
}

// check if we have done this op already but then crashed
func (kv *ShardKV) status(op Op) (Err, string) {
    switch op.Name {
    case Get, Put:
        var key string
        var seq int64
        var client int64

        switch (op.Name) {
        case Get:
            key = op.Args.(GetArgs).Key
            seq = op.Args.(GetArgs).Seq
            client = op.Args.(GetArgs).Client
        case Put:
            key = op.Args.(PutArgs).Key
            seq = op.Args.(PutArgs).Seq
            client = op.Args.(PutArgs).Client
        }
        if shard := key2shard(key); kv.gid != kv.config.Shards[shard] {
            // this shard isn't in our group
            return ErrWrongGroup, Nil
        } else if seqq, exists := kv.ids[client]; exists && seq <= seqq {
            // we've already have a response for this client
            return OK, kv.ret[client]
        }
    case Reconfigure:
        if kv.config.Num >= op.Args.(ReconfigArgs).Config.Num {
            return OK, Nil
        }
    }

    return Nil, Nil
}

// carry out this op and apply the effects to the store
func (kv *ShardKV) execute(op Op) (Err,string) {
    res := Op{Nil, -1, nil}
    for res.Id != op.Id {
        if result, ret := kv.status(op); result != Nil {
            return result, ret
        }

        kv.seq++
        if decided, t := kv.px.Status(kv.seq); decided {
            res = t.(Op)
        } else {
            kv.px.Start(kv.seq, op)
            res = kv.waitForPaxos(kv.seq)
        }

        switch res.Name {
        case Get:
            previous, _ := kv.store[res.Args.(GetArgs).Key]
            kv.ret[res.Args.(GetArgs).Client] = previous
            kv.ids[res.Args.(GetArgs).Client] = res.Args.(GetArgs).Seq
        case Put:
            previous, _ := kv.store[res.Args.(PutArgs).Key]
            kv.ret[res.Args.(PutArgs).Client] = previous
            kv.ids[res.Args.(PutArgs).Client] = res.Args.(PutArgs).Seq

            if res.Args.(PutArgs).DoHash {
                kv.store[res.Args.(PutArgs).Key] = strconv.Itoa(int(hash(previous + res.Args.(PutArgs).Value)))
            } else {
                kv.store[res.Args.(PutArgs).Key] = res.Args.(PutArgs).Value
            }
        case Reconfigure:
            reconfig := res.Args.(ReconfigArgs).Inquiry
            for key := range reconfig.Store {
                kv.store[key] = reconfig.Store[key]
            }
            for client := range reconfig.Seen {
                if seqq, exists := kv.ids[client]; !exists || seqq < reconfig.Seen[client] {
                    kv.ids[client] = reconfig.Seen[client]
                    kv.ret[client] = reconfig.Replies[client]
                }
            }
            kv.config = res.Args.(ReconfigArgs).Config
        }

        kv.px.Done(kv.seq)
    }

    var client int64
    switch (op.Name) {
        case Get:
            client = op.Args.(GetArgs).Client
        case Put:
            client = op.Args.(PutArgs).Client
    }
    return OK, kv.ret[client]
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    // check for new configurations since our last tick and apply any changes
    for i := kv.config.Num + 1; i <= kv.sm.Query(-1).Num; i++ {
        if !kv.Reconfigure(kv.sm.Query(i)) {
            break
        }
    }
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

    // needed to marshal these structs into RPCs
	gob.Register(GetArgs{})
    gob.Register(PutArgs{})
    gob.Register(ReconfigArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.store = map[string]string{}
    kv.ids = map[int64]int64{}
    kv.ret = map[int64]string{}
    kv.seq = 0
    kv.config = shardmaster.Config{Num:-1}

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
