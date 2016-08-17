package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

// my imports
import "time"
import crand "crypto/rand"
import "bytes"
import "encoding/binary"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 1 {
		fmt.Printf(format, a...)
	} else if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs  []Config // indexed by config num
	seqTried int
	seqDone  int
}

const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

type Op struct {
	// Your data here.
	Name    string // pick one of the 4 constants as declared
	OpId    uint64 // random generated ID
	GID     int64
	Shard   int
	Num     int
	Servers []string
}

//
// Generates a 64-bit UUID
//
func (sm *ShardMaster) uuid() uint64 {
	// get 8 random bytes
	rbytes := make([]byte, 8)
	n, err := crand.Read(rbytes)
	for n < 8 || err != nil {
		n, err = crand.Read(rbytes)
	}

	// read those bytes into a variable
	var randid uint64
	binary.Read(bytes.NewReader(rbytes), binary.LittleEndian, &randid)

	return randid
}

func (sm *ShardMaster) waitForPaxos(seq int) bool {
	for to := 10 * time.Millisecond;; {
		if decided, _ := sm.px.Status(seq); decided {
			return true
		} else if time.Sleep(to); to < 10*time.Second {
			to *= 2
		}

		DPrintf("Waiting for paxos (seq %d) on server %d.", seq, sm.me)
	}
	return false
}

// assigns the given op to a Paxos sequence
func (sm *ShardMaster) decide(op Op) int {
	for {
		DPrintf("Waiting for lock (to decide) on server %d.", sm.me)

		sm.mu.Lock()
		seq := sm.seqTried
		sm.seqTried++
		sm.mu.Unlock()

		sm.px.Start(seq, op)
		sm.waitForPaxos(seq)

		if decided, val := sm.px.Status(seq); decided {
			if op.OpId == val.(Op).OpId {
				return seq
			}
		}
	}
}

// implement load-balancing for join/leave
func (sm *ShardMaster) loadBalance(config Config) Config {
	// the number of shards per server; since shards might not distribute exactly evenly, we need two numbers
	q1 := NShards / len(config.Groups)
	q2 := q1 + 1

	// the number of groups with q2 or q1 shards, respectively
	// mathematically, n1 * q1 + n2 * q2 = NShards; n1 + n2 = len(Groups)
	n2 := NShards % len(config.Groups)
	n1 := len(config.Groups) - n2

	DPrintf("Load-balancer assertion: %d * %d + %d * %d = %d", n1, q1, n2, q2, NShards)

	// first pass: we count shards per group;
	// and make a list of un-assigned shards or shards assigned to too-heavy blocks
	assign := make(map[int64]int)
	unassign := []int{}

	for gid, _ := range config.Groups {
		assign[gid] = 0
	}

	for shard, gid := range config.Shards {
		if count, ok := assign[gid]; !ok {
			unassign = append(unassign, shard)
		} else if count >= q2 {
			unassign = append(unassign, shard)
		} else if n2 == 0 && count >= q1 {
			unassign = append(unassign, shard)
		} else if assign[gid]++; count+1 >= q2 {
			// if this caused the group to fill; mark off the count of numbers of groups
			n2 -= 1
		}
	}

	DPrintf("Unassigned: %d", unassign)

	// now we have a list of shards to re-assign

	// second pass: add the unassigned shards to groups without enough shards
	// we had better reach the end of our unassigned list and the end of our groups list at the same time
	shard_idx := 0
	for gid, count := range assign {
		// check if group is full
		if count >= q2 || (count >= q1 && n2 == 0) {
			continue
		}

		var q int
		if n2 > 0 {
			q = q2
			n2--
		} else {
			q = q1
		}


		// reassign from the list until the group is full
		for i := 0; i < q - count; i++ {
			if shard_idx >= len(unassign) {
				DPrintf("Error: Load-balancer had too few unassigned shards.")
				return config
			}
			config.Shards[unassign[shard_idx]] = gid
			shard_idx += 1
		}
	}

	if shard_idx < len(unassign) {
		DPrintf("Error: Load-balancer did not reassign all shards.")
		// the remaining shards get distributed over whichever gid comes up
		// (iterating over a map is not guaranteed to be consistent for the same
		// set of <key/value>s so this may distribute to more than one group)
		for ; shard_idx < len(unassign); shard_idx++ {
			for gid, _ := range config.Groups {
				config.Shards[unassign[shard_idx]] = gid
				break;
			}
		}
	}

	return config
}

func (sm *ShardMaster) duplicateLast() Config {
	prev := sm.configs[len(sm.configs)-1]

	// copy over the previous view's shards
	var shards [NShards]int64
	for idx, elem := range prev.Shards {
		shards[idx] = elem
	}

	// copy over the previous view's groups
	groups := make(map[int64][]string)
	for idx, elem := range prev.Groups {
		groups[idx] = elem
	}

	// return the config with an incremented view number
	return Config{prev.Num + 1, shards, groups}
}

// because Configs are immutable, we don't need to cache versions for Query requests
// but we do return the number of the last config
func (sm *ShardMaster) doOps(seqEnd int) int {
	DPrintf("Waiting for lock (to do) on server %d.", sm.me)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	DPrintf("Doing ops from %d to %d on server %d.\n", sm.seqDone, seqEnd, sm.me)

	// if we've already done this op, just return
	if seqEnd <= sm.seqDone {
		return len(sm.configs) - 1
	}

	// jump-start all Paxos sequences between our Done() one and this one
	for seq := sm.seqDone + 1; seq <= seqEnd; seq++ {
		if decided, _ := sm.px.Status(seq); !decided {
			sm.px.Start(seq, Op{Query, sm.uuid(), 0, 0, 0, nil})
		}
	}

	// now do all the ops in order; this loop is blocking on Paxos decision
	for seq := sm.seqDone + 1; seq <= seqEnd; seq++ {
		decided, val := sm.px.Status(seq)
		if !decided {
			sm.waitForPaxos(seq)
			decided, val = sm.px.Status(seq)
		}

		// for Join, Leave, and Move operations only:
		switch op := val.(Op); op.Name {
			case Join:
				config := sm.duplicateLast()
				config.Groups[op.GID] = op.Servers
				config = sm.loadBalance(config)
				sm.configs = append(sm.configs, config)
			case Leave:
				config := sm.duplicateLast()
				delete(config.Groups, op.GID)
				config = sm.loadBalance(config)
				sm.configs = append(sm.configs, config)
			case Move:
				config := sm.duplicateLast()
				config.Shards[op.Shard] = op.GID
				sm.configs = append(sm.configs, config)
		}
	}

	sm.seqDone = seqEnd
	sm.px.Done(sm.seqDone)

	return len(sm.configs) - 1
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	DPrintf("Join %d on server %d.\n", args.GID, sm.me)

	op := Op{Join, sm.uuid(), args.GID, 0, 0, args.Servers}
	seq := sm.decide(op)
	sm.doOps(seq)

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	DPrintf("Leave %d on server %d.\n", args.GID, sm.me)

	op := Op{Leave, sm.uuid(), args.GID, 0, 0, nil}
	seq := sm.decide(op)
	sm.doOps(seq)

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	DPrintf("Move %d on server %d.\n", args.GID, sm.me)

	op := Op{Move, sm.uuid(), args.GID, args.Shard, 0, nil}
	seq := sm.decide(op)
	sm.doOps(seq)

	return nil
}

// Query(-1) may return fresher data than the Paxos log would suggest, but it won't return stale data
// to return exact values, we would need a per-op cache, and we don't have a good way to garbage-collect that cache
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	DPrintf("Query %d on server %d.\n", args.Num, sm.me)

	op := Op{Query, sm.uuid(), 0, 0, args.Num, nil}
	seq := sm.decide(op)
	con := sm.doOps(seq)

	DPrintf("Waiting for lock (to query) on server %d.", sm.me)

	sm.mu.Lock()
	if 0 <= args.Num && args.Num < len(sm.configs) {
		reply.Config = sm.configs[args.Num]
	} else {
		reply.Config = sm.configs[con]
	}
	sm.mu.Unlock()

	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.seqTried = 0
	sm.seqDone = -1

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs, false, "", false)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
