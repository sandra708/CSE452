package kvpaxos

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
import "strconv"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// used as const - a no-op to fill holes in Paxos
var (
	Nop = Op{0, 0, 0, "", ""}
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client uint64
	OpId   uint64
	Put    int // 0 = Get; 1 = Put; 2 = PutHash
	Key    string
	Value  string //"" for Gets
}

type OpResult struct {
	Op     Op
	Result string //Value for Get or PreviousValue for PutHash
	Seq    int    //Paxos sequence #
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	store    map[string]string           //keeps the state-replicated kv store
	opLog    map[uint64]map[int]OpResult //keeps a list of ops per client per seqence #; clears previous's when THIS SERVER receives a request from that client
	idLog	map[uint64]bool			// keeps a list of all the op-ids done; in order to discard old/outdated requests
	seqTried int                         // a hint to what sequence to try first; can be lower than the first un-proposed sequence but not higher
	seqDone  int                         // the last sequence for which we've called Done
}

func (kv *KVPaxos) waitForPaxos(seq int) bool {
	to := 10 * time.Millisecond
	for {
		decided, _ := kv.px.Status(seq)
		if decided {
			return true
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		} else {
			DPrintf("Waiting for Paxos seq(%d) on server %d", seq, kv.me)
		}
		if seq < kv.seqDone {
			DPrintf("Seq(%d) is already done, through %d. Stop waiting on server %d.", seq, kv.seqDone, kv.me)
			return false
		}
	}
	return false
}

// NON-LOCKING! internal helper only
// does a put, computing a hash if indicated, and stores in the client-indexed cache (if indicated)
func (kv *KVPaxos) doPut(op Op, seq int, forceLog bool) string {
	if op.Put > 1 { // hash
		prevValue, ok := kv.store[op.Key]
		if !ok {
			//DPrintf("Put(%s) on server %d at Seq(%d), no prior instances of key found.", op.Key, kv.me, seq)
			prevValue = ""
		} // line added
		value := strconv.Itoa(int(hash(prevValue + op.Value)))
		kv.store[op.Key] = value
		kv.logResult(OpResult{op, prevValue, seq})
		return prevValue
	} else {
		kv.store[op.Key] = op.Value
		kv.logResult(OpResult{op, "", seq})
		return ""
	}
}

// NON-LOCKING! internal function. Callers responsible for locking all data.
func (kv *KVPaxos) doGet(req Op, seqEnd int) string {
	// we do all the puts up to and including the seqence number given
	// and return the value if that sequence was a Get or the previous value if it was a Put
	// and it logs everything

	if req != Nop {
		DPrintf("DoGet(%d) with Done = %d on server %d", seqEnd, kv.seqDone, kv.me)
	}

	// if we've already done this sequence - we need to find the log-id
	if seqEnd <= kv.seqDone {
		//DPrintf("We've done this Op, looking in log! Server %d, Seq(%d)", kv.me, seqEnd)
		record, ok := kv.checkLog(req)
		if ok {
			return record.Result
		} else {
			return ""
		}
	}

	//jump-start the decision if the server needs to catch up - essential to avoid deadlock
	for seq := kv.seqDone + 1; seq < seqEnd; seq++ {
		decided, _ := kv.px.Status(seq)
		if !decided {
			// submit a No-op (Get on the empty string)
			kv.px.Start(seq, Nop)
		}
	}

	for seq := kv.seqDone + 1; seq < seqEnd; seq++ {
		decided, val := kv.px.Status(seq)
		if !decided {
			kv.waitForPaxos(seq)
			decided, val = kv.px.Status(seq)
		}
		op := val.(Op)
		res := "!!!!!!!!"
		if op.Put > 0 {
			res = kv.doPut(op, seq, false)
		} else {
			res = kv.store[op.Key]
			kv.logResult(OpResult{op, res, seq})
		}
		DPrintf("Server %d has caught up with Seq(%d): Get/Put(%s) ID = %d: %s", kv.me, seq, op.Key, op.OpId, res)
	}

	// now do the particular op which was requested - make sure to log and return
	decided, val := kv.px.Status(seqEnd)
	for !decided {
		kv.px.Start(seqEnd, Nop)
		kv.waitForPaxos(seqEnd)
		decided, val = kv.px.Status(seqEnd)
	}

	value := ""

	op := val.(Op)
	if op.Put > 0 {
		value = kv.doPut(op, seqEnd, true)
	} else {
		v, ok := kv.store[op.Key]
		if ok {
			value = v
		} else {
			value = ""
		}
		kv.logResult(OpResult{op, value, seqEnd})
	}

	// tell Paxos we are finished with this op and all previous
	kv.px.Done(seqEnd)
	kv.seqDone = seqEnd
	return value
}

func (kv *KVPaxos) logResult(op OpResult) {
	if op.Op.Put == 0 {
		return // don't need to log Gets!
	}
	_, ok := kv.opLog[op.Op.Client]
	if !ok {
		kv.opLog[op.Op.Client] = make(map[int]OpResult)
	}
	kv.opLog[op.Op.Client][op.Seq] = op
}

func (kv *KVPaxos) checkLog(op Op) (OpResult, bool) {
	// get log up to date
	records, ok := kv.opLog[op.Client]
	if !ok {
		return OpResult{Nop, "", -1}, false
	}
	for _, record := range records {
		if record.Op.OpId == op.OpId {
			return record, true
		}
	}
	return OpResult{Nop, "", -1}, false
}

// clears the log
// we keep every op at and ahead of the given seq
// and 8 ops behind it
func (kv *KVPaxos) clearLog(client uint64, seq int) {
	records, ok := kv.opLog[client]
	if !ok {
		return
	}
	prev := map[int]bool{}
	prevMin := -1
	for _, record := range records {
		if record.Seq < prevMin {
			delete(records, record.Seq)
		} else if record.Seq < seq {
			prev[record.Seq] = true
			if len(prev) > 6 {
				delete(records, prevMin)
				prevMin = seq
				for key, _ := range prev {
					if prevMin < key {
						prevMin = key
					}
				}
				delete(prev, prevMin)
			}
		}
	}
	kv.opLog[client] = records
}

func (kv *KVPaxos) decideSeq(op Op) int {
	for {
		seq := kv.seqTried

		// check to make sure this sequence isn't still undecided
		if decided, _ := kv.px.Status(seq); decided {
			kv.seqTried += 1
			continue
		}

		// check to make sure the op hasn't turned up in the log more recently
		kv.doGet(Nop, seq-1)
		record, ok := kv.checkLog(op)
		if ok {
			return record.Seq
		}

		DPrintf("OpID=%d not found in log on server %d. Attepmting Paxos seq=%d", op.OpId, kv.me, seq)

		kv.seqTried += 1

		kv.px.Start(seq, op)
		kv.waitForPaxos(seq)
		decided, val := kv.px.Status(seq)
		if decided && val != nil && (op.OpId == val.(Op).OpId) {
			return seq
		}
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	DPrintf("Server Get(%s), ID=%d, to server %d\n", args.Key, args.OpId, kv.me)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Your code here.

	op := Op{args.Client, args.OpId, 0, args.Key, ""}

	// first, check log
	kv.doGet(Nop, kv.seqTried-1)

	record, ok := kv.checkLog(op)
	if ok && record.Op.OpId == args.OpId {
		DPrintf("Server Get(%s) to server %d found in log\n", args.Key, kv.me)
		reply.Value = record.Result
		reply.Err = ""
		//DPrintf("Server Get(%s) to server %d found answer in log, resolved to %s\n", args.Key, kv.me, reply.Value)
		return nil
	}

	// try to claim a Paxos sequence: try each iteratively
	seq := kv.decideSeq(op)

	DPrintf("Server Get(%s) on server %d decided for Seq(%d)", args.Key, kv.me, seq)
	// start a log entry

	reply.Value = kv.doGet(op, seq)

	reply.Err = ""

	kv.clearLog(args.Client, seq)

	DPrintf("Server Get(%s), ID = %d, on server %d, Seq(%d) returns value %s", args.Key, args.OpId, kv.me, seq, reply.Value)

	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	DPrintf("Server Put(%s), ID=%d, to server %d\n", args.Key, args.OpId, kv.me)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// first, check logs
	put := 1
	if args.DoHash {
		put += 1
	}
	op := Op{args.Client, args.OpId, put, args.Key, args.Value}

	kv.doGet(Nop, kv.seqTried-1)
	record, ok := kv.checkLog(op)
	if ok && record.Op.OpId == args.OpId {
		DPrintf("Server Put(%s) to server %d found in log.\n", args.Key, kv.me)
		reply.PreviousValue = record.Result
		reply.Err = ""
		//DPrintf("Server Put(%s, %s) to server %d found answer in log, resolved to %s\n", args.Key, args.Value, kv.me, reply.PreviousValue)
		return nil
	}

// then check to see if we've seen the op id at all - if so, it is an old request and can be ignored
	ok = kv.idLog[args.OpId]
	if ok {
		reply.PreviousValue = ""
		return nil
}

	seq := kv.decideSeq(op)

	DPrintf("Server Put(%s) on server %d decided on Seq(%d)", args.Key, kv.me, seq)
	// start a log entry

	// we only need to actually perform ops if it's a hash (because that has a Get() wrapped in)
	if args.DoHash {
		reply.PreviousValue = kv.doGet(op, seq)
	} else {
		reply.PreviousValue = ""
	}

	reply.Err = ""
	kv.clearLog(args.Client, seq)
	kv.idLog[args.OpId] = true

	DPrintf("Server Put(%s, %s) ID=%d on server %d, Seq(%d) returns value %s", args.Key, args.Value, args.OpId, kv.me, seq, reply.PreviousValue)
	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.seqTried = 0
	kv.seqDone = -1
	kv.store = make(map[string]string)
	kv.opLog = make(map[uint64]map[int]OpResult)
	kv.idLog = make(map[uint64]bool)
	// Your initialization code here.

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				DPrintf("RPC handler error: Killing server %d.\n", me)
				kv.kill()
			}
		}
	}()

	return kv
}
