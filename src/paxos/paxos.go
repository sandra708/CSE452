package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string, dir string, restart bool)
// dir should be a safe directory to store paxos state
// restart should be false the first time the paxos instance is started and true when recovering from a crash
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math"
import "math/rand"
import "time"
import "strings"
import "strconv"
import "bytes"
import "encoding/gob"
import "io/ioutil"

// Interval at which we garbage-collect forgotten instances
const GCInterval = 500 * time.Millisecond

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instances map[int]PaxosInstance // map is easier than array/slice for gc
	done      map[int]int           // stores the Done(%d) ints
	nseq      int                   // the highest sequence no seen
	nmajority int                   // the number of peers required for majority

	dir        string
	saveToDisk bool
}

type PaxosInstance struct {
	N_p     int
	N_a     int
	V_a     interface{}
	Decided bool
}

type PrepareArgs struct {
	Seq int
	N   int
}

type PrepareReply struct {
	N      int
	V      interface{}
	Reject bool
	Done   int
}

type AcceptArgs struct {
	Seq int
	N   int
	V   interface{}
}

type AcceptReply struct {
	Reject bool
	Done   int
}

type DecideArgs struct {
	Seq int
	N   int
	V   interface{}
}

type DecideReply struct {
	Reject bool
	Done   int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) updatePaxos(seq int, instance PaxosInstance) {
	if px.saveToDisk {
		px.fileUpdatePaxos(seq, instance)
	}
	px.instances[seq] = instance
}

func (px *Paxos) fileUpdatePaxos(seq int, op PaxosInstance) error {
	DPrintf("Saving paxos sequence %d: %d on server %d", seq, op, px.me)
	fullname := px.dir + "/paxos-" + strconv.Itoa(seq)
	tempname := px.dir + "/temp-" + strconv.Itoa(seq)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(op)

	if err := ioutil.WriteFile(tempname, w.Bytes(), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}
	return nil
}

func (px *Paxos) fileRetrievePaxos() map[int]PaxosInstance {
	m := map[int]PaxosInstance{}
	d := px.dir
	files, err := ioutil.ReadDir(d)
	if err != nil {
		log.Fatalf("fileRetrievePaxos could not read %v: %v", d, err)
	}
	for _, fi := range files {
		n1 := fi.Name()
		if n1[0:6] == "paxos-" {
			key, err := strconv.Atoi(n1[6:])
			if err != nil {
				log.Fatalf("fileRetrievePaxos bad file name %v: %v", n1, err)
			}
			fullname := px.dir + n1
			content, err := ioutil.ReadFile(fullname)
			if err != nil {
				log.Fatalf("fileRetrievePaxos fileGet failed for %v: %v", key, err)
			}
			buf := bytes.NewBuffer(content)
			g := gob.NewDecoder(buf)
			var inst PaxosInstance
			g.Decode(&inst)
			DPrintf("Retrieved paxos sequence %d: %d on server %d", key, inst, px.me)
			m[key] = inst
		}
	}
	return m
}

// Proposes a value to all peers and works to get it accepted for this instance
func (px *Paxos) propose(seq int, value interface{}) {
	//DPrintf("propose(%d)\n", seq)

	// proposer(v):

	//initialize to unique value - seq rotates so that peers alternate going "first"
	init_psn := (px.me + seq) % len(px.peers)
	n := init_psn
	// while not decided
	for !px.dead {

		// check if it is decided
		if inst, ok := px.instances[seq]; ok && inst.Decided {
			return
		}

		nseen := -1
		nprepare_ok := 0
		serv_prepare_ok := make([]int, 0)
		// send prepare(n) to all servers including self
		for peer := 0; peer < len(px.peers); peer++ {
			args := PrepareArgs{seq, n}
			var reply PrepareReply

			if px.send(peer, "Paxos.Prepare", args, &reply) && !reply.Reject {
				nprepare_ok++
				serv_prepare_ok = append(serv_prepare_ok, peer)
				// v' = v_a with highest n_a; choose own v otherwise
				if reply.N > nseen {
					nseen = reply.N
					value = reply.V
				}
			}
		}

		// didn't get approval from the majority, so pick a higher n and retry
		if nprepare_ok < px.nmajority {
			//DPrintf("prepare_reject(%d)\n", seq)
			if n < nseen {
				n = (nseen / len(px.peers)) + init_psn
			}
			n += len(px.peers)
			continue
		}

		//DPrintf("%d: prepare_ok(%d) for (%s): %d\n", px.me, seq, value, serv_prepare_ok)

		// if prepare_ok(n_a, v_a) from majority, send accept(n, v') to all
		naccept_ok := 0
		serv_accept_ok := make([]int, 0)
		for peer := 0; peer < len(px.peers); peer++ {
			args := AcceptArgs{seq, n, value}
			var reply AcceptReply
			if px.send(peer, "Paxos.Accept", args, &reply) && !reply.Reject {
				naccept_ok++
				serv_accept_ok = append(serv_accept_ok, peer)
			}
		}

		// didn't get approval from the majority, so pick a higher n and retry
		if naccept_ok < px.nmajority {
			//DPrintf("accept_reject(%d)\n", seq)
			if n < nseen {
				n = (nseen / len(px.peers)) + init_psn
			}
			n += len(px.peers)
			continue
		}

		//DPrintf("%d: accept_ok(%d) for (%s): %d\n", px.me, seq, value, serv_accept_ok)

		// if accept_ok(n) from majority, send decided(v') to all
		for peer := 0; peer < len(px.peers); peer++ {
			args := DecideArgs{seq, n, value}
			var reply DecideReply
			if px.send(peer, "Paxos.Decided", args, &reply) && !reply.Reject {
				px.done[peer] = reply.Done
			}
		}

		//DPrintf("decided(%d)\n", seq)

		break
	}
}

// Makes an LPC to self or an RPC to other peers
func (px *Paxos) send(peer int, pc string, args interface{},
	reply interface{}) bool {
	// RPC
	if peer != px.me {
		//DPrintf("%s()\n", pc)
		return call(px.peers[peer], pc, args, reply)
	}

	pc = strings.TrimPrefix(pc, "Paxos.") // trim to print and see it's an LPC
	//DPrintf("%s()\n", pc)
	// LPC for the local acceptor
	switch pc {
	case "Prepare":
		px.Prepare(args.(PrepareArgs), reply.(*PrepareReply))
	case "Accept":
		px.Accept(args.(AcceptArgs), reply.(*AcceptReply))
	case "Decided":
		px.Decided(args.(DecideArgs), reply.(*DecideReply))
	default:
		return false
	}

	return true
}

// Asks this peer to prepare to accept this value for this instance
func (px *Paxos) Prepare(args PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	var pi PaxosInstance
	if pii, ok := px.instances[args.Seq]; ok {
		pi = pii
	} else {
		pi = PaxosInstance{-1, -1, nil, false}
	}

	// acceptor's prepare(n) handler:
	// if n > n_p
	if args.N > pi.N_p {
		// n_p = n
		instance := PaxosInstance{args.N, pi.N_a, pi.V_a, pi.Decided}
		px.updatePaxos(args.Seq, instance)

		// reply prepare_ok(n_a, v_a)
		reply.N = pi.N_a
		reply.V = pi.V_a
	} else {
		// else reply prepare_reject
		reply.Reject = true
	}

	return nil
}

// Asks this peer to accept the value for this paxos instance
func (px *Paxos) Accept(args AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	pi := px.instances[args.Seq]

	// acceptor's accept(n, v) handler:
	// if n >= n_p
	if args.N >= pi.N_p {
		// n_p = n; n_a = n; v_a = v
		instance := PaxosInstance{args.N, args.N, args.V, pi.Decided}
		px.updatePaxos(args.Seq, instance)
	} else {
		// else reply accept_reject
		reply.Reject = true
	}

	return nil
}

// Marks this seq no as having been decided
func (px *Paxos) Decided(args DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	// decided(v')
	instance := PaxosInstance{args.N, args.N, args.V, true}
	px.updatePaxos(args.Seq, instance)

	// piggybacking the Done value
	reply.Done = px.done[px.me]

	return nil
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	//DPrintf("Start(%d, %v)\n", seq, v)
	go px.propose(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	//DPrintf("Done(%d)\n", seq)
	px.done[px.me] = seq
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	//DPrintf("Max()\n")
	return px.nseq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	//DPrintf("Min()\n")

	min := math.MaxInt64
	for _, seq := range px.done {
		if seq < min {
			min = seq
		}
	}
	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	//DPrintf("Status(%d)\n", seq)

	if pi, ok := px.instances[seq]; ok {
		return pi.Decided, pi.V_a
	}

	// instace for the seq no does not exist yet
	if seq > px.nseq {
		px.nseq = seq
	}

	return false, -1
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server, saveToDisk bool, dir string, restart bool) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	gob.Register(PaxosInstance{})
	px.dir = dir

	// Your initialization code here.
	px.instances = make(map[int]PaxosInstance)
	px.done = make(map[int]int)
	for peer := 0; peer < len(px.peers); peer++ {
		// a peers z_i is -1 if it has never called Done()
		px.done[peer] = -1
	}
	px.nseq = -1
	px.nmajority = len(px.peers)/2 + 1
	px.saveToDisk = saveToDisk

	if saveToDisk && restart {
		px.instances = px.fileRetrievePaxos()
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	// garbage collector - cleans up forgotten instances
	go func() {
		for !px.dead {
			px.mu.Lock()
			min := px.Min()
			for ninst := range px.instances {
				if ninst < min {
					//DPrintf("delete(%d)\n", ninst)
					delete(px.instances, ninst)
					if px.saveToDisk {
						fullname := px.dir + "/paxos-" + strconv.Itoa(ninst)
						tempname := px.dir + "/paxos-" + strconv.Itoa(ninst)
						os.Remove(fullname)
						os.Remove(tempname)

					}
				}
			}
			px.mu.Unlock()
			time.Sleep(GCInterval)
		}
	}()

	return px
}
