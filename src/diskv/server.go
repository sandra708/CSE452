package diskv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "encoding/base32"
import "math/rand"
import "shardmaster"
import "io/ioutil"
import "strconv"
import "bytes"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

const (
	Get         = "Get"
	Put         = "Put"
	Inquire     = "Inquire"
	Reconfigure = "Reconfigure"
	Nop         = "Nop"
)

type Op struct {
	// Your definitions here.
	Name string
	Id   int64
	Args interface{}
}

type DisKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos
	dir        string // each replica has its own data directory

	gid int64 // my replica group ID

	// Your definitions here.
	store  map[string]string  // key-value store
	ret    map[int64]string   // client => lastest_reply in case of crash
	ids    map[int64]int64    // client => highest_seq for at-most-once semantics
	seq    int                // highest operation made on the store
	config shardmaster.Config // our current configuration
}

// For shard re-alignment RPC's
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

// submit collected reconfiguration to Paxos as an atomic operation
type ReconfigArgs struct {
	Config  shardmaster.Config
	Inquiry InquireReply
}

//
// these are handy functions that might be useful
// for reading and writing key/value files, and
// for reading and writing entire shards.
// puts the key files for each shard in a separate
// directory.
//

func (kv *DisKV) shardDir(shard int) string {
	d := kv.dir + "/shard-" + strconv.Itoa(shard) + "/"
	// create directory if needed.
	_, err := os.Stat(d)
	if err != nil {
		if err := os.Mkdir(d, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", d, err)
		}
	}
	return d
}

// cannot use keys in file names directly, since
// they might contain troublesome characters like /.
// base32-encode the key to get a file name.
// base32 rather than base64 b/c Mac has case-insensitive
// file names.
func (kv *DisKV) encodeKey(key string) string {
	return base32.StdEncoding.EncodeToString([]byte(key))
}

func (kv *DisKV) decodeKey(filename string) (string, error) {
	key, err := base32.StdEncoding.DecodeString(filename)
	return string(key), err
}

// read the content of a key's file.
func (kv *DisKV) fileGet(shard int, key string) (string, error) {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key)
	content, err := ioutil.ReadFile(fullname)
	return string(content), err
}

// replace the content of a key's file.
// uses rename() to make the replacement atomic with
// respect to crashes.
func (kv *DisKV) filePut(shard int, key string, content string) error {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key)
	tempname := kv.shardDir(shard) + "/temp-" + kv.encodeKey(key)
	if err := ioutil.WriteFile(tempname, []byte(content), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}
	return nil
}

// return content of every key file in a given shard.
func (kv *DisKV) fileReadShard(shard int) map[string]string {
	m := map[string]string{}
	d := kv.shardDir(shard)
	files, err := ioutil.ReadDir(d)
	if err != nil {
		log.Fatalf("fileReadShard could not read %v: %v", d, err)
	}
	for _, fi := range files {
		n1 := fi.Name()
		if n1[0:4] == "key-" {
			key, err := kv.decodeKey(n1[4:])
			if err != nil {
				log.Fatalf("fileReadShard bad file name %v: %v", n1, err)
			}
			content, err := kv.fileGet(shard, key)
			if err != nil {
				log.Fatalf("fileReadShard fileGet failed for %v: %v", key, err)
			}
			m[key] = content
		}
	}
	return m
}

// replace an entire shard directory.
func (kv *DisKV) fileReplaceShard(shard int, m map[string]string) {
	d := kv.shardDir(shard)
	os.RemoveAll(d) // remove all existing files from shard.
	for k, v := range m {
		kv.filePut(shard, k, v)
	}
}

func (kv *DisKV) fileIncrementSeq(seq int) error {
	fullname := kv.dir + "/seq"
	tempname := kv.dir + "/seq-temp"
	if err := ioutil.WriteFile(tempname, []byte(strconv.Itoa(seq)), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}

	// then garbage-collect old atomic append data
	fullname = kv.dir + "/append-" + strconv.Itoa(seq-1)
	tempname = kv.dir + "/temp-append-" + strconv.Itoa(seq-1)
	os.Remove(fullname)
	os.Remove(tempname)

	fullname = kv.dir + "/op-" + strconv.Itoa(seq-1)
	tempname = kv.dir + "/op-temp" + strconv.Itoa(seq-1)
	os.Remove(fullname)
	os.Remove(tempname)

	return nil
}

// We store the number of the current config on disk, as it is necessary to know which shards we are responsible for
func (kv *DisKV) fileIncrementConfig(config int) error {
	fullname := kv.dir + "/config"
	tempname := kv.dir + "/config-temp"
	if err := ioutil.WriteFile(tempname, []byte(strconv.Itoa(config)), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}
	return nil
}

// Logs the current op while it is being done; so that recovery doesn't have to wait on Paxos
func (kv *DisKV) fileLogOp(op Op, seq int) error {
	fullname := kv.dir + "/op-" + strconv.Itoa(seq)
	tempname := kv.dir + "/op-temp" + strconv.Itoa(seq)

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

// Retrieves the current op
func (kv *DisKV) fileRetrieveOp(seq int) (Op, error) {
	fullname := kv.dir + "/op-" + strconv.Itoa(seq)

	content, err := ioutil.ReadFile(fullname)
	buf := bytes.NewBuffer(content)
	d := gob.NewDecoder(buf)

	var op Op
	d.Decode(&op)

	return op, err
}

// Recovers the sequence and config numbers
func (kv *DisKV) fileRecoverPaxosInfo() (int, int) {
	file_seq := kv.dir + "/seq"
	s, _ := ioutil.ReadFile(file_seq)
	file_config := kv.dir + "/config"
	c, _ := ioutil.ReadFile(file_config)
	seq, _ := strconv.Atoi(string(s))
	config, _ := strconv.Atoi(string(c))
	return seq, config
}

// Stores information about an append operation in progress
func (kv *DisKV) fileTempAppend(seq int, content string) error {
	fullname := kv.dir + "/append-" + strconv.Itoa(seq)
	tempname := kv.dir + "/temp-append-" + strconv.Itoa(seq)
	if err := ioutil.WriteFile(tempname, []byte(content), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}
	return nil
}

// Gets information about an append operation that was in progress during a crash
func (kv *DisKV) fileGetTempAppend(seq int) (string, error) {
	fullname := kv.dir + "/append-" + strconv.Itoa(seq)
	content, err := ioutil.ReadFile(fullname)
	return string(content), err
}

// Adds client info to disk, in order to support at-most-once guarentees - two sub-directories each store a "map" of file name to file content
func (kv *DisKV) fileSetClientInfo(client int64, id int64, ret string) error {
	fullname_id := kv.dir + "/ids/" + "client-" + strconv.Itoa(int(client))
	tempname_id := kv.dir + "/ids/" + "temp-" + strconv.Itoa(int(client))
	if err := ioutil.WriteFile(tempname_id, []byte(strconv.Itoa(int(id))), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname_id, fullname_id); err != nil {
		return err
	}

	fullname_res := kv.dir + "/results/" + "client-" + strconv.Itoa(int(client))
	tempname_res := kv.dir + "/results/" + "temp-" + strconv.Itoa(int(client))
	if err := ioutil.WriteFile(tempname_res, []byte(ret), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname_res, fullname_res); err != nil {
		return err
	}

	return nil
}

// Recovers client info from disk 
func (kv *DisKV) fileRecoverClientInfo() (map[int64]int64, map[int64]string) {
	id := map[int64]int64{}
	res := map[int64]string{}
	d_id := kv.dir + "/ids/"
	d_res := kv.dir + "/results/"
	files, err := ioutil.ReadDir(d_id)
	if err != nil {
		log.Fatalf("fileRecoverClientInfo could not read %v: %v", d_id, err)
	}
	for _, fi := range files {
		n1 := fi.Name()
		if n1[0:7] == "client-" {
			key, err := strconv.Atoi(n1[7:])
			if err != nil {
				log.Fatalf("fileRecoverClientInfo-ids bad file name %v: %v", n1, err)
			}
			fullname := kv.dir + "/ids/" + n1
			content, err := ioutil.ReadFile(fullname)
			if err != nil {
				log.Fatalf("fileRecoverClientInfo-ids fileGet failed for %v: %v", key, err)
			}
			v, _ := strconv.Atoi(string(content))
			k := int64(key)
			id[k] = int64(v)
		}
	}

	files, err = ioutil.ReadDir(d_res)
	if err != nil {
		log.Fatalf("fileRecoverClientInfo could not read %v: %v", d_res, err)
	}
	for _, fi := range files {
		n1 := fi.Name()
		if n1[0:7] == "client-" {
			key, err := strconv.Atoi(n1[7:])
			if err != nil {
				log.Fatalf("fileRecoverClientInfo-results bad file name %v: %v", n1, err)
			}
			fullname := kv.dir + "/results/" + n1
			content, err := ioutil.ReadFile(fullname)
			if err != nil {
				log.Fatalf("fileRecoverClientInfo-results fileGet failed for %v: %v", key, err)
			}
			res[int64(key)] = string(content)
		}
	}
	return id, res
}

// RPC handler for client Get requests
func (kv *DisKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Get() on server %d-%d.\n", kv.me, kv.gid)

	// execute and reply
	op := Op{Get, uuid(), *args}
	reply.Err, reply.Value = kv.execute(op)
	return nil
}

// RPC handler for client Put and Append requests
func (kv *DisKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Put-Append() on server %d-%d.\n", kv.me, kv.gid)

	// execute and reply
	op := Op{Put, uuid(), *args}
	reply.Err, _ = kv.execute(op)
	return nil
}

// RPC to hand off the data (and client data) of a shard passing out of this servers control
func (kv *DisKV) Inquire(args *InquireArgs, reply *InquireReply) error {
	// Do not lock this check or deadlock will occur in Concurrent Put/Get/Move
	if kv.config.Num < args.Config.Num {
		reply.Err = ErrNoConfig
		return nil
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Inquire(%d) on server %d-%d.\n", args.Config.Num, kv.me, kv.gid)

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

// Collect data for all shards arriving into this server's control
func (kv *DisKV) Reconfigure(config shardmaster.Config) bool {
	state := InquireReply{OK, map[string]string{}, map[int64]int64{}, map[int64]string{}}

	// get latest info from each group for each shard
	for i, gid := range kv.config.Shards {
		// only if the shard should be in our group but currently is not
		if config.Shards[i] == kv.gid && gid != kv.gid {
			args := InquireArgs{i, kv.config}
			var reply InquireReply
			for _, srv := range kv.config.Groups[gid] {
				if ok := call(srv, "DisKV.Inquire", &args, &reply); ok {
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

// wait for Paxos to decide on a sequence number for this op - blocking
func (kv *DisKV) waitForPaxos(seq int) Op {
	for to := 10 * time.Millisecond; ; {
		if decided, val := kv.px.Status(seq); decided {
			return val.(Op)
		}
		if time.Sleep(to); to < 10*time.Second {
			to *= 2
		}
	}
}

// check if we have done this op already but then crashed
func (kv *DisKV) status(op Op) (Err, string) {
	switch op.Name {
	case Get, Put:
		var key string
		var seq int64
		var client int64

		switch op.Name {
		case Get:
			key = op.Args.(GetArgs).Key
			seq = op.Args.(GetArgs).Seq
			client = op.Args.(GetArgs).Client
		case Put:
			key = op.Args.(PutAppendArgs).Key
			seq = op.Args.(PutAppendArgs).Seq
			client = op.Args.(PutAppendArgs).Client
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

// literally just does the given op, immediately
func (kv *DisKV) do(res Op) {
	switch res.Name {
	case Get:
		previous, _ := kv.store[res.Args.(GetArgs).Key]
		id := res.Args.(GetArgs).Seq
		client := res.Args.(GetArgs).Client

		kv.fileSetClientInfo(client, id, previous)

		kv.ret[client] = previous
		kv.ids[client] = id
	case Put:
		id := res.Args.(PutAppendArgs).Seq
		client := res.Args.(PutAppendArgs).Client
		previous, _ := kv.store[res.Args.(PutAppendArgs).Key]

		// store to disk
		kv.fileSetClientInfo(client, id, previous)

		// store to memory
		kv.ret[client] = previous
		kv.ids[client] = id

		if res.Args.(PutAppendArgs).Op == "Append" {
			// do append
			val := previous + res.Args.(PutAppendArgs).Value
			key := res.Args.(PutAppendArgs).Key
			kv.fileTempAppend(kv.seq, val)
			DPrintf("Put-Append() stored to temp file at seq(%d) on server %d-%d.\n", kv.seq, kv.me, kv.gid)
			kv.filePut(key2shard(key), key, val)
			kv.store[key] = val
			DPrintf("Put-Append() implemented at seq(%d) on server %d-%d.\n", kv.seq, kv.me, kv.gid)
		} else {
			val := res.Args.(PutAppendArgs).Value
			key := res.Args.(PutAppendArgs).Key

			kv.filePut(key2shard(key), key, val)
			kv.store[res.Args.(PutAppendArgs).Key] = res.Args.(PutAppendArgs).Value
		}
	case Reconfigure:
		reconfig := res.Args.(ReconfigArgs).Inquiry

		for key := range reconfig.Store {
			kv.store[key] = reconfig.Store[key]
			kv.filePut(key2shard(key), key, reconfig.Store[key])
		}
		for client := range reconfig.Seen {
			if seqq, exists := kv.ids[client]; !exists || seqq <= reconfig.Seen[client] {
				kv.ids[client] = reconfig.Seen[client]
				kv.ret[client] = reconfig.Replies[client]
				kv.fileSetClientInfo(client, reconfig.Seen[client], reconfig.Replies[client])
			}
		}
		kv.fileIncrementConfig(res.Args.(ReconfigArgs).Config.Num)
		kv.config = res.Args.(ReconfigArgs).Config
	case Nop:
		// we are literally just catching up on Paxos logs here - do nothing
	}
}

// carry out this op and all previous un-done ops, apply the effects to the store
func (kv *DisKV) execute(op Op) (Err, string) {
	res := Op{Nil, -1, nil}
	for res.Id != op.Id {
		if result, ret := kv.status(op); result != Nil {
			return result, ret
		}

		kv.seq++
		kv.fileIncrementSeq(kv.seq)

		if decided, t := kv.px.Status(kv.seq); decided {
			res = t.(Op)
		} else {
			kv.px.Start(kv.seq, op)
			res = kv.waitForPaxos(kv.seq)
		}

		kv.fileLogOp(res, kv.seq)
		kv.do(res)
		kv.px.Done(kv.seq)
	}

	var client int64
	switch op.Name {
	case Get:
		client = op.Args.(GetArgs).Client
	case Put:
		client = op.Args.(PutAppendArgs).Client
	}
	return OK, kv.ret[client]
}

func (kv *DisKV) restore() bool {
	DPrintf("Restore: %d on server %d-%d.\n", kv.seq, kv.me, kv.gid)

	res, err := kv.fileRetrieveOp(kv.seq)
	if err != nil {
		DPrintf("Restore: %d did not reach log-op checkpoint; will resolve through Paxos; server %d-%d.\n", kv.seq, kv.me, kv.gid)
		// decrement the local sequencing to be before the Paxos check
		kv.seq--
		kv.fileIncrementSeq(kv.seq)
		return false
	}

	DPrintf("Restoring op: %s() on server %d-%d.\n", res.Name, kv.me, kv.gid)
	switch res.Name {
	case Get:
		kv.do(res)
		return true
	case Put:
		args := res.Args.(PutAppendArgs)
		if args.Op == "Put" {
			kv.do(res)
			return true
		} else {
			DPrintf("Restoring Put-Append() on server %d-%d.\n", kv.me, kv.gid)
			val, err := kv.fileGetTempAppend(kv.seq)
			if err != nil {
				DPrintf("Restoring Put-Append(); err = %s; server %d-%d.\n", err, kv.me, kv.gid)
				DPrintf("Restoring Put-Append(); did not find temp file; redoing; server %d-%d.\n", kv.me, kv.gid)
				kv.do(res)
				return true
			}
			DPrintf("Restoring Put-Append(); found temp file with value []. On server %d-%d.\n", kv.me, kv.gid)
			key := args.Key
			kv.filePut(key2shard(key), key, val)
			kv.store[key] = val
			return true
		}
	case Reconfigure:
		kv.do(res)
		return true
	}

	kv.seq--
	return false
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
// Keep Paxos active in order to make sure that all servers are up-to-date and garbage-collected
func (kv *DisKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//DPrintf("Tick() on server %d-%d.\n", kv.me, kv.gid)

	// check for new configurations since our last tick and apply any changes
	next := kv.sm.Query(-1)
	if next.Num == kv.config.Num {
		kv.execute(Op{Nop, uuid(), nil})
	}

	for i := kv.config.Num + 1; i <= next.Num; i++ {
		if !kv.Reconfigure(kv.sm.Query(i)) {
			break
		}
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *DisKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *DisKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *DisKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *DisKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
// dir is the directory name under which this
//   replica should store all its files.
//   each replica is passed a different directory.
// restart is false the very first time this server
//   is started, and true to indicate a re-start
//   after a crash or after a crash with disk loss.
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int, dir string, restart bool) *DisKV {

	kv := new(DisKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.dir = dir

	// needed to marshal these structs into RPCs
	gob.Register(GetArgs{})
	gob.Register(PutAppendArgs{})
	gob.Register(ReconfigArgs{})

	// Your initialization code here.
	// Don't call Join().

	paxosdir := kv.dir + "/state/"

	if !restart {
		// Initialize to empty
		kv.store = map[string]string{}
		kv.ids = map[int64]int64{}
		kv.ret = map[int64]string{}
		kv.seq = 0
		kv.config = shardmaster.Config{Num: -1}

		// client and paxos sub-directories
		if err := os.Mkdir(kv.dir+"/ids/", 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", kv.dir+"/ids/", err)
		}
		if err := os.Mkdir(kv.dir+"/results/", 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", kv.dir+"/results/", err)
		}
		if err := os.Mkdir(paxosdir, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", paxosdir, err)
		}

	} else {
		// Recover client and operational info from client
		kv.ids, kv.ret = kv.fileRecoverClientInfo()
		knownSeq, knownConfig := kv.fileRecoverPaxosInfo()
		kv.seq = knownSeq

		// Recover configuration by querying shardmaster
		kv.config = kv.sm.Query(knownConfig)

		// load the key value store from disk
		kv.store = map[string]string{}
		for shard, group := range kv.config.Shards {
			if group != gid {
				continue
			}
			shard_store := kv.fileReadShard(shard)
			for k, v := range shard_store {
				kv.store[k] = v
			}
		}

		// restore the paxos op in progress - we don't know whether or not it completed either partially or wholly
		kv.restore()

	}

	// log.SetOutput(ioutil.Discard)

	gob.Register(Op{})

	rpcs := rpc.NewServer()
	rpcs.Register(kv)
	// log.SetOutput(os.Stdout)

	// Parameters added to paxos in order to store paxos state on disk - only store if the middle flag is "true"
	kv.px = paxos.Make(servers, me, rpcs, true, paxosdir, restart)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.isdead() == false {
				fmt.Printf("DisKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
