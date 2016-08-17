package kvpaxos

import "net/rpc"
import "fmt"
import "crypto/rand"
import "bytes"
import "encoding/binary"
import "time"

type Clerk struct {
	servers []string
	me      uint64 // stable id representing this client in all RPC calls
	// You will have to modify this struct.
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.me = ck.uuid()
	// You'll have to add code here.
	return ck
}

//
// Generates a 64-bit UUID
//
func (ck *Clerk) uuid() uint64 {
	// get 8 random bytes
	rbytes := make([]byte, 8)
	n, err := rand.Read(rbytes)
	for n < 8 || err != nil {
		n, err = rand.Read(rbytes)
	}

	// read those bytes into a variable
	var randid uint64
	binary.Read(bytes.NewReader(rbytes), binary.LittleEndian, &randid)

	return randid
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	DPrintf("Client Get(%s) \n", key)
	// You will have to modify this function.
	args := GetArgs{key, ck.me, ck.uuid()}
	var reply GetReply

	// try different servers until one responds
	for i := 0; true; i++ {
		ok := call(ck.servers[i%len(ck.servers)], "KVPaxos.Get", args, &reply)
		if ok {
			return reply.Value
		}
		time.Sleep(100 * time.Millisecond)
	}

	return ""
}

//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	// You will have to modify this function.
	args := PutArgs{key, value, dohash, ck.me, ck.uuid()}
	var reply PutReply

	for i := 0; true; i++ {
		ok := call(ck.servers[i%len(ck.servers)], "KVPaxos.Put", args, &reply)
		if ok {
			return reply.PreviousValue
		}
		time.Sleep(100 * time.Millisecond)
	}

	return ""
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("Client Put(%s, %s)\n", key, value)
	ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	DPrintf("Client PutHash(%s, %s)\n", key, value)
	v := ck.PutExt(key, value, true)
	return v
}
