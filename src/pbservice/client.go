package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

// You'll probably need to uncomment these:
import "time"
import "crypto/rand"
import "bytes"
import "encoding/binary"

type Clerk struct {
    vs     *viewservice.Clerk
    // Your declarations here
    vshost string
    me     string
    view   viewservice.View
}

func MakeClerk(vshost string, me string) *Clerk {
    ck := new(Clerk)
    ck.vs = viewservice.MakeClerk(me, vshost)
    // Your ck.* initializations here
    ck.vshost = vshost
    ck.me = me
    ck.view = viewservice.View{0, "", ""}
    return ck
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
// Pings the viewserver and updates the current view
//
func (ck *Clerk) vsping() {
    args := viewservice.PingArgs{ck.me, ck.view.Viewnum}
    var reply viewservice.PingReply

    // ping the veiwserver
    if ok := call(ck.vshost, "ViewServer.Ping", &args, &reply); !ok {
        return
    }
    ck.view = reply.View
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {
    DPrintf("Client Get(%s) \n", key)

    args := GetArgs{key, ck.uuid(), ck.me}
    var reply GetReply

    // if this is the first request, ping for view
    if ck.view.Viewnum == 0 {
        ck.vsping()
    }

    // retry until the RPC is successful
    ok := call(ck.view.Primary, "PBServer.Get", &args, &reply)
    for !ok || (reply.Err != OK && reply.Err != ErrNoKey) {
        // sleep for a tick()
        time.Sleep(viewservice.PingInterval)
        // check if the view changed
        ck.vsping()
        ok = call(ck.view.Primary, "PBServer.Get", &args, &reply)
    }

    return reply.Value
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
    args := PutArgs{key, value, dohash, ck.uuid(), ck.me}
    var reply PutReply

    // if this is the first request, ping for view
    if ck.view.Viewnum == 0 {
        ck.vsping()
    }

    // retry until the RPC is successful
    ok := call(ck.view.Primary, "PBServer.Put", &args, &reply)
    for !ok || reply.Err != OK {
        // sleep for a tick()
        time.Sleep(viewservice.PingInterval)
        // check if the view changed
        ck.vsping()
        ok = call(ck.view.Primary, "PBServer.Put", &args, &reply)
    }

    return reply.PreviousValue
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
