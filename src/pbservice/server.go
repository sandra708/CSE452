package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

import "strconv"

// Debugging
const Debug = false     // boolean since there's only one debug level

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug {
        n, err = fmt.Printf(format, a...)
    }
    return
}

type PBServer struct {
    l          net.Listener
    dead       bool // for testing
    unreliable bool // for testing
    me         string
    vs         *viewservice.Clerk
    done       sync.WaitGroup
    finish     chan interface{}
    // Your declarations here.
    vshost     string               // for call()-ing Viewserver.Ping
    view       viewservice.View     // the current view
    store      map[string]string    // our key-value store
    gets       map[uint64]GetEntry  // a record or op requests seen
    puts       map[uint64]PutEntry  // a record or op requests seen
    mu         sync.Mutex

}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
    pb.mu.Lock()
    defer pb.mu.Unlock()

    DPrintf("Server Put(%s, %s)\n", args.Key, args.Value)

    // only the primary can execute Put ops
    if pb.me != pb.view.Primary {
        reply.PreviousValue = ""
        reply.Err = ErrWrongServer
        return nil
    }

    // if we've seen this op id before, return the previous values
    if entry, exists := pb.puts[args.Id]; exists {
        if args.Client == entry.Client {
            reply.Err = entry.Reply.Err
            reply.PreviousValue = entry.Reply.PreviousValue
            return nil
        }
    }

    if args.DoHash {
        // check for an old value or use the empty string
        if value, exists := pb.store[args.Key]; exists {
            reply.PreviousValue = value
        } else {
            reply.PreviousValue = ""
        }
        args.Value = strconv.Itoa(int(hash(reply.PreviousValue + args.Value)))
    }
    reply.Err = OK

    // if we have a backup, forward the Put op to it
    if pb.view.Backup != "" {
        fargs := ForwardPutArgs{args, reply}
        var freply ForwardReply

        ok := call(pb.view.Backup, "PBServer.ForwardPut", &fargs, &freply)
        if !ok || freply.Err != OK {
            reply.PreviousValue = ""
            reply.Err = ErrBackup
            return nil
        }
    }

    // now that the backup has completed the op, we complete it and make a
    // log entry of it
    pb.puts[args.Id] = PutEntry{*reply, args.Client}
    pb.store[args.Key] = args.Value

    return nil
}

//
// RPC - Forward Put value/reply from Primary to Backup
//
func (pb *PBServer) ForwardPut(args *ForwardPutArgs, reply *ForwardReply) error {
    pb.mu.Lock()
    defer pb.mu.Unlock()

    DPrintf("Server ForwardPut(%s, %s)\n", args.Args.Key, args.Args.Value)

    // only the backup server can accept forwarded ops
    if pb.me != pb.view.Backup {
        reply.Err = ErrWrongServer
        return nil
    }

    // perform the ops on our own store and make a log entry of it
    pb.puts[args.Args.Id] = PutEntry{*args.Reply, args.Args.Client}
    pb.store[args.Args.Key] = args.Args.Value

    reply.Err = OK

    return nil
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
    pb.mu.Lock()
    defer pb.mu.Unlock()

    DPrintf("Server Get(%s)\n", args.Key)

    // only the primary can execute Get ops
    if pb.me != pb.view.Primary {
        reply.Value = ""
        reply.Err = ErrWrongServer
        return nil
    }

    // if we've seen this op id before, return the same values
    if entry, exists := pb.gets[args.Id]; exists {
        if args.Client == entry.Client {
            reply.Err = entry.Reply.Err
            reply.Value = entry.Reply.Value
            return nil
        }
    }

    // if we've already stored a value for this key, return it or else
    // return the empty srtring
    if value, exists := pb.store[args.Key]; exists {
        reply.Err = OK
        reply.Value = value
    } else {
        reply.Err = ErrNoKey
        reply.Value = ""
    }

    // if we have a backup right now, forward the get op to it
    if pb.view.Backup != "" {
        fargs := ForwardGetArgs{args, reply}
        var freply ForwardReply

        ok := call(pb.view.Backup, "PBServer.ForwardGet", &fargs, &freply)
        if  !ok || freply.Err != OK {
            reply.Err = ErrBackup
            reply.Value = ""
            return nil
        }
    }

    // now that the backup has completed the request, we complete it and make
    // a log entry of it
    pb.gets[args.Id] = GetEntry{*reply, args.Client}

    return nil
}

//
// RPC - Forward Get reply from Primary to Backup
//
func (pb *PBServer) ForwardGet(args *ForwardGetArgs, reply *ForwardReply) error {
    pb.mu.Lock()
    defer pb.mu.Unlock()

    DPrintf("Server ForwardGet(%s)\n", args.Args.Key)

    // only the backup server can accept forwarded ops
    if pb.me != pb.view.Backup {
        reply.Err = ErrWrongServer
        return nil
    }

    // the backup only needs to log the reply
    pb.gets[args.Args.Id] = GetEntry{*args.Reply, args.Args.Client}

    reply.Err = OK

    return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
    pb.mu.Lock()
    defer pb.mu.Unlock()

    DPrintf("-\n")

    args := viewservice.PingArgs{pb.me, pb.view.Viewnum}
    var reply viewservice.PingReply

    // ping the veiwserver
    if ok := call(pb.vshost, "ViewServer.Ping", &args, &reply); !ok {
        return
    }

    // if we are the primary and the view has changed, forward our state to the
    // potential new backup
    if reply.View.Primary == pb.me && pb.view.Viewnum != reply.View.Viewnum {
        if reply.View.Backup != "" {
            fargs := ForwardStateArgs{pb.me, pb.store, pb.gets, pb.puts}
            var freply ForwardReply

            ok := call(reply.View.Backup, "PBServer.ForwardState", &fargs, &freply)
            if !ok || freply.Err != OK {
                return
            }
        }
    }

    pb.view = reply.View
}

//
// RPC - Forward state from Primary to Backup
//
func (pb *PBServer) ForwardState(args *ForwardStateArgs, reply *ForwardReply) error {
    pb.mu.Lock()
    defer pb.mu.Unlock()

    DPrintf("Server ForwardState()\n")

    // only the backup can accept state and only from who it thinks is
    // the current primary
    if pb.me != pb.view.Backup {
        reply.Err = ErrWrongServer
        return nil
    }

    pb.store = args.Store
    pb.gets = args.Gets
    pb.puts = args.Puts

    reply.Err = OK

    return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
    pb.dead = true
    pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
    pb := new(PBServer)
    pb.me = me
    pb.vs = viewservice.MakeClerk(me, vshost)
    pb.finish = make(chan interface{})
    // Your pb.* initializations here.
    pb.vshost = vshost
    pb.view = viewservice.View{0, "", ""}
    pb.store = make(map[string]string)
    pb.gets = make(map[uint64]GetEntry)
    pb.puts = make(map[uint64]PutEntry)

    rpcs := rpc.NewServer()
    rpcs.Register(pb)

    os.Remove(pb.me)
    l, e := net.Listen("unix", pb.me)
    if e != nil {
        log.Fatal("listen error: ", e)
    }
    pb.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    go func() {
        for pb.dead == false {
            conn, err := pb.l.Accept()
            if err == nil && pb.dead == false {
                if pb.unreliable && (rand.Int63()%1000) < 100 {
                    // discard the request.
                    conn.Close()
                } else if pb.unreliable && (rand.Int63()%1000) < 200 {
                    // process the request but force discard of reply.
                    c1 := conn.(*net.UnixConn)
                    f, _ := c1.File()
                    err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
                    if err != nil {
                        fmt.Printf("shutdown: %v\n", err)
                    }
                    pb.done.Add(1)
                    go func() {
                        rpcs.ServeConn(conn)
                        pb.done.Done()
                    }()
                } else {
                    pb.done.Add(1)
                    go func() {
                        rpcs.ServeConn(conn)
                        pb.done.Done()
                    }()
                }
            } else if err == nil {
                conn.Close()
            }
            if err != nil && pb.dead == false {
                fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
                pb.kill()
            }
        }
        DPrintf("%s: wait until all request are done\n", pb.me)
        pb.done.Wait()
        // If you have an additional thread in your solution, you could
        // have it read to the finish channel to hear when to terminate.
        close(pb.finish)
    }()

    pb.done.Add(1)
    go func() {
        for pb.dead == false {
            pb.tick()
            time.Sleep(viewservice.PingInterval)
        }
        pb.done.Done()
    }()

    return pb
}
