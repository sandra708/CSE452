package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	ticks		int				// number of ticks since initialization
	pings 		map[string]int 			// time we last heard from each server
	current		View				// the current view
	ack 		bool				// has the current view been acknowledged?

}

//
// Returns an active idle server. As it looks for an idle server, if it notices
// a server is 'dead', it will remove it from the pings map. But it may not
// remove all or any 'dead' servers.
//
func (vs *ViewServer) getIdle() string {
	for server, ticks := range vs.pings {
		if vs.ticks - ticks > DeadPings {
			// this server is 'dead'
			delete(vs.pings, server)
		} else if server != vs.current.Primary && server != vs.current.Backup {
			return server
		}
	}
	return ""
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// Lock the whole function (read to vs.next as well)
	vs.mu.Lock()
	defer vs.mu.Unlock()

	server := args.Me

	// if this is the first server we've heard from, it becomes the primary
  	if vs.current.Viewnum == 0 {
  		vs.current.Viewnum = 1
  		vs.current.Primary = server
		vs.ack = false
  		reply.View = vs.current
  		return nil
  	}

	vs.pings[server] = vs.ticks

	switch server {
	case vs.current.Primary:
		switch args.Viewnum {
		case vs.current.Viewnum:
			// acknowledge the new view
			vs.ack = true
		case 0:
			// primary crashed!
			if !vs.ack {
				log.Fatal("primary crashed before acknowledging view")
			}
			vs.current.Viewnum++
			vs.current.Primary = vs.current.Backup
			vs.current.Backup = vs.getIdle()
			vs.ack = false
		}
	case vs.current.Backup:
		if args.Viewnum == 0 {
			// backup crashed!
			if !vs.ack {
				log.Fatal("backup crashed before primary acknowledged view")
				// here it would be more sensible to eliminate the backup from the view 
				// and promote a new backup

				// however, we can't transition the view until it has been ack'ed, 
				// and remaining in the current view would cause inconsistencies 
				// (what if the primary's ack is already in transit?)
			}
			vs.current.Viewnum++
			vs.current.Backup = vs.getIdle()
			vs.ack = false
		}
	default:
		// if we don't have a backup, promote the new server
		if vs.current.Backup == "" && vs.ack {
			vs.current.Viewnum++
			vs.current.Backup = server
			vs.ack = false
		}
	}

	// After updating the state - return the most recent view
	reply.View = vs.current

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	reply.View = vs.current
	return nil
}

//
// tick() is called once per PingInterval; it checks that the primary and backup
// servers are responsive and updates the view if not.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()

    vs.ticks++

    if vs.current.Viewnum == 0 {
    	// haven't heard from any servers yet, there's nothing to update
        return
    }
    if !vs.ack {
    	// we have an unacknowledged view, do nothing
    	return
    }

    // Since we'd expect tick() to be called more often than getIdle(), tick()
    // only checks on the primary and backup servers. The other servers are
    // checked when an idle server is needed. This should reduce the overall
    // amount of time tick() holds the lock and more so as the number of idle
    // servers increases
    pticks := vs.pings[vs.current.Primary]
    bticks := vs.pings[vs.current.Backup]

	if vs.ticks - pticks > DeadPings {
		// primary has failed
		delete(vs.pings, vs.current.Primary)
		vs.current.Viewnum++
		vs.current.Primary = vs.current.Backup
		vs.current.Backup = vs.getIdle()
	} else if vs.ticks - bticks > DeadPings {
		// backup has failed
		delete(vs.pings, vs.current.Backup)
		vs.current.Viewnum++
		vs.current.Backup = vs.getIdle()
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.ticks = 0
	vs.pings = make(map[string]int)
	vs.current = View{0, "", ""}
	vs.ack = false

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
