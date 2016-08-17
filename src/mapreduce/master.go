package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
    address string
    // You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
    l := list.New()
    for _, w := range mr.Workers {
        DPrintf("DoWork: shutdown %s\n", w.address)
        args := &ShutdownArgs{}
        var reply ShutdownReply
        ok := call(w.address, "Worker.Shutdown", args, &reply)
        if ok == false {
            fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
        } else {
            l.PushBack(reply.Njobs)
        }
    }
    return l
}

func (mr *MapReduce) RunMaster() *list.List {
    // as workers register, have them wait on the readyChannel
    go mr.registerWorkers()

    // delegate the map jobs
    for i := 0; i < mr.nMap; i++ {
        go mr.delegateJob(Map, i)
    }
    // wait for the map jobs to complete
    for i := 0; i < mr.nMap; i++ {
        <-mr.doneChannel    // sending/receiving block
    }

    // delegate the reduce jobs
    for i := 0; i < mr.nReduce; i++ {
        go mr.delegateJob(Reduce, i)
    }
    // wait for the reduce jobs to complete
    for i := 0; i < mr.nReduce; i++ {
        <-mr.doneChannel
    }

    return mr.KillWorkers()
}

// Wait for workers to register and place them on the ready channel
func (mr *MapReduce) registerWorkers() {
    for {
        address := <-mr.registerChannel
        mr.Workers[address] = &WorkerInfo{address}  // for shutdown routine
        mr.readyChannel <- address
    }
}

// Sends a job request to the next available worker until the request is
// successful. There will be one call to delegateJob per job (nMap + nReduce
// calls total).
func (mr *MapReduce) delegateJob(jtype JobType, jno int) {
    // currently, we loop indefintely until the job is completed
    for {
        // wait for an available worker then perform an RPC for the job
        worker := <-mr.readyChannel
        // if the request was successful, report completion, move worker
        // back to the ready channel, and return
        if ok := mr.assignJob(worker, jtype, jno); ok {
            // the doneChannel could be used to track number of failures
            mr.doneChannel <- true
            mr.readyChannel <- worker
            return
        }
        // else, loop and wait for another worker
    }
}

// Performs an RPC to a worker for a particlar job. Returns true if the call was
// successful.
func (mr *MapReduce) assignJob(worker string, jtype JobType, jno int) bool {
    var args DoJobArgs
    switch jtype {
        case Map:
            args = DoJobArgs{mr.file, Map, jno, mr.nReduce}
        case Reduce:
            args = DoJobArgs{mr.file, Reduce, jno, mr.nMap}
    }
    var reply DoJobReply
    return call(worker, "Worker.DoJob", args, &reply)
}
