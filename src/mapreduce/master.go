package mapreduce

import "container/list"
import "fmt"
import "strconv"
import "math/rand"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	id string
}

type JobResult struct {
	jobNum     int
	jobSuccess bool
	workerId   string
}

func ConsumeJob(workerInfo *WorkerInfo, args *DoJobArgs, r chan JobResult) {
	var reply DoJobReply
	call(workerInfo.address, "Worker.DoJob", args, &reply)
	r <- JobResult{jobNum: args.JobNumber, jobSuccess: reply.OK, workerId: workerInfo.id}
}

func (mr *MapReduce) GetRandAvailableWorker() (workerInfo *WorkerInfo) {
	for {
		mr.RLock()
		n := len(mr.Workers)
		if n == 0 {
			mr.RUnlock()
			continue
		}
		index := rand.Intn(n)
		for key, val := range mr.Workers {
			if index == 0 {
				mr.RUnlock()
				return &WorkerInfo{address: val.address, id: key}
			}
			index--
		}
	}
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
	// Your code here
	//get registered workers in channel
	workerId := 0
	workerAddress := <-mr.registerChannel                                    //block here until first worker is registered in the channel
	mr.Workers[strconv.Itoa(workerId)] = &WorkerInfo{address: workerAddress} //store first worker info
	workerId++
	//iteratively store all worker info
	go func() {
		for {
			workerAddress := <-mr.registerChannel
			mr.Lock()
			mr.Workers[strconv.Itoa(workerId)] = &WorkerInfo{address: workerAddress}
			workerId++
			mr.Unlock()
		}
	}()

	//map
	r := make(chan JobResult) // channel for reduce jobs' result
	go func() {
		for i := 0; i < mr.nMap; i++ {
			args := &DoJobArgs{File: mr.file, Operation: Map, JobNumber: i, NumOtherPhase: mr.nReduce}
			go ConsumeJob(mr.GetRandAvailableWorker(), args, r)
		}
	}()

	//wait map and failure tolerance
	cnt := 0
	for cnt < mr.nMap {
		result := <-r
		if result.jobSuccess {
			cnt++
		} else { //if map job failed, select a new worker to consume this job
			mr.Lock()
			delete(mr.Workers, result.workerId) //claim the worker is not available anymore
			mr.Unlock()

			args := &DoJobArgs{File: mr.file, Operation: Map, JobNumber: result.jobNum, NumOtherPhase: mr.nReduce}
			go ConsumeJob(mr.GetRandAvailableWorker(), args, r)
		}
	}

	//reduce
	go func() {
		for i := 0; i < mr.nReduce; i++ {
			args := &DoJobArgs{File: mr.file, Operation: Reduce, JobNumber: i, NumOtherPhase: mr.nMap}
			go ConsumeJob(mr.GetRandAvailableWorker(), args, r)
		}
	}()

	//wait reduce and failure tolerance
	cnt = 0
	for cnt < mr.nReduce {
		result := <-r
		if result.jobSuccess {
			cnt++
		} else { // if reduce job failed, select a new worker to consume the job
			mr.Lock()
			delete(mr.Workers, result.workerId)
			mr.Unlock()
			args := &DoJobArgs{File: mr.file, Operation: Reduce, JobNumber: result.jobNum, NumOtherPhase: mr.nMap}
			go ConsumeJob(mr.GetRandAvailableWorker(), args, r)
		}
	}

	return mr.KillWorkers()
}
