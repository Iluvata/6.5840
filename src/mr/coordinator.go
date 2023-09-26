package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const lagtime = 10

const (
	unstarted = 0
	running   = 1
	finished  = 2
	lagged    = 3
)

type mrStat struct {
	stat     int
	workerID int
}

type Coordinator struct {
	// Your definitions here.
	nMap            int
	nReduce         int
	mapDone         bool
	reduceDone      bool
	mapStat         []mrStat
	reduceStat      []mrStat
	mapStraggler    chan int
	reduceStraggler chan int
	files           []string

	mapMux    sync.Mutex
	reduceMux sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) testAssignMap(args *TaskArgs, reply *TaskReply) bool {
	var i int
	var finicnt int = 0
	c.mapMux.Lock()
	for i = 0; i < c.nMap; i++ {
		if c.mapStat[i].stat == unstarted || c.mapStat[i].stat == lagged {
			c.mapStat[i].stat = running
			c.mapStat[i].workerID = args.WorkerID
			break
		} else if c.mapStat[i].stat == finished {
			finicnt += 1
		}
	}
	if finicnt == c.nMap {
		c.mapDone = true
		close(c.mapStraggler)
	}
	c.mapMux.Unlock()
	if i < c.nMap {
		// assign map task
		reply.TaskID = i
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce
		reply.Filename = c.files[i]
		reply.MapTask = true

		go func() {
			i := i
			WorkerID := args.WorkerID
			timer := time.NewTimer(time.Second * lagtime)
			<-timer.C
			c.mapMux.Lock()
			if c.mapStat[i].workerID == WorkerID && c.mapStat[i].stat == running {
				c.mapStat[i].stat = lagged
				c.mapStraggler <- i
			}
			c.mapMux.Unlock()
		}()
		return true
	}
	return false
}

func (c *Coordinator) checkMapDone() bool {
	c.mapMux.Lock()
	defer c.mapMux.Unlock()
	return c.mapDone
}

func (c *Coordinator) testAssignReduce(args *TaskArgs, reply *TaskReply) bool {
	var i int
	var finicnt int = 0
	c.reduceMux.Lock()
	for i = 0; i < c.nReduce; i++ {
		if c.reduceStat[i].stat == unstarted || c.reduceStat[i].stat == lagged {
			c.reduceStat[i].stat = running
			c.reduceStat[i].workerID = args.WorkerID
			break
		} else if c.reduceStat[i].stat == finished {
			finicnt += 1
		}
	}
	if finicnt == c.nReduce {
		c.reduceDone = true
		close(c.reduceStraggler)
	}
	c.reduceMux.Unlock()
	if i < c.nReduce {
		// assign reduce task
		reply.TaskID = i
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce
		// reply.Filename = c.files[i]
		reply.MapTask = false

		go func() {
			i := i
			WorkerID := args.WorkerID
			timer := time.NewTimer(time.Second * lagtime)
			<-timer.C
			c.reduceMux.Lock()
			if c.reduceStat[i].workerID == WorkerID && c.reduceStat[i].stat == running {
				c.reduceStat[i].stat = lagged
				c.reduceStraggler <- i
			}
			c.reduceMux.Unlock()
		}()
		return true
	}
	return false
}

func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	for !c.checkMapDone() {
		if c.testAssignMap(args, reply) {
			return nil
		}
		<-c.mapStraggler
	}
	for !c.Done() {
		if c.testAssignReduce(args, reply) {
			return nil
		}
		<-c.reduceStraggler
	}
	reply.Done = true
	return nil
}

func (c *Coordinator) FinishTask(args *TaskDoneArgs, reply *TaskDoneReply) error {
	if !c.checkMapDone() && args.MapTask {
		c.mapMux.Lock()
		c.mapStat[args.TaskID].stat = finished
		c.mapMux.Unlock()
	} else if !args.MapTask {
		c.reduceMux.Lock()
		c.reduceStat[args.TaskID].stat = finished
		c.reduceMux.Unlock()
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.reduceMux.Lock()
	defer c.reduceMux.Unlock()
	return c.reduceDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nMap = len(files)
	c.nReduce = nReduce
	c.files = files
	c.mapStat = make([]mrStat, c.nMap)
	c.reduceStat = make([]mrStat, nReduce)
	c.mapStraggler = make(chan int)
	c.reduceStraggler = make(chan int)
	// c.mapDone = false

	c.server()
	return &c
}
