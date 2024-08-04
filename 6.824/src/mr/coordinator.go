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

type Coordinator struct {
	// Your definitions here.
	MapTask      []Task
	ReduceTask   []Task
	Path         string
	FileName     []string
	MapRemain    int
	ReduceRemain int
	mu           sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AskMapTask(args *AskMapArgs, reply *AskMapReply) error {
	curTime := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.MapRemain == 0 {
		reply.TaskId = -1
		reply.Over = true
		return nil
	}
	for i, task := range c.MapTask {
		dur := curTime.Sub(c.MapTask[i].StartTime)
		// timeout or still waiting
		if (dur.Seconds() > 10.0 && task.State == IN_PROGRESS) || task.State == WAITING {
			c.MapTask[i].StartTime = curTime
			c.MapTask[i].State = IN_PROGRESS
			c.MapTask[i].WorkerMachine = Machine{args.WorkerMachine.Path} //create a new object
			reply.Path = c.Path
			reply.FileName = c.FileName[i]
			reply.TaskId = i
			reply.Nreduce = len(c.ReduceTask)
			reply.Over = false
			return nil
		}
	}
	reply.TaskId = -1
	reply.Over = false
	return nil
}

func (c *Coordinator) MapTaskOver(args *TaskOverArgs, reply *TaskOverReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	idx := args.TaskId
	c.MapTask[idx].State = FINISH
	c.MapRemain -= 1
	return nil
}

func (c *Coordinator) AskReduceTask(args *AskReduceArgs, reply *AskReduceReply) error {
	curTime := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ReduceRemain == 0 {
		reply.TaskId = -1
		reply.Over = true
		return nil
	}
	for i, task := range c.ReduceTask {
		dur := curTime.Sub(c.ReduceTask[i].StartTime)
		// timeout or still waiting
		if (dur.Seconds() > 10.0 && task.State == IN_PROGRESS) || task.State == WAITING {
			c.ReduceTask[i].StartTime = curTime
			c.ReduceTask[i].State = IN_PROGRESS
			c.ReduceTask[i].WorkerMachine = Machine{args.WorkerMachine.Path}
			for _, task := range c.MapTask {
				reply.IntermediateMachine = append(reply.IntermediateMachine, task.WorkerMachine)
			}
			reply.TaskId = i
			reply.Over = false
			return nil
		}
	}
	reply.TaskId = -1
	reply.Over = false
	return nil
}

func (c *Coordinator) ReduceTaskOver(args *TaskOverArgs, reply *TaskOverReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	idx := args.TaskId
	c.ReduceTask[idx].State = FINISH
	c.ReduceRemain -= 1
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ReduceRemain == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	nMap := len(files)
	c.MapTask = make([]Task, nMap)
	c.ReduceTask = make([]Task, nReduce)
	path, err := os.Getwd()
	if err != nil {
		log.Fatal("open error")
	}
	c.Path = path
	c.FileName = make([]string, nMap)
	c.MapRemain = nMap
	c.ReduceRemain = nReduce
	for i, filename := range files {
		if err != nil {
			log.Fatal("open file %s error", filename)
		}
		c.FileName[i] = filename
	}
	c.server()
	return &c
}
