package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

const (
	WAITING     = 0
	IN_PROGRESS = 1
	FINISH      = 2
)

type Machine struct {
	Path string
}

type Task struct {
	State         int
	WorkerMachine Machine
	StartTime     time.Time
}

type AskMapArgs struct {
	WorkerMachine Machine
}

type AskMapReply struct {
	Path     string
	FileName string
	TaskId   int
	Nreduce  int
	Over     bool
}

type TaskOverArgs struct {
	TaskId int
}

type TaskOverReply struct{}

type AskReduceArgs struct {
	WorkerMachine Machine
}

type AskReduceReply struct {
	IntermediateMachine []Machine
	TaskId              int
	Over                bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
