package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
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

// Implemented workerID.Add your RPC definitions here.

type JoinArgs struct {
}

type JoinReply struct {
	WorkerId int
}

type ReportArgs struct {
	TaskId    int
	TaskPhase string //"mapPhase" or "reducePhase"
	Done      bool
	// Error     bool
	WorkerId int
}

type ReportReply struct {
}

type RequestArgs struct {
	WorkerId int
}

type RequestReply struct {
	Task *Task
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
