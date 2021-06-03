package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ClaimArgs struct {
	Wid string
}

type ClaimReply struct {
	T       task
	NReduce int
}

type FinishArgs struct {
	T task
}

type FinishReply struct {
	F bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can'T use the current directory since
// Athena AFS doesn'T support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
