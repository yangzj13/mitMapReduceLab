package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type state byte

const (
	IDLE    state = 0
	RUNNING state = 1
	FINISH  state = 2
)

type taskType byte

const (
	MAP    taskType = 0
	REDUCE taskType = 1
	DONE   taskType = 2
	WAIT   taskType = 3
)

type task struct {
	T   taskType
	Id  int
	St  state
	Wid string
	F   []string
	bt  int64
}

var DONE_TASK = task{DONE, 0, FINISH, string(0), nil, 0}
var WAIT_TASK = task{WAIT, 0, FINISH, string(0), nil, 0}

type Master struct {
	mut sync.Mutex
	// Your definitions here.
	mapTask    []task
	reduceTask []task
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) PrintInfo() {
	for _, t := range m.mapTask {
		log.Printf("Map task %v is %v", t.Id, t.St)
	}
	for _, t := range m.reduceTask {
		log.Printf("Reduce task %v is %v", t.Id, t.St)
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Claim(args *ClaimArgs, reply *ClaimReply) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	//defer m.PrintInfo()
	mapDone := true
	reply.NReduce = len(m.reduceTask)
	now := time.Now().Unix()
	for i, t := range m.mapTask {
		if t.St == IDLE {
			log.Printf("assign map task %v to worker %v\n", t, args.Wid)
			mapDone = false
			m.mapTask[i].Wid = args.Wid
			m.mapTask[i].St = RUNNING
			m.mapTask[i].bt = now
			reply.T = m.mapTask[i]
			return nil
		} else if t.St == RUNNING {
			mapDone = false
			if now-t.bt > 10 {
				m.mapTask[i].Wid = args.Wid
				m.mapTask[i].bt = now
				reply.T = m.mapTask[i]
				return nil
			}
		}
	}
	reduceDone := true
	if mapDone {
		for i, t := range m.reduceTask {
			if t.St == IDLE {
				log.Printf("assign reduce task %v to worker %v\n", t, args.Wid)
				reduceDone = false
				m.reduceTask[i].St = RUNNING
				m.reduceTask[i].Wid = args.Wid
				m.reduceTask[i].bt = now
				reply.T = m.reduceTask[i]
				return nil
			} else if t.St == RUNNING {
				reduceDone = false
				if now-t.bt > 10 {
					m.reduceTask[i].Wid = args.Wid
					m.reduceTask[i].bt = now
					reply.T = m.reduceTask[i]
					return nil
				}
			}
		}
		if reduceDone {
			reply.T = DONE_TASK
			return nil
		}
	}
	reply.T = WAIT_TASK
	return nil
}

func (m *Master) Finish(args *FinishArgs, reply *FinishReply) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	tid := args.T.Id
	ttype := args.T.T
	now := time.Now().Unix()
	if ttype == MAP {
		if tid >= 0 && tid < len(m.mapTask) && m.mapTask[tid].Wid == args.T.Wid && m.mapTask[tid].St == RUNNING && now-m.mapTask[tid].bt <= 10 {
			log.Printf("finish map task %v by worker %v\n", tid, args.T.Wid)
			reply.F = true
			m.mapTask[tid].St = FINISH
			return nil
		} else {
			reply.F = false
			return nil
		}
	}
	if ttype == REDUCE {
		if tid >= 0 && tid < len(m.reduceTask) && m.reduceTask[tid].Wid == args.T.Wid && m.reduceTask[tid].St == RUNNING && now-m.reduceTask[tid].bt <= 10 {
			log.Printf("finish reduce task %v by worker %v\n", tid, args.T.Wid)
			reply.F = true
			m.reduceTask[tid].St = FINISH
			return nil
		} else {
			reply.F = false
			return nil
		}
	}
	reply.F = false
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	// Your code here.
	for _, t := range m.mapTask {
		if t.St != FINISH {
			return false
		}
	}

	for _, t := range m.reduceTask {
		if t.St != FINISH {
			return false
		}
	}

	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	for i, file := range files {
		m.mapTask = append(m.mapTask, task{MAP, i, IDLE, "", []string{file}, 0})
	}

	nMap := len(files)

	for i := 0; i < nReduce; i++ {
		var fs []string
		for j := 0; j < nMap; j++ {
			kvName := fmt.Sprintf("mr-%v-%v", j, i)
			fs = append(fs, kvName)
		}
		m.reduceTask = append(m.reduceTask, task{REDUCE, i, IDLE, "", fs, 0})
	}

	m.server()
	return &m
}
