package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type kvs []KeyValue

// for sorting by key.
func (a kvs) Len() int           { return len(a) }
func (a kvs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a kvs) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	wid := fmt.Sprintf("w-%v", os.Getpid())
	// Your worker implementation here.
	for {
		t, nReduce, st := CallClaim(wid)
		if !st {
			// master is disconnected, job is finished
			break
		}
		if t.T == DONE {
			// job is finished
			break
		}
		if t.T == WAIT {
			// should wait for next try
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if t.T == MAP {
			// read input files, call mapF, and split kvs into R pieces
			var intermediate []kvs
			for i := 0; i < nReduce; i++ {
				intermediate = append(intermediate, []KeyValue{})
			}
			for _, filename := range t.F {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				for _, kv := range kva {
					r := ihash(kv.Key) % nReduce
					intermediate[r] = append(intermediate[r], kv)
				}
			}
			//save to tmp files
			var filenames []string
			for i, kva := range intermediate {
				filename := fmt.Sprintf("mr-%v-%v.tmp", t.Id, i)
				filenames = append(filenames, filename)
				file, _ := os.Create(filename)
				enc := json.NewEncoder(file)
				for _, kv := range kva {
					enc.Encode(&kv)
				}
				file.Close()
			}
			//todo: maybe should rename before set task to FINISH?
			if CallFinish(t) {
				for _, filename := range filenames {
					log.Printf("rename file %v", filename)
					os.Rename(filename, strings.TrimSuffix(filename, ".tmp"))
				}
			}
			continue
		}
		if t.T == REDUCE {
			//read files to intermediate
			var intermediate kvs
			for _, filename := range t.F {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(intermediate)

			oname := fmt.Sprintf("mr-out-%v.tmp", t.Id)
			ofile, _ := os.Create(oname)

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			ofile.Close()
			if CallFinish(t) {
				log.Printf("rename file %v", oname)
				os.Rename(oname, strings.TrimSuffix(oname, ".tmp"))
			}
			continue
		}

	}
}

func CallClaim(wid string) (task, int, bool) {
	args := ClaimArgs{wid}
	reply := ClaimReply{}
	st := call("Master.Claim", &args, &reply)
	log.Println("worker %v get task %v", wid, reply.T)
	if st {
		return reply.T, reply.NReduce, true
	} else {
		log.Fatalln("call claim failed")
		return task{}, 0, false
	}
}

func CallFinish(t task) bool {
	args := FinishArgs{t}
	reply := FinishReply{}
	st := call("Master.Finish", &args, &reply)
	if st {
		return reply.F
	}
	return false
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
