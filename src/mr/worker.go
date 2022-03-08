package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

const (
	RequestInterval = time.Millisecond * 500
)

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
	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// fmt.Println("a worker started")

	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.join()
	w.start()
}

type worker struct {
	workerId int
	mapf     func(string, string) []KeyValue
	reducef  func(string, []string) string
}

func (w *worker) start() {

	for {
		t := w.getTask()
		w.work(t)
		time.Sleep(RequestInterval)

	}

}

//get worker Id from coordinator
func (w *worker) join() {
	args := &JoinArgs{}
	reply := &JoinReply{}
	ok := call("Coordinator.WorkerJoin", args, reply)
	if !ok {
		log.Fatal("worker can not join")
	}
	// fmt.Println("A worker joined successfully", reply.WorkerId)
	w.workerId = reply.WorkerId
}

func (w *worker) work(t Task) {
	switch t.TaskPhase {
	case "mapPhase":
		w.doMap(t)
	case "reducePhase":
		w.doReduce(t)
	default:
		panic("cannot read the phase of a task")

	}
}

func (w *worker) doReduce(t Task) {
	reduceinputs := make(map[string][]string)
	for mapi := 0; mapi < t.NMap; mapi++ {
		fileName := itmFn(mapi, t.Id)
		file, err := os.Open(fileName)
		if err != nil {
			w.report(false, t, err)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			//check if already in slice
			if _, ok := reduceinputs[kv.Key]; !ok {
				reduceinputs[kv.Key] = make([]string, 0, 1000)
			}
			reduceinputs[kv.Key] = append(reduceinputs[kv.Key], kv.Value)
		}

	}
	err := w.writeResults(reduceinputs, t)
	if err != nil {
		w.report(false, t, err)
	}
	w.report(true, t, nil)

}

func (w *worker) writeResults(inputs map[string][]string, t Task) error {
	res := make([]string, 0, 1000)
	for key, values := range inputs {
		res = append(res, fmt.Sprintf("%v %v\n", key, w.reducef(key, values)))
	}
	Fn := outputFn(t.Id)
	err := ioutil.WriteFile(Fn, []byte(strings.Join(res, "")), 0600)
	if err != nil {
		return err
	}
	return nil

}
func outputFn(Id int) string {
	return fmt.Sprintf("mr-out-%d", Id)

}

func (w *worker) doMap(t Task) {

	//read content
	// fmt.Println("started map task", w.workerId)

	// file, err := os.Open(t.Fn)
	// if err != nil {
	// 	log.Fatalf("cannot open %v", t.Fn)
	// }
	content, err := ioutil.ReadFile(t.Fn)
	if err != nil {
		log.Fatalf("cannot read %v", t.Fn)
	}
	// file.Close()

	//apply map functions to get keyvalue pairs
	kva := w.mapf(t.Fn, string(content))

	//create a slice for every reduce file, NReduce reduce files
	reducea := make([][]KeyValue, t.NReduce)

	//hash key append to slice
	for _, kv := range kva {
		index := ihash(kv.Key) % t.NReduce
		reducea[index] = append(reducea[index], kv)

	}

	//encode and write to file
	for index, kvs := range reducea {
		Fn := itmFn(t.Id, index)
		// fmt.Println("writing to ", Fn)
		file, err := os.Create(Fn)
		if err != nil {
			fmt.Println("create file failed")
			w.report(false, t, err)
			return
		}
		enc := json.NewEncoder(file)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Println("encode failed")
				w.report(false, t, err)
				return
			}
		}
		err = file.Close()
		if err != nil {
			fmt.Println("close file failed")
			w.report(false, t, err)
			return
		}
		// fmt.Println("wrote successfully", Fn)

	}

	w.report(true, t, nil)
	// fmt.Println("successfully reported finished task", t.Id, t.Fn)
}

//helper functions for file names

func itmFn(mapi int, reducei int) string {
	return fmt.Sprintf("mr-%d-%d", mapi, reducei)
}

//request task from coordinator
func (w *worker) getTask() Task {
	args := RequestArgs{}
	args.WorkerId = w.workerId
	reply := RequestReply{}

	if ok := call("Coordinator.RequestTask", &args, &reply); !ok {

		fmt.Println("request task failed", w.workerId)

		// os.Exit(1)
	}
	// fmt.Println("workergot task", w.workerId)
	return *reply.Task

}

//report task to coordinator
func (w *worker) report(done bool, task Task, err error) {
	args := ReportArgs{}
	reply := ReportReply{}
	args.Done = done
	args.TaskId = task.Id
	args.TaskPhase = task.TaskPhase
	args.WorkerId = w.workerId
	ok := call("Coordinator.WorkerReport", &args, &reply)
	if !ok {
		fmt.Println("call to report task failed", args)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.

// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
