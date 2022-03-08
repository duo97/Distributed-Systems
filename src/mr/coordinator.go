package mr

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	MaxRunTime = time.Second * 10
	//incase workers crashed and don't request
	ReScheduleInterval = time.Second * 5
)

//Phase can be  "mapPhase","reducePhase" and "done"
type Phase string

type Coordinator struct {
	//paper 3.2, tasks Stateand Worker
	tasks        []Task
	currentPhase Phase
	files        []string
	//NMap=len(files)
	NReduce int
	nWorker int
	NMap    int

	mu       sync.Mutex
	taskChan chan Task

	// Your definitions here.

}

type Task struct {
	Worker    int //WorkerId
	StartTime time.Time
	NReduce   int
	NMap      int
	//task states can be "Idle","queued","inprogress","finished"
	State     string
	Id        int
	TaskPhase string //"mapPhase" or "reducePhase"
	// Id        string
	Fn string
}

//to solve the race problem
//worker report and reducestart try to access the same task data

// func (c *Coordinator) CreateTask(taskidx int) Task{
// 	task:=Task{
// 		Worker    int //WorkerId
// 	    StartTime time.Time
// 	    NReduce  : c.NReduce
// 	    NMap     : len(c.files)
// 	//task states can be "Idle","queued","inprogress","finished"
// 		State     string
// 		Id        int
// 		TaskPhase string //"mapPhase" or "reducePhase"
// 		// Id        string
// 		Fn string
// 	}
// }

// Your code here -- RPC handlers for the Worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

//give the Worker an Id when joined.
//don't need to maintain Worker Statetho
func (c *Coordinator) WorkerJoin(args *JoinArgs, reply *JoinReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.WorkerId = c.nWorker
	c.nWorker += 1
	return nil

}

//Worker report the task is done or failed
func (c *Coordinator) WorkerReport(args *ReportArgs, reply *ReportReply) error {
	c.mu.Lock()
	// fmt.Println("got lock to report!")
	defer c.mu.Unlock()
	//check if the task is outdated
	if args.TaskPhase != string(c.currentPhase) || args.WorkerId != c.tasks[args.TaskId].Worker {
		fmt.Println("task is outdated")
		return nil
	}
	//check if task is finished or failed
	if args.Done {
		c.tasks[args.TaskId].State = "finished"
	} else {
		c.tasks[args.TaskId].State = "idle"
	}
	// fmt.Println(c.tasks)
	go c.scheduleTask()
	return nil

}

//Idle Worker send a request to get a task
func (c *Coordinator) updateTask(args *RequestArgs, Id int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tasks[Id].State = "inprogress"
	c.tasks[Id].Worker = args.WorkerId
	c.tasks[Id].StartTime = time.Now()
}
func (c *Coordinator) RequestTask(args *RequestArgs, reply *RequestReply) error {
	task := <-c.taskChan
	reply.Task = &task
	c.updateTask(args, task.Id)
	return nil

}

//
// start a thread that listens for RPCs from Worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := false

	// Your code here.
	if c.currentPhase == "done" {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.

//Scheduling tasks by sending tasks that are Idle or took too long to channel

func (c *Coordinator) scheduleTask() {
	c.mu.Lock()
	defer c.mu.Unlock()
	// fmt.Println("start scheduling!")
	if c.currentPhase == "done" {
		return
	}
	IsDone := true
	for index, task := range c.tasks {
		switch task.State {
		case "idle":
			IsDone = false
			// fmt.Println("sending idle task to channel", task)
			c.taskChan <- task
			c.tasks[index].State = "queued"
		case "queued":
			IsDone = false
		case "inprogress":
			IsDone = false
			runtime := time.Since(task.StartTime) //changed
			if runtime > MaxRunTime {
				// fmt.Println("sending non responsive task to channel", task)
				c.taskChan <- task
				c.tasks[index].State = "queued"
			}
		case "finished":
		default:
			fmt.Println("task State is", task.State)
			panic("task Stateis wrong, can not schedule")

		}
	}
	// fmt.Println("tasks rescheduled!", c.tasks)
	if IsDone {
		if c.currentPhase == "mapPhase" {
			// fmt.Println("tring to strat reduce state while the tasks are", c.tasks)
			c.startReducePhase()
		} else {
			c.currentPhase = "done"
		}
	}
}

//initialize the reduce tasks and give each an index
func (c *Coordinator) startReducePhase() {
	// c.mu.Lock()
	// fmt.Println("reduce phase starter got the lock!")
	// defer c.mu.Unlock()

	c.tasks = make([]Task, c.NReduce)
	for index, task := range c.tasks {
		c.tasks[index].Id = index
		c.tasks[index].TaskPhase = "reducePhase"
		c.tasks[index].NMap = c.NMap
		c.tasks[index].NReduce = c.NReduce
		c.tasks[index].State = "idle"
		task.Id = index
	}
	c.currentPhase = "reducePhase"
	// fmt.Println("Reduce tasks started!", c.tasks)
	// go c.Schedule()
}

func (c *Coordinator) Schedule() {
	// fmt.Println("insIde function schedule")
	for c.currentPhase != "done" {
		go c.scheduleTask()
		time.Sleep(ReScheduleInterval)
	}
}

func (c *Coordinator) startMapPhase() {
	//create a task for each file and give each an index
	c.tasks = make([]Task, c.NMap)
	for index, name := range c.files {
		c.tasks[index].Id = index
		c.tasks[index].TaskPhase = "mapPhase"
		c.tasks[index].State = "idle"
		c.tasks[index].Fn = name
		c.tasks[index].NReduce = c.NReduce
		c.tasks[index].NMap = c.NMap
	}
	c.currentPhase = "mapPhase"
}

func MakeCoordinator(files []string, NReduce int) *Coordinator {
	// Your code here.
	//initialize coordinator
	c := Coordinator{}
	c.NReduce = NReduce
	c.NMap = len(files)
	c.mu = sync.Mutex{}
	c.files = files
	// fmt.Println(c.files)

	bufferSize := int(math.Max(float64(NReduce), float64(len(files))))
	c.taskChan = make(chan Task, bufferSize)

	//start the map phase
	c.startMapPhase()
	// fmt.Println("starting scheduling")
	go c.Schedule()

	c.server()
	return &c
}
