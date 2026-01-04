package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type PhaseState int

const (
	MapPhase PhaseState = iota
	ReducePhase
	Done
)

// Master struct tracks the global execution state, manages map/reduce tasks,
// assigns work to workers, and records progress in a thread-safe way.
type Master struct {
	// State indicates the current phase of the job
	// Ex.: MapPhase / ReducePhase / Done
	Phase PhaseState

	// NReduce is the number of reduce tasks
	NReduce int

	// MapTasks holds all map task descriptors
	MapTasks []*MapTask

	// ReduceTasks holds all reduce task descriptors
	ReduceTasks []*ReduceTask

	// MappedTaskId records which map task IDs have completed
	MappedTaskId map[int]struct{}

	// MaxTaskId is the highest task ID assigned so far
	MaxTaskId int

	////////////////////////////////////////////////////
	// Concurrency control
	////////////////////////////////////////////////////

	// Mutex protects all shared state in Master
	Mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	// Listen on a Unix-domain socket instead of TCP.
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	Printf("server() started listening on %s", sockname)

	// Start serving RPC requests in a new goroutine.
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := m.Phase == Done

	return ret
}

// MakeMaster creates a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
// This function creates a Master instance, initializes all Map tasks
// based on the input files, initializes all Reduce tasks based on nReduce,
// sets the Master state to the Map phase, starts the RPC server,
// and finally returns a pointer to the Master.
func MakeMaster(files []string, nReduce int) *Master {
	// Create and initialize a Master struct.
	m := Master{
		Phase:        MapPhase,               // Initial state is MapPhase
		NReduce:      nReduce,                // Number of Reduce tasks
		MaxTaskId:    0,                      // Highest task ID assigned so far
		MappedTaskId: make(map[int]struct{}), // Tracks completed Map task IDs
	}

	// Initialize Map tasks. One Map task per input file.
	for _, f := range files {
		Printf("MakeMaster() initialize a map task for file %s", f)
		m.MapTasks = append(
			m.MapTasks,
			&MapTask{
				TaskInfo: TaskInfo{State: Pending}, // Initial state is Pending
				Filename: f,
			},
		)
	}

	// Initialize Reduce tasks up to nReduce.
	for i := 0; i < nReduce; i++ {
		Printf("MakeMaster() initialize reduce task %d", i)
		m.ReduceTasks = append(
			m.ReduceTasks,
			&ReduceTask{
				TaskInfo: TaskInfo{
					State: Pending, // Reduce task starts as Pending
					Id:    i,       // Reduce task ID is i
				},
			},
		)
	}

	// Start the RPC server to listen for worker requests.
	m.server()
	return &m
}
