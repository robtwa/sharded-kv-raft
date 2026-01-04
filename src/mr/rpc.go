package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"time"
)
import "strconv"

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

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

////////////////////////////////////////////////////////////////////////////////
// Core RPC handlers
////////////////////////////////////////////////////////////////////////////////

type Void struct{}

type CompleteArgs struct {
	IsMap bool
	Id    int
}

type TaskState int

const (
	Pending TaskState = iota
	Running
	Completed
)

type TaskInfo struct {
	State     TaskState
	StartTime time.Time
	Id        int
}

type MapTask struct {
	TaskInfo
	Filename string
}

type ReduceTask struct {
	TaskInfo
	IFiles []string
}

type TaskAction int

const (
	ToWait TaskAction = iota
	ToRun
)

type Task struct {
	IsMap   bool
	Action  TaskAction
	NReduce int
	Map     MapTask
	Reduce  ReduceTask
}

const TIMEOUT = 10 * time.Second

// GetTask is an RPC handler that assigns tasks to workers.
func (m *Master) GetTask(_ *Void, reply *Task) error {
	reply.Action = ToWait // Default to ToWait

	// Assign tasks based on the current phase.
	if m.Phase == MapPhase { // Map phase
		// Iterate over Map tasks to find a pending one.
		for _, task := range m.MapTasks {
			now := time.Now()
			m.Mutex.Lock() // Lock the mutex to safely access task state.
			// Reassign task if it has timed out.
			if task.State == Running && task.StartTime.Add(TIMEOUT).Before(now) {
				task.State = Pending
			}
			// Assign the task to the worker if it's pending.
			if task.State == Pending {
				// Update task id, state, and start time.
				m.MaxTaskId++
				task.Id = m.MaxTaskId
				task.State = Running
				task.StartTime = now

				// Release the mutex after updating the task.
				m.Mutex.Unlock()

				// Prepare the reply with task details.
				reply.IsMap = true
				reply.Map = *task
				reply.Action = ToRun
				reply.NReduce = m.NReduce

				return nil
			}
			m.Mutex.Unlock()
		}
	} else if m.Phase == ReducePhase { // Reduce phase
		// Iterate over Reduce tasks to find a pending one.
		for _, task := range m.ReduceTasks {
			now := time.Now()
			m.Mutex.Lock() // Lock the mutex to safely access task state.

			// Reassign task if it has timed out.
			if task.State == Running && task.StartTime.Add(TIMEOUT).Before(now) {
				task.State = Pending
			}

			// Assign the task to the worker if it's pending.
			if task.State == Pending {
				// Update task state, start time, and intermediate filenames.
				task.State = Running
				task.StartTime = now

				// Iterate over completed map task ids to generate filenames
				// id is the map task id
				// task.Id is the reduce task id
				task.IFiles = nil
				for id := range m.MappedTaskId {
					fn := fmt.Sprintf("mr-%d-%d", id, task.Id)
					task.IFiles = append(task.IFiles, fn)
				}

				// Release the mutex after updating the task.
				m.Mutex.Unlock()

				// Prepare the reply with task details.
				reply.Action = ToRun
				reply.IsMap = false
				reply.NReduce = m.NReduce
				reply.Reduce = *task
				return nil
			}
			m.Mutex.Unlock()
		}
	}
	return nil
}

// Complete is an RPC handler that marks tasks as finished.
func (m *Master) Complete(args *CompleteArgs, _ *Void) error {
	// Update task state based on whether it's a Map or Reduce task.
	if args.IsMap {
		// If the received task is a Map task:
		// Iterate over Map tasks to find the completed one.
		for _, task := range m.MapTasks {
			if task.Id == args.Id {
				task.State = Completed // Mark task as Completed if found
				// Add finished map taks id to the set of mapped task ids
				m.MappedTaskId[task.Id] = struct{}{}
				break
			}
		}
		// Check if all Map tasks are finished.
		for _, t := range m.MapTasks {
			if t.State != Completed {
				return nil
			}
		}

		// Transition to Reduce phase if all Map tasks are done.
		m.Phase = ReducePhase
	} else {
		// If the received task is a Reduce task:
		// Iterate over Reduce tasks to find the completed one.
		for _, task := range m.ReduceTasks {
			if task.Id == args.Id {
				task.State = Completed // Mark task as Finished if found
				break
			}
		}
		// Check if all Reduce tasks are finished.
		for _, t := range m.ReduceTasks {
			if t.State != Completed {
				return nil
			}
		}

		// Transition to Done state if all Reduce tasks are done.
		m.Phase = Done
	}
	return nil
}
