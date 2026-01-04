package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	return false
}

////////////////////////////////////////////////////////////////////////////////
// Core worker logic
////////////////////////////////////////////////////////////////////////////////

// MapF is a user-defined Map function type.
type MapF func(string, string) []KeyValue

// ReduceF is a user-defined Reduce function type.
type ReduceF func(string, []string) string

// Worker runs the main loop for a MapReduce worker.
// main/mrworker.go calls this function.
func Worker(mapf MapF, reducef ReduceF) {
	// Main worker loop: repeatedly ask the Master for work.
	for {
		// Sleep briefly to avoid busy-waiting.
		time.Sleep(1 * time.Second)

		// Prepare a Task struct to receive the assigned task.
		task := Task{}

		// Call the GetTask RPC on the Master to request a task.
		call("Master.GetTask", &Void{}, &task)

		// Case 1: No task assigned; worker should wait.
		if task.Action == ToWait {
			continue
		}

		// If reached here, a task has been assigned.
		if task.IsMap {
			// Case 2: work is a Map task.
			err := handleMap(task, mapf)
			if err != nil {
				log.Fatalf(err.Error())
				return
			}
		} else {
			// Case 3: work is a Reduce task.
			err := handleReduce(task, reducef)
			if err != nil {
				log.Fatalf(err.Error())
				return
			}
		}
	}
}

// handleMap executes a single Map task assigned by the Master.
func handleMap(task Task, mapf MapF) error {
	filename := task.Map.Filename
	content, err := os.ReadFile(filename) // Read the entire file content.
	if err != nil {
		log.Fatalf("handleMap cannot read %v", filename)
	}

	// Apply the user-defined Map function to the file content.
	kva := mapf(filename, string(content))

	// Prepare JSON encoders for each Reduce partition.
	encoders := make([]*json.Encoder, task.NReduce)
	for reduK := 0; reduK < task.NReduce; reduK++ {
		// Create an intermediate file for (MapId, ReduceId).
		fn := fmt.Sprintf("mr-%d-%d", task.Map.Id, reduK)
		file, err := os.Create(fn)
		if err != nil {
			log.Fatalf("Worker: 2.1 handleMap cannot create" +
				" intermediate result file")
		}
		// Associate a JSON encoder with this file.
		encoders[reduK] = json.NewEncoder(file)
	}

	// Iterate over all key-value pairs.
	for _, kv := range kva {
		// Determine Reduce partition by hashing the key.
		bucketNum := ihash(kv.Key) % task.NReduce

		// Encode the key-value pair into the appropriate file.
		err = encoders[bucketNum].Encode(&kv)
		if err != nil {
			log.Fatalf("handleMap cannot encode key-value pair %v", kv)
		}
	}

	// Call the finish RPC to notify the Master of task completion.
	ok := call(
		"Master.Complete",
		&CompleteArgs{
			IsMap: true,
			Id:    task.Map.Id,
		},
		&Void{},
	)

	if !ok {
		log.Fatalf("handleMap failed to notify the master of task" +
			" completion")
	}

	return nil
}

// handleReduce executes a single Reduce task assigned by the Master.
func handleReduce(task Task, reducef ReduceF) error {
	var kva []KeyValue // Declare a slice to hold all key-value pairs.

	// Iterate over all intermediate files to read key-value pairs from this
	// Reduce task.
	for _, filename := range task.Reduce.IFiles {
		// Open the intermediate file.
		iFile, err := os.Open(filename)
		if err != nil {
			log.Fatalf("handleReduce cannot open %v", filename)
		}

		// Create a JSON decoder based on the opened file.
		decoder := json.NewDecoder(iFile)
		// Read all key-value pairs from the file one by one using the decoder.
		for {
			var kv KeyValue
			// Decode one key-value pair from the file.
			err := decoder.Decode(&kv)
			if err != nil {
				break
			}
			kva = append(kva, kv) // Append the decoded pair to the kva slice.
		}
		err = iFile.Close() // Close the intermediate file after reading.
		if err != nil {
			log.Fatalf("handleReduce cannot close %v", filename)
		}
	}

	// Sort the kva key-value pairs by key.
	sort.Sort(SortByKey(kva))

	rFileName := fmt.Sprintf("mr-out-%d", task.Reduce.Id)
	// Create a temporary file for the Reduce results.
	// Ex. mr-out-0-123456789.tmp
	// Why temp file?
	// For atomicity and fault tolerance.
	// 1. ensures that the output file is fully complete or not existent.
	// 2. ensure that there is no partial output if the worker crashes.
	temp, err := os.CreateTemp(".", rFileName)
	if err != nil {
		log.Fatalf("handleReduce cannot create temp file for %s",
			rFileName)
		return err
	}

	// Iterate over the sorted key-value pairs with sliding window approach to
	// process identical keys.
	// The window is defined by [left, right].
	left := 0
	for left < len(kva) {
		// ex. kva = [<k1,1>, <k1,1>, <k2,1>, ...]

		// Find the range of identical keys.
		// ex. kva = [<k1,1>, <k1,1>, <k2,1>, ...]
		// left = 0, right = 1
		right := left + 1
		for right < len(kva) && kva[left].Key == kva[right].Key {
			right++
		}

		// Collect all values for the same key.
		// ex. values = [ "1", "1" ]
		var values []string
		for k := left; k < right; k++ {
			values = append(values, kva[k].Value)
		}

		// Apply the user-defined reduce function.
		// ex. result = "2"
		result := reducef(kva[left].Key, values)

		// Fprintf formats a string and writes it to the temp file.
		fmt.Fprintf(temp, "%v %v\n", kva[left].Key, result)

		// Move to the next group of keys.
		left = right
	}

	// Rename the temporary file to the final output file.
	// Ex. rename mr-out-0-123456789.tmp -> mr-out-0
	err = os.Rename(temp.Name(), rFileName)
	if err != nil {
		return err
	}

	// Call the finish RPC to notify the Master of task completion.
	call(
		"Master.Complete",
		&CompleteArgs{
			IsMap: false,
			Id:    task.Reduce.Id,
		},
		&Void{},
	)
	return nil
}
