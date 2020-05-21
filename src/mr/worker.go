package mr

import (
	"fmt"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

type environ struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

var workerId int

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	workerId = (os.Getpid() | (time.Now().Nanosecond() << 16)) & 0x7fffffff
	log.Printf("Worker %d has started.\n", workerId)

	const WaitDuration = 5 * time.Second
	env := environ{
		mapf:    mapf,
		reducef: reducef,
	}

	for {
		status, task, ok := CallGetTask()
		if !ok {
			time.Sleep(WaitDuration)
			continue
		}
		if status == JobStatusFreeTasks {
			runTask(task, env)
		} else if status == JobStatusWait {
			time.Sleep(WaitDuration)
		} else if status == JobStatusNoTasks {
			break
		} else {
			time.Sleep(WaitDuration)
		}
	}
}

func runTask(task Task, env environ) {
	res := TaskResult{
		TaskId: task.Id,
	}

	switch task.Kind {
	case TaskKindMap:
		log.Printf("")
		keys := make([]string, 0)
		values := make([]string, 0)
		for _, kv := range env.mapf(task.GetMapFilename(), task.GetMapInput()) {
			keys = append(keys, kv.Key)
			values = append(values, kv.Value)
		}
		res.Keys, res.Values = keys, values
		break

	case TaskKindReduce:
		keys, values := task.GetReduceInput()
		resValues := make([]string, 0, len(values))
		for i := range keys {
			k, v := keys[i], values[i]
			resValues = append(resValues, env.reducef(k, v))
		}
		res.Keys, res.Values = keys, resValues
		break

	default:
		// Unrecognized task kind.
		log.Printf("Unrecognized task kind: %d", task.Kind)
		return
	}

	// Upload task result to the master node.
	CallSubmitTaskResult(workerId, res)
}

func CallGetTask() (int, Task, bool) {
	var reply GetTaskReply
	if !call("Master.FetchTask", workerId, &reply) {
		log.Printf("Failed to call \"Master.FetchTask\".")
		return 0, Task{}, false
	}
	return reply.Status, reply.Task, true
}

func CallSubmitTaskResult(workerId int, result TaskResult) bool {
	args := SubmitTaskResultArgs{
		WorkerId: workerId,
		Result:   result,
	}
	if !call("Master.SubmitTask", &args, nil) {
		log.Printf("Failed to call \"Master.SubmitTask\".")
		return false
	}
	return true
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
