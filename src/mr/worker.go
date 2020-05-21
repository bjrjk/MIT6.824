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
		log.Println("Getting task from master...")
		status, task, ok := callGetTask()
		if !ok {
			log.Printf("Failed to call GetTask. Retry in %f seconds.", WaitDuration.Seconds())
			time.Sleep(WaitDuration)
			continue
		}
		if status == JobStatusFreeTasks {
			log.Println("Got a free task to run.")
			runTask(task, env)
		} else if status == JobStatusWait {
			log.Println("Master tell me to wait.")
			time.Sleep(WaitDuration)
		} else if status == JobStatusNoTasks {
			log.Println("No more tasks to run.")
			break
		} else {
			log.Printf("Unrecognized status code: %d. Retry in %f seconds.", status, WaitDuration.Seconds())
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
		log.Printf("Map task received. File name is \"%s\".", task.GetMapFilename())
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
		log.Printf("Reduce task received. Number of Keys is %d.", len(keys))
		resValues := make([]string, 0, len(values))
		for i := range keys {
			k, v := keys[i], values[i]
			resValues = append(resValues, env.reducef(k, v))
		}
		res.Keys, res.Values = keys, resValues
		saveReduceTaskResult(&res)
		// It's wasteful to transport the result of the reduce work to the master.
		// So clear them.
		res.Keys = nil
		res.Values = nil
		break

	default:
		// Unrecognized task kind.
		log.Printf("Unrecognized task kind: %d", task.Kind)
		return
	}

	log.Println("Task finished. Submitting task result to master...")

	// Upload task result to the master node.
	callSubmitTaskResult(workerId, res)
}

func saveReduceTaskResult(result *TaskResult) {
	keys, values := result.Keys, result.Values
	if len(keys) != len(values) {
		panic("Length of Keys does not equal to length of Values.")
	}
	file := fmt.Sprintf("mr-out-%d", result.TaskId)
	fp, err := os.Create(file)
	if err != nil {
		panic(fmt.Sprintf("Failed to create output file \"%s\": %v", file, err))
	}
	for i := range keys {
		k, v := keys[i], values[i]
		_, _ = fmt.Fprintf(fp, "%s %s\n", k, v)
	}
}

func callGetTask() (int, Task, bool) {
	var reply GetTaskReply
	if !call("Master.FetchTask", workerId, &reply) {
		log.Printf("Failed to call \"Master.FetchTask\".")
		return 0, Task{}, false
	}
	return reply.Status, reply.Task, true
}

func callSubmitTaskResult(workerId int, result TaskResult) bool {
	args := SubmitTaskResultArgs{
		WorkerId: workerId,
		Result:   result,
	}
	reply := SubmitTaskResultReply{}
	if !call("Master.SubmitTask", &args, &reply) {
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
