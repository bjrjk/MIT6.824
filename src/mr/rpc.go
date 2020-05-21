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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
const (
	TaskKindMap    = 0
	TaskKindReduce = 1
)

type Task struct {
	Id       int
	Kind     int
	filename string
	keys     []string
	values   [][]string
}

func (t *Task) GetMapInput() string {
	if t.Kind != TaskKindMap {
		panic("Task is not a map task.")
	}
	return t.keys[0]
}

func (t *Task) GetMapFilename() string {
	if t.Kind != TaskKindMap {
		panic("Task is not a map task.")
	}
	return t.filename
}

func (t *Task) SetMapFilename(filename string) {
	t.filename = filename
}

func (t *Task) SetMapInput(value string) {
	t.Kind = TaskKindMap
	t.keys[0] = value
}

func (t *Task) GetReduceInput() ([]string, [][]string) {
	if t.Kind != TaskKindReduce {
		panic("Task is not a reduce task.")
	}
	return t.keys, t.values
}

func (t *Task) SetReduceInput(keys []string, values [][]string) {
	t.Kind = TaskKindReduce
	t.keys = keys
	t.values = values
}

type GetTaskReply struct {
	Status int
	Task   Task
}

type TaskResult struct {
	TaskId int
	Keys   []string
	Values []string
}

const (
	JobStatusFreeTasks = 0
	JobStatusWait      = 1
	JobStatusNoTasks   = 2
)

type SubmitTaskResultArgs struct {
	WorkerId int
	Result   TaskResult
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
