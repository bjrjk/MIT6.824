package mr

import (
	"container/list"
	"fmt"
	"io/ioutil"
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
	TaskTimeout = 10
)

type taskDistribution struct {
	task  Task
	since time.Time
}

type Master struct {
	// Your definitions here.
	nextTaskId            int
	mapTasksTodo          []Task
	reduceTasksTodo       []Task
	mapTasks              uint
	reduceTasks           uint
	maxReduceTasks        uint
	intermediateKeyValues map[string][]string
	distributed           map[int]*list.List
	lock                  sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func popTask(tasks []Task) (rest []Task, last Task) {
	if len(tasks) == 0 {
		panic("Trying to pop an empty slice.")
	}
	last = tasks[len(tasks)-1]
	rest = tasks[:len(tasks)-1]
	return
}

func popString(str []string) (rest []string, last string) {
	if len(str) == 0 {
		panic("Trying to pop an empty slice.")
	}
	last = str[len(str)-1]
	rest = str[:len(str)-1]
	return
}

func (m *Master) init() {
	m.nextTaskId = 1
	m.intermediateKeyValues = make(map[string][]string)
	m.distributed = make(map[int]*list.List)
}

func (m *Master) loadTaskFromInput(file string) {
	rawContent, err := ioutil.ReadFile(file)
	if err != nil {
		// Panic for labs. In real life, error handling mechanisms should kick in and handle this.
		panic(fmt.Sprintf("Read file \"%s\" failed: %v", file, err))
	}
	content := string(rawContent)
	task := Task{
		Id:   m.nextTaskId,
		Kind: TaskKindMap,
	}
	m.nextTaskId++
	task.SetMapFilename(file)
	task.SetMapInput(content)
	m.mapTasksTodo = append(m.mapTasksTodo, task)
	m.mapTasks++
}

func (m *Master) lockAndRun(fn func() error) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	return fn()
}

func (m *Master) fixExpired() {
	now := time.Now()
	freeWorkers := make([]int, 0)
	for workerId, lst := range m.distributed {
		if lst.Len() == 0 {
			freeWorkers = append(freeWorkers, workerId)
			continue
		}
		expiredElements := make([]*list.Element, 0)
		for el := lst.Front(); el != nil; el = el.Next() {
			d := el.Value.(taskDistribution)
			if now.Sub(d.since).Seconds() > TaskTimeout {
				expiredElements = append(expiredElements, el)
			}
		}
		for _, el := range expiredElements {
			d := el.Value.(taskDistribution)
			lst.Remove(el)
			switch d.task.Kind {
			case TaskKindMap:
				m.mapTasksTodo = append(m.mapTasksTodo, d.task)
				break
			case TaskKindReduce:
				m.reduceTasksTodo = append(m.reduceTasksTodo, d.task)
				break
			default:
				panic(fmt.Sprintf("Unrecognized kind: %d", d.task.Kind))
			}
		}
	}
}

func (m *Master) hasMoreTasks() bool {
	return m.mapTasks != 0 || m.reduceTasks != 0
}

func (m *Master) distributeTask(workerId int, task Task) {
	var lst *list.List
	var ok bool
	if lst, ok = m.distributed[workerId]; !ok {
		lst = list.New()
		m.distributed[workerId] = lst
	}
	lst.PushBack(taskDistribution{
		task:  task,
		since: time.Now(),
	})
}

func (m *Master) FetchTask(workerId int, reply *GetTaskReply) error {
	log.Printf("FetchTask called from worker #%d", workerId)
	return m.lockAndRun(func() error {
		if !m.hasMoreTasks() {
			reply.Status = JobStatusNoTasks
			return nil
		}

		// Try to distribute a map task to the worker.

		m.fixExpired()

		if len(m.mapTasksTodo) != 0 {
			m.mapTasksTodo, reply.Task = popTask(m.mapTasksTodo)
			reply.Status = JobStatusFreeTasks
			m.distributeTask(workerId, reply.Task)
			return nil
		}

		// No map task available.
		// If there are map tasks that do not finish yet, tell the worker to wait for them to finish.
		if m.mapTasks != 0 {
			reply.Status = JobStatusWait
			return nil
		}

		// Try to distribute a reduce task to the worker.

		if len(m.reduceTasksTodo) != 0 {
			m.reduceTasksTodo, reply.Task = popTask(m.reduceTasksTodo)
			reply.Status = JobStatusFreeTasks
			m.distributeTask(workerId, reply.Task)
			return nil
		}

		// Tell the worker to wait for all reduce tasks to finish.
		reply.Status = JobStatusWait
		return nil
	})
}

func (m *Master) submitMapResult(keys []string, values []string) {
	if len(keys) != len(values) {
		panic("Length of Keys does not equal to length of Values.")
	}
	for i := 0; i < len(keys); i++ {
		k, v := keys[i], values[i]
		if _, exist := m.intermediateKeyValues[k]; exist {
			m.intermediateKeyValues[k] = append(m.intermediateKeyValues[k], v)
		} else {
			m.intermediateKeyValues[k] = []string{v}
		}
	}
}

func (m *Master) buildReduceTasks() {
	keys := make([]string, 0, len(m.intermediateKeyValues))
	values := make([][]string, 0, len(m.intermediateKeyValues))
	for k, v := range m.intermediateKeyValues {
		keys = append(keys, k)
		values = append(values, v)
	}

	tasksCount := m.maxReduceTasks
	if tasksCount > uint(len(keys)) {
		tasksCount = uint(len(keys))
	}

	m.nextTaskId = 1
	for tasksCount > 0 {
		keysCount := uint(len(keys)) / tasksCount
		task := Task{
			Id:   m.nextTaskId,
			Kind: TaskKindReduce,
		}
		m.nextTaskId++
		task.SetReduceInput(keys[:keysCount], values[:keysCount])
		m.reduceTasksTodo = append(m.reduceTasksTodo, task)
		m.reduceTasks++
		keys = keys[keysCount:]
		values = values[keysCount:]
		tasksCount--
	}
}

func (m *Master) SubmitTask(result *SubmitTaskResultArgs, _ *SubmitTaskResultReply) error {
	log.Printf("SubmitTask called from worker #%d", result.WorkerId)
	return m.lockAndRun(func() error {
		var lst *list.List
		var exist bool
		if lst, exist = m.distributed[result.WorkerId]; !exist {
			return nil
		}
		for el := lst.Front(); el != nil; el = el.Next() {
			d := el.Value.(taskDistribution)
			if d.task.Id != result.Result.TaskId {
				continue
			}
			keys, values := result.Result.Keys, result.Result.Values
			switch d.task.Kind {
			case TaskKindMap:
				m.submitMapResult(keys, values)
				m.mapTasks--
				if m.mapTasks == 0 {
					// The last map task has finished.
					// Build all reduce tasks in memory.
					m.buildReduceTasks()
				}
				break
			case TaskKindReduce:
				m.reduceTasks--
				break
			default:
				panic(fmt.Sprintf("Unknown task kind: %d", d.task.Kind))
			}

			lst.Remove(el)
			if lst.Len() == 0 {
				delete(m.distributed, result.WorkerId)
			}

			break
		}
		return nil
	})
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false

	// Your code here.
	_ = m.lockAndRun(func() error {
		ret = !m.hasMoreTasks()
		return nil
	})

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.init()
	m.maxReduceTasks = uint(nReduce)
	if m.maxReduceTasks == 0 {
		m.maxReduceTasks = math.MaxUint32
	}

	for _, file := range files {
		log.Printf("Loading map task from input file \"%s\"...", file)
		m.loadTaskFromInput(file)
	}

	m.server()
	return &m
}
