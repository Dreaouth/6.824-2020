package mr

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)


//必须所有的map操作做完后才能进行reduce
type Master struct {
	// Your definitions here.
	files []string
	nReduce int
	taskStats []Task
	tasksType TaskType
	workerStats map[int]list.List
	workerSeq int
	taskCh chan Task
	done bool
	mutex sync.Mutex
}


func (m *Master) initMapTask() {
	m.tasksType = TaskMap
	m.taskStats = make([]Task, len(m.files))
	fmt.Println("Init map task")
}


func (m *Master) initReduceTask() {
	m.tasksType = TaskReduce
	m.taskStats = make([]Task, m.nReduce)
	fmt.Println("Init reduce task")
}


func (m *Master) initTask(index int) Task {
	task := Task{
		TaskID: index,
		FileName: "",
		TaskStatus: TaskReady,
		TaskType: m.tasksType,
		NMaps: len(m.files),
		NReduce: m.nReduce,
		Alive: true,
	}
	fmt.Printf("Inited task taskID:%d, lenfiles:%d, lents:%d, taskType:%d\n", index, len(m.files), len(m.taskStats), task.TaskType)
	if m.tasksType == TaskMap {
		task.FileName = m.files[index]
	}
	m.taskStats[index] = task
	return task
}


func (m *Master) checkAndInitTasks(finished bool) bool {
	for i,t := range m.taskStats {
		switch t.TaskStatus {
		case TaskInit:
			//将初始化完成的
			m.taskCh <- m.initTask(i)
			finished = false
		case TaskReady:
			finished = false
		case TaskRunning:
			//为了提高容错性，可以在task中设置时间属性，当task中的map/reduce函数错误导致异常退出后，通过判断task运行时间来进行恢复操作
			finished = false
		case TaskFinished:
		case TaskError:
			finished = false
			fmt.Println("error task")
			m.taskStats[i].TaskStatus = TaskReady
			m.taskCh <- m.taskStats[i]
		default:
			panic("Task status error")
		}
	}
	return finished
}


//后台调度函数，判断每一个task的状态
func (m *Master) schedule() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.done {
		return
	}
	finished := true
	finished = m.checkAndInitTasks(finished)
	if finished {
		if m.tasksType == TaskMap {
			m.initReduceTask()
			m.checkAndInitTasks(finished)
		} else {
			m.done = true
		}
	}
}


// Your code here -- RPC handlers for the worker to call.
func (m *Master) RegisterWorker(req *RegisterReq, reply *RegisterReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.workerSeq += 1
	reply.WorkerID = m.workerSeq
	fmt.Printf("Get a register worker request, reply worker id is %v\n", reply.WorkerID)
	return nil
}


func (m *Master) GetTask(req *GetTaskReq, reply *GetTaskReply) error {
	// 通过 goroutine 中的通道获取task，这里不需要加锁，因为通道相当于队列。加锁的话会和 Done() 函数构成死锁
	// 所以要减小锁的粒度
	task := <-m.taskCh
	reply.Task = &task
	if task.Alive {
		m.mutex.Lock()
		defer m.mutex.Unlock()
		task.WorkerID = req.WorkerID
		task.TaskStatus = TaskRunning
		m.taskStats[task.TaskID] = task
	}
	fmt.Printf("Distribute a task %+v\n", reply.Task)
	return nil

}


func (m* Master) FinishTask(req *FinishTaskReq, reply *FinishTaskReply) error {
	fmt.Printf("get a finished task %+v, type:%+v \n", req, req.TaskType)
	if m.tasksType != req.TaskType || req.WorkerID != m.taskStats[req.TaskID].WorkerID {
		fmt.Println("Finish task error, its shouldn't happen")
		return nil
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.taskStats[req.TaskID].TaskStatus = req.TaskStatus
	go m.schedule()
	return nil
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleReq, reply *ExampleReply) error {
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


// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.done
}



// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.done = false
	m.workerSeq = 0
	m.files = files
	m.nReduce = nReduce
	m.mutex = sync.Mutex{}
	if nReduce > len(files) {
		m.taskCh = make(chan Task, nReduce)
	} else {
		m.taskCh = make(chan Task, len(m.files))
	}
	m.workerStats = make(map[int]list.List)
	m.initMapTask()
	go m.schedule()
	m.server()
	fmt.Printf("Master start success!\n")
	return &m
}
