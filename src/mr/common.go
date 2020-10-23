package mr

type TaskType int

const (
	TaskMap    TaskType = 0
	TaskReduce TaskType = 1
)

type TaskStatus int
const (
	//go中默认初始化是0
	TaskInit 	 TaskStatus = 0
	TaskReady 	 TaskStatus = 1
	TaskRunning  TaskStatus = 2
	TaskFinished TaskStatus = 3
	TaskError    TaskStatus = 4
)

// ！！！go中涉及到RPC调用时，变量一律开头字母大写，要不然无法传输！！！
type Task struct {
	WorkerID 	int
	TaskID 		int
	TaskType 	TaskType
	TaskStatus 	TaskStatus
	FileName 	string
	NMaps 		int
	NReduce 	int
	Alive 		bool
}


