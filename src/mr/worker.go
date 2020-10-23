package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
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

type worker struct {
	workerID int
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	// 首先注册worker，得到一个注册ID，然后使用
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.registerWorker()
	w.runWorker()
}


func (w *worker) registerWorker() {
	registerReq := &RegisterReq{}
	registerReply := &RegisterReply{}
	ok := call("Master.RegisterWorker", &registerReq, &registerReply)
	if !ok {
		fmt.Printf("Register Error!\n")
		os.Exit(1)
	} else {
		w.workerID = registerReply.WorkerID
		fmt.Printf("Successful get register reply, workerID is %v\n", registerReply.WorkerID)
	}
}


func (w *worker) runWorker() {
	for {
		task := w.getTask()
		if !task.Alive {
			fmt.Printf("worker get task not alive\n")
			return
		}
		w.runTask(task)
	}
}


func (w *worker) getTask() Task {
	getTaskReq := GetTaskReq{}
	getTaskReq.WorkerID = w.workerID
	getTaskReply := GetTaskReply{}
	ok := call("Master.GetTask", &getTaskReq, &getTaskReply)
	if !ok {
		fmt.Printf("Get task Error!\n")
		os.Exit(1)
	} else {
		fmt.Printf("Successful get a task %+v\n", getTaskReply.Task)
	}
	return *getTaskReply.Task
}




func (w *worker) finishTask(t Task) {
	finishTaskReq := FinishTaskReq{}
	finishTaskReply := FinishTaskReply{}
	finishTaskReq.TaskID = t.TaskID
	finishTaskReq.WorkerID = t.WorkerID
	finishTaskReq.TaskType = t.TaskType
	finishTaskReq.TaskStatus = t.TaskStatus
	ok := call("Master.FinishTask", &finishTaskReq, &finishTaskReply)
	if !ok {
		fmt.Printf("Finish task Error!\n")
	}
}


func (w *worker) runTask(t Task) {
	switch t.TaskType {
	case TaskMap:
		w.runMapTask(t)
	case TaskReduce:
		w.runReduceTask(t)
	}
}


func (w *worker) runMapTask(t Task)  {
	fmt.Printf("Start to run map task, filename is %v\n", t.FileName)
	file, err := os.Open(t.FileName)
	if err != nil {
		fmt.Printf("Cannot open %v", t.FileName)
		t.TaskStatus = TaskError
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("Cannot read %v", t.FileName)
		t.TaskStatus = TaskError
	}
	file.Close()
	//通过map函数将文件读取为键值对
	kva := w.mapf(t.FileName, string(content))
	//将读取出来的键值对按照NReduce数量分散存储
	reduces := make([][]KeyValue, t.NReduce)
	for _,kv := range kva {
		index := ihash(kv.Key) % t.NReduce
		reduces[index] = append(reduces[index], kv)
	}
	for index, kvs := range reduces {
		//将中间文件存储成mr-X-Y的形式，X是map任务ID，Y是reduce任务ID，由于必须map全部执行完之后才能执行reduce，所以不冲突
		filename := fmt.Sprintf("mr-%d-%d", t.TaskID, index)
		f, err := os.Create(filename)
		if err != nil {
			fmt.Printf("Cannot create file %v to save reduce task", filename)
			t.TaskStatus = TaskError
		}
		enc := json.NewEncoder(f)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Printf("Cannot encode kv to file error is%v", err)
				t.TaskStatus = TaskError
			}
		}
		f.Close()
	}
	t.TaskStatus = TaskFinished
	fmt.Println("Map task done")
	w.finishTask(t)
}


func (w *worker) runReduceTask(t Task) {
	fmt.Printf("Start to run reduce task, task id is %v\n", t.TaskID)
	//将存储在map生成的临时文件中的内容读出来
	maps := make(map[string][]string)
	for idx := 0; idx < t.NMaps; idx++ {
		fileNames := fmt.Sprintf("mr-%d-%d", idx, t.TaskID)
		f, err := os.Open(fileNames)
		if err != nil {
			fmt.Printf("Cannot open %v", fileNames)
			t.TaskStatus = TaskError
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}
	//res存储reduce返回即将输出的字符串
	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}
	outputFile := fmt.Sprintf("mr-out-%d", t.TaskID)
	err := ioutil.WriteFile(outputFile, []byte(strings.Join(res, "")), 0600)
	if err != nil {
		fmt.Println("Write output file error when running reduce task")
		t.TaskStatus = TaskError
	}
	t.TaskStatus = TaskFinished
	fmt.Println("Reduce task done")
	w.finishTask(t)
}


//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleReq{}

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
