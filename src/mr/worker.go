package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "strconv"
import "path/filepath"
// import "sort"
// import "strings"


type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for{
		task := getTask()
		switch task.TaskState {
		case Map:
			doMapf(mapf, &task)
		case Reduce:
			doReducef(reducef, &task)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			return
		}
	}
}

//获取任务
func getTask() Task{
	req := Req{}
	resp := Task{}
	call("Coordinator.AssignTask",&req,&resp)
	return resp
}

func doMapf(mapf func(string, string) []KeyValue,task *Task){
	intermediates := []KeyValue{}
	file, err := os.Open(task.InputFile)
	//map任务
	if err != nil {
		log.Fatalf("cannot open %v", file)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", file)
	}
	file.Close()
	kva := mapf(task.InputFile, string(content))
	intermediates = append(intermediates, kva...)
	
	//写入本地并将地址发送给coordinator
	task.IntermediateFiles = writeToLocal(intermediates, task.TaskNumber, task.NReduce)
	//map任务结束通知master
	taskCompleted(task)
}

//
func doReducef(reducef func(string, []string) string,task *Task) bool{

	return true
}

//任务结束通知Master
func taskCompleted(task *Task){
	resp := Resp{}
	call("Coordinator.TaskCompleted",task,&resp)
}

//分为nReduce份写入本地,返回地址
func writeToLocal(intermediates []KeyValue,taskNumber,nReduce int) []string{
	buffers := make([][]KeyValue,nReduce)
	for _, intermediate := range intermediates {
		salt := ihash(intermediate.Key) % nReduce
		buffers[salt] = append(buffers[salt],intermediate)
	}

	locations := make([]string, nReduce)
	for i, buffer := range buffers {
		dir, _ := os.Getwd()
		file, err := os.Create("mr-"+strconv.Itoa(taskNumber)+"-"+strconv.Itoa(i))
		if err != nil{
			log.Fatal("writeToLocal[os.Create]:", err)
		}
		for _, subBuffer := range buffer {
			fmt.Fprintf(file, "%v %v\n", subBuffer.Key, subBuffer.Value)
		}
		file.Close()
		locations = append(locations,filepath.Join(dir,file.Name()))
	}

	return locations
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return true
}
