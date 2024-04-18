package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
// import "os"
// import "io/ioutil"
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
	task := Task{}
	call("Coordinator.AssignTask",&req,&task)
	return task
}

func doMapf(mapf func(string, string) []KeyValue,task *Task) bool{

	return true
}

//
func doReducef(reducef func(string, []string) string,task *Task) bool{

	return true
}


func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
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
