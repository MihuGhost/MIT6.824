package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "sort"
import "strings"
import "time"

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

func Worker(mapf func(string, string) []string, reducef func([]string) string) {
	for{
		task := getTask()
		switch task.TaskState {
		case Map:
			doMapf(mapf)
		case Reduce:
			doReducef(reducef)
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

//获得文件名
func ReqFileName() ([]string) {
	req := Req{}
	resp := Resp{}

	ok := call("Coordinator.ProvideFileName", &req, &resp)
	if ok {
		return resp.Args
	}else{
		log.Fatal("ReqFileName error")
	}
	return nil
}

func doMapf(mapf func(string, string) []string) bool{
	filenames := ReqFileName()
	intermediate := []string{}
	
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		strSlice := mapf(filename, string(content))
		intermediate = append(intermediate, strSlice...)
	}

	sort.Strings(intermediate)
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	
	for _, v := range intermediate {
		_,err := ofile.WriteString(v+"\n")
		if err != nil {
			fmt.Println("Error writing to file:", err)
            return false
		}
	}
	fmt.Println("DoMapf successfully.")
	return true
}

//
func doReducef(reducef func([]string) string) bool{
	file, err := os.Open("mr-out-0")
	if err != nil {
		log.Fatalf("cannot open %v", "mr-out-0")
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", "mr-out-0")
	}
	file.Close()

	contentStr := string(content)
	words := strings.Fields(contentStr)

	oname := "mr-out-1"
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	i := 0
	for i < len(words){
		j := i+1
		for j< len(words) && words[j] == words[i]{
			j++
		}
		output := reducef(words[i:j])
		fmt.Fprintf(ofile,"%v %v\n", words[i],output)
		i=j
	}
	fmt.Println("do Reducef successfully.")
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
