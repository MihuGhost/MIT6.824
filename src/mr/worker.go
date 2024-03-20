package mr

//go run -race mrworker.go wc.so

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	intermediate := []KeyValue{}
	filenames := ReqFileName()

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

		kva := mapf(filename, string(content))

		intermediate = append(intermediate, kva...)
	}

	sort.Sort(ByKey(intermediate))
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	
	for _, v := range intermediate {
		_,err := ofile.WriteString(v.Key+" "+v.Value+"\n")
		if err != nil {
			fmt.Println("Error writing to file:", err)
            return
		}
	}

	fmt.Println("Data written successfully.")
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
