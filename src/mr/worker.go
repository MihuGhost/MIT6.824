package mr

//go run -race mrworker.go wc.so

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "sort"
import "bufio"

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	mapStatus := doMapf(mapf)
	if mapStatus {
		
	}else{
		log.Fatal("doMapf error")
	}

}

//Map切分，输出为一个文件
func doMapf(mapf func(string, string) []KeyValue) bool{
	intermediate := []KeyValue{}
	filenames := ReqFileName()

	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return false
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
			return false
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
            return false
		}
	}
	fmt.Println("DoMapf successfully.")
	return true
}

//Reduce任务处理
func doReducef(reducef func(string, []string) string) bool{
	file,err := os.Open("mr-out-0")
	if err != nil{
		log.Fatal("doReducef error, file open error")
		return false
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)
	// for scanner.Scan() {
	// 	line := scanner.Text()
		
	// }

	if err := scanner.Err(); err != nil {
        fmt.Println("Error reading file:", err)
        return false
    }

	return true
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
