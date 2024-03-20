package mr

//go run -race mrcoordinator.go pg-*.txt
import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

//1.命令行传入参数传给worker
type Coordinator struct {
	// Your definitions here.
}

//获取文件名称
func (c *Coordinator) ProvideFileName(req *Req, resp *Resp) error{
		if len(os.Args) < 2 {
			fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
			os.Exit(1)
		}

		for _, filename := range os.Args[1:] {
			resp.Args = append(resp.Args,filename)
		}
		return nil
}

// Your code here -- RPC handlers for the worker to call.

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// ret := false
	ret := true
	// Your code here.
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.


	c.server()
	return &c
}
