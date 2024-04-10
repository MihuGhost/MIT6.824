package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type State int

const (
	Map = 10
	Reduce = 20
	Wait = 30
	Exit = 40
)

type MapTask struct{
	TaskState State,
	InputFIle string,
}

type Coordinator struct {
	InputFiles []string, 
	TaskQueue chan *Task,
}


func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		InputFiles : files,
		TaskQueue : make(chan, max(nReduce,len(files))),
	}

	//创建Map任务
	//放入任务队列
	c.CreateMapTask()

	c.server()
	return &c
}

//分配任务
func (c *Coordinator) AssignTask(req *Req, task *Task) error{
		if len(c.TaskQueue) > 0{
			//有任务 -> 分配任务
		} 
		//没有任务
}

//创建Map任务
func (c *Coordinator) CreateMapTask(){
	for i,filename := range c.InputFiles{
		mapTask := MapTask{
			FileName : filename
			TaskState :Map
		}
		//放入任务队列
		c.TaskQueue <- &mapTask
	}
}

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

func (c *Coordinator) Done() bool {
	ret := true

	return ret
}

