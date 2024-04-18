package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type State int

const (
	Map State = iota
	Reduce
	Wait
	Exit
)

type Task struct{
	TaskState State
	InputFile string
	IntermediateFiles []string //中间文件
}

type Coordinator struct {
	InputFiles []string  //输入文件
	TaskQueue chan *Task //任务队列
	IntermediateFiles [][]string //Map产生nReduce份中间文件
	TaskPhase State //Coordnator的阶段，确认退出还是等待
}


func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		InputFiles : files,
		TaskQueue : make(chan *Task, max(nReduce,len(files))),
	}

	//创建Map任务
	//放入任务队列
	c.CreateMapTask()

	c.server()
	return &c
}

func max(a,b int)int {
	if a > b {
		return a
	}
	return b
}

//分配任务
func (c *Coordinator) AssignTask(req *Req, task *Task) error{
	if len(c.TaskQueue) > 0{
		//有任务 -> 分配任务
		*task = *<-c.TaskQueue
	} else if c.TaskPhase == Exit{
		//是否结束所有任务
		*task = Task{TaskState : Exit}
	} else{
		//未结束，无任务
		*task = Task{TaskState : Wait}
	}
	return nil
}

//创建Map任务
func (c *Coordinator) CreateMapTask(){
	for _,filename := range c.InputFiles{
		mapTask := Task{
			InputFile : filename,
			TaskState :Map,
		}
		//放入任务队列
		c.TaskQueue <- &mapTask
	}
}

//创建Reduce任务
func (c *Coordinator) CreateReduceTask(){
	for _, files := range c.IntermediateFiles {
		reduceTask := Task{
			TaskState : Reduce,
			IntermediateFiles : files,	
		}
		//放入任务队列
		c.TaskQueue <- &reduceTask
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

