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

const (
	Init Status = iota
	Inprogress
	Completed
)

type Task struct{
	TaskState State //任务状态
	InputFile string
	IntermediateFiles []string //中间文件
	NReduce int //nReduce
	TaskNumber int //任务ID
	TaskStatus Status //任务完成状态 初始化、正进行、已完成
}

type Coordinator struct {
	InputFiles []string  //输入文件
	TaskQueue chan *Task //任务队列
	IntermediateFiles [][]string //Map产生nReduce份中间文件
	TaskPhase State //Coordnator阶段
	NReduce int //nReduce
	TaskPool []*Task //任务池，记录任务完成状态
}


func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		InputFiles : files,
		TaskQueue : make(chan *Task, max(nReduce,len(files))),
		NReduce : nReduce,
		TaskPool : make([]*Task, max(nReduce,len(files))),
	}

	//创建Map任务
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
		c.TaskPool[task.TaskNumber].TaskStatus = Inprogress
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
	for index,filename := range c.InputFiles{
		mapTask := Task{
			InputFile : filename,
			TaskState : Map,
			NReduce : c.NReduce,
			TaskNumber : index,
			TaskStatus : Init,
		}
		//放入任务队列
		c.TaskQueue <- &mapTask
		//放入任务池
		c.TaskPool[index] = &mapTask
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

//worker任务完成
func (c *Coordinator)TaskCompleted(task *Task,resp *Resp) error {
	if c.TaskPhase != task.TaskState || task.TaskStatus == Completed {
		return nil
	}
	//处理任务状态
	task.TaskStatus = Completed
	
	//处理中间文件
	switch task.TaskState{
	case Map:
		c.IntermediateFiles[task.TaskNumber] = task.IntermediateFiles
		if allTaskDone(){
			//创建Reduce任务
			c.CreateReduceTask()
			c.TaskPhase = Reduce
		}
	case Reduce:
		//todo: 输出处理
		
		if allTaskDone(){
			c.TaskPhase = Exit
		}	
	}


}

//判断当前任务是否全部完成
func (c *Coordinator)allTaskDone() bool{
	for _, task := range c.TaskPool {
		if task.TaskStatus != Completed{
			return false
		}
	}
	return true
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

