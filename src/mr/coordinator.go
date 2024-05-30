package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type State int
type Status int

var mu sync.Mutex

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
	StartTime time.Time
}

type Coordinator struct {
	InputFiles []string  //输入文件
	TaskQueue chan *Task //任务队列
	IntermediateFiles [][]string //Map产生nReduce份中间文件
	TaskPhase State //Coordnator阶段
	NReduce int //nReduce
	TaskPool map[int]*Task //任务池，记录任务完成状态，检查任务是否超时
}

//todo backup tasks
//10s 等待时长
//任务崩溃时，从任务池中取出崩溃前的任务分配

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		InputFiles : files,
		TaskQueue : make(chan *Task, max(nReduce,len(files))),
		NReduce : nReduce,
		TaskPhase: Map,
		TaskPool : make(map[int]*Task),
		IntermediateFiles: make([][]string,len(files)), //限制为X个Reduce任务，X为输入文件数量
	}

	//创建Map任务
	c.CreateMapTask()
	c.server()

	go c.checkCrash()
	return &c
}

func (c *Coordinator) checkCrash(){
	for{
		time.Sleep(5 * time.Second)
		mu.Lock()
		if c.TaskPhase == Exit{
			mu.Unlock()
			return
		}

		for _, task := range c.TaskPool {
			if task.TaskStatus == Inprogress && time.Now().Sub(task.StartTime) > 10*time.Second{
				c.TaskQueue <- c.TaskPool[task.TaskNumber]
			}
		}
		mu.Unlock()
	}
}

func max(a,b int)int {
	if a > b {
		return a
	}
	return b
}

//分配任务
func (c *Coordinator) AssignTask(req *Req, task *Task) error{
	mu.Lock()
	defer mu.Unlock()
	if len(c.TaskQueue) > 0{
		//有任务 -> 分配任务
		*task = *<-c.TaskQueue
		task.TaskStatus = Inprogress
		c.TaskPool[task.TaskNumber] = task
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
			StartTime : time.Now(),
		}
		//放入任务队列
		c.TaskQueue <- &mapTask
		//放入任务池
		c.TaskPool[index] = &mapTask
	}
}

//创建Reduce任务
func (c *Coordinator) CreateReduceTask(){
	for index, files := range c.IntermediateFiles {
		reduceTask := Task{
			TaskState : Reduce,
			IntermediateFiles : files,	
			TaskNumber : index,
			StartTime : time.Now(),
			TaskStatus : Init,
		}
		//放入任务队列
		c.TaskQueue <- &reduceTask
		reduceTask.TaskStatus = Inprogress
		c.TaskPool[index] = &reduceTask
	}
}

//worker任务完成
func (c *Coordinator)TaskCompleted(task *Task,resp *Resp) error {
	mu.Lock()
	defer mu.Unlock()
	if c.TaskPhase != task.TaskState || task.TaskStatus == Completed {
		return nil
	}
	//处理任务状态
	c.TaskPool[task.TaskNumber].TaskStatus = Completed
	//处理中间文件
	switch task.TaskState{
	case Map:
		//NReduce份临时文件集
		for nReduceID, filepath := range task.IntermediateFiles {
			index := nReduceID % len(c.InputFiles)
			c.IntermediateFiles[index] = append(c.IntermediateFiles[index],filepath)
		}
		if c.allTaskDone(){
			//创建Reduce任务
			c.CreateReduceTask()
			c.TaskPhase = Reduce
		}
	case Reduce:
		if c.allTaskDone(){
			c.TaskPhase = Exit
		}	
	}
	return nil
}

//判断任务池中是否全部完成
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
	mu.Lock()
	defer mu.Unlock()
	ret := c.TaskPhase == Exit
	return ret
}

