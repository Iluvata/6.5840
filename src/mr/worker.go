package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	id := os.Getpid()
	for {
		task := TaskReply{}
		AskForTask(&TaskArgs{id}, &task)
		if task.Done {
			return
		}
		if task.MapTask {
			maptask(&task, mapf)
		} else {
			reducetask(&task, reducef)
		}
		taskdone := TaskDoneArgs{}
		taskdone.MapTask = task.MapTask
		taskdone.TaskID = task.TaskID
		taskdone.WorkerID = id
		TaskDone(&taskdone, &TaskDoneReply{})
	}
}

func maptask(task *TaskReply, mapf func(string, string) []KeyValue) {
	// map task
	ifile, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := io.ReadAll(ifile)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	ifile.Close()

	tempfiles := make([]*os.File, task.NReduce)
	encoders := make([]*json.Encoder, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		file, err := os.CreateTemp("", fmt.Sprintf("mr-%d-%d", task.TaskID, i))
		if err != nil {
			log.Fatal(err)
		}
		// defer os.Remove(file.Name())

		tempfiles[i] = file
		encoders[i] = json.NewEncoder(file)
	}

	kva := mapf(task.Filename, string(content))

	for _, kv := range kva {
		enc := encoders[ihash(kv.Key)%task.NReduce]
		enc.Encode(kv)
	}

	for i, tf := range tempfiles {
		if err := os.Rename(tf.Name(), fmt.Sprintf("mr-%d-%d", task.TaskID, i)); err != nil {
			fmt.Printf("mr-%d-%d has exist?\n", task.TaskID, i)
			if err := tf.Close(); err != nil {
				log.Fatal(err)
			}
			os.Remove(tf.Name())
		} else {
			// rename succeeded
			if err := tf.Close(); err != nil {
				log.Fatal(err)
			}
		}
	}
}

func reducetask(task *TaskReply, reducef func(string, []string) string) {
	// reduce task
	kva := []KeyValue{}
	for i := 0; i < task.NMap; i++ {
		file, err := os.Open(fmt.Sprintf("mr-%d-%d", i, task.TaskID))
		if err != nil {
			log.Fatalf("cannot open mr-%d-%d", i, task.TaskID)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		if err := file.Close(); err != nil {
			log.Fatal(err)
		}
	}
	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%d", task.TaskID)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot open %v", oname)
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
}

func AskForTask(args *TaskArgs, reply *TaskReply) {
	if ok := call("Coordinator.AssignTask", args, reply); !ok {
		fmt.Printf("AskForTask: call failed!\n")
		return
	}
}

func TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) {
	if ok := call("Coordinator.FinishTask", args, reply); !ok {
		fmt.Printf("TaskDone: call failed!\n")
		return
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
