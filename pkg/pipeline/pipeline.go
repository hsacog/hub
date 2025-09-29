package pipeline

import (
	"errors"
	"hub/pkg/pipeline/task"
	"hub/pkg/pipeline/task/calc"
	"hub/pkg/pipeline/task/kafka"
	"log"
	"os"

	// "hub/pkg/pipeline/task/kafka"
	"time"
)

type PlTaskType uint8
const (
	INPUT = iota
	OUTPUT
	INTERNAL
)

type Pipeline struct {
	inTasks map[string]task.Tasker
	outTasks map[string]task.Tasker
	tasks map[string]task.Tasker
}

type PlInPlug[T any] struct {
	task *task.Task[T, T]
}

func NewPipeline() *Pipeline {
	return &Pipeline{
		inTasks: make(map[string]task.Tasker),
		outTasks: make(map[string]task.Tasker),
		tasks: make(map[string]task.Tasker),
	}
}

func(pl *Pipeline) Register(name string, task task.Tasker, taskType PlTaskType) {
	pl.tasks[name] = task
	if taskType == INPUT {
		pl.inTasks[name] = task
	} else if taskType == OUTPUT {
		pl.outTasks[name] = task
	}
}

func PlRegister[T any, R any](pl *Pipeline, name string, t *task.Task[T, R], taskType PlTaskType) {
	if taskType == INPUT {
		plug := task.NewTask(0, func (data T) T {
			return data
		})
		pl.inTasks[name] = plug
		pl.tasks[name] = plug
		pl.tasks[name + "soc"] = t
		task.TaskConnector(plug, t)
	} else if taskType == OUTPUT {
		plug := task.NewTask(0, func (data R) R {
			return data
		})
		pl.outTasks[name] = plug
		pl.tasks[name] = plug
		pl.tasks[name + "soc"] = t
		task.TaskConnector(t, plug)
	} else {
		pl.tasks[name] = t
	}

}

func(pl *Pipeline) Run() {
	for _, task := range pl.tasks {
		task.Run()
	}
}

func(pl *Pipeline) Stop() {
	for _, task := range pl.inTasks {
		task.Run()
	}
}

func PlInput[T any](pl *Pipeline, name string, data T) error {
	t, ok := pl.inTasks[name]	
	if !ok {
		return errors.New("no input plug exist")
	}
	st, ok := t.(*task.Task[T, T])
	if !ok {
		return errors.New("input type not matches") 
	}

	st.In() <- data

	return nil
}

func PlOutput[R any](pl *Pipeline, name string) (<-chan R, error) {
	t, ok := pl.outTasks[name]	
	if !ok {
		return nil, errors.New("no output plug exist")
	}
	st, ok := t.(*task.Task[R, R])
	if !ok {
		return nil, errors.New("output type not matches") 
	}
	return st.Out(), nil
}

func GetPlTask[T any, R any](pl *Pipeline, name string) (*task.Task[T, R], error) {
	t, ok := pl.tasks[name]
	if !ok {
		return nil, errors.New("no task exist")
	}

	st, ok := t.(*task.Task[T, R])
	if !ok {
		return nil, errors.New("type not matches") 
	}

	return st, nil
}

func CryptoPipeline() (*Pipeline, error) {
	pl := NewPipeline()
	nullTask := task.NullTask()
	PlRegister(pl, "null", nullTask, OUTPUT)
	logTask := task.LogTask(false)
	PlRegister(pl, "log", logTask, INTERNAL)
	conv1 := task.ConvTask(task.PL_EXCH_UPBIT)
	PlRegister(pl, "in1", conv1, INPUT)
	conv2 := task.ConvTask(task.PL_EXCH_BITHUMB)
	PlRegister(pl, "in2", conv2, INPUT)

	task.TaskConnector(conv1, logTask)
	task.TaskConnector(conv2, logTask)
	task.TaskMetricConnector(logTask, nullTask, time.Second)

	return pl, nil
}

func CryptoProdPipeline() (*Pipeline, error) {
	pl := NewPipeline()
	nullTask := task.NullTask()
	PlRegister(pl, "null", nullTask, OUTPUT)
	logTask := task.LogTask(false)
	PlRegister(pl, "log", logTask, INTERNAL)
	prodTask, err := kafka.ProduceTask(kafka.ProduceUnitConfig{
		Brokers: []string{os.Getenv("KAFKA_BROKER")},
	})
	if err != nil {
		return nil, err
	}
	PlRegister(pl, "pd", prodTask, INTERNAL)
	conv1 := task.ConvTask(task.PL_EXCH_UPBIT)
	PlRegister(pl, "in1", conv1, INPUT)
	conv2 := task.ConvTask(task.PL_EXCH_BITHUMB)
	PlRegister(pl, "in2", conv2, INPUT)

	task.TaskConnector(conv1, prodTask)
	task.TaskConnector(conv2, prodTask)
	task.TaskConnector(prodTask, logTask)
	task.TaskMetricConnector(logTask, nullTask, time.Second)

	return pl, nil
}

func DiffPipeline() (*Pipeline, error) {
	pl := NewPipeline()
	conv1 := task.ConvTask(task.PL_EXCH_UPBIT)
	PlRegister(pl, "in1", conv1, INPUT)
	conv2 := task.ConvTask(task.PL_EXCH_BITHUMB)
	PlRegister(pl, "in2", conv2, INPUT)
	log.Println("state")
	state := calc.StateTask(task.PL_EXCH_UPBIT, task.PL_EXCH_BITHUMB)
	PlRegister(pl, "state", state, INTERNAL)
	log.Println("cal")
	cal := calc.CalcTask(task.PL_EXCH_UPBIT, task.PL_EXCH_BITHUMB)
	PlRegister(pl, "cal", cal, INTERNAL)
	nullTask := task.NullTask()
	PlRegister(pl, "null", nullTask, OUTPUT)

	task.TaskConnector(conv1, state)
	task.TaskConnector(conv2, state)
	task.TaskConnector(state, cal)

	return pl, nil
}


