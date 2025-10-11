package pipeline

import (
	"errors"
	"fmt"
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
type PlUnit[T any, R any] struct {
	name string
	task *task.Task[T, R]
}
func NewPlUnit[T any, R any](name string, t *task.Task[T, R]) *PlUnit[T, R] {
	return &PlUnit[T, R] {
		name: name,
		task: t,
	}
}


type Pipeline struct {
	inTasks map[string]task.Tasker
	outTasks map[string]task.Tasker
	tasks map[string]task.Tasker
	ch chan PlMeta
}

func NewPipeline() *Pipeline {
	return &Pipeline{
		inTasks: make(map[string]task.Tasker),
		outTasks: make(map[string]task.Tasker),
		tasks: make(map[string]task.Tasker),
		ch: make(chan PlMeta, 1000),
	}
}

func PlRegister[T any, R any](pl *Pipeline, pu *PlUnit[T, R], taskType PlTaskType) {
	if taskType == INPUT {
		plug := NewPlUnit(pu.name + "_soc", task.NewTask(0, func (data T) T {
			return data
		}))
		
		pl.inTasks[pu.name] = plug.task
		pl.tasks[pu.name] = plug.task
		pl.tasks[pu.name + "soc"] = pu.task
		PlPipe(pl, plug, pu)
	} else if taskType == OUTPUT {
		plug := NewPlUnit(pu.name + "_soc", task.NewTask(0, func (data R) R {
			return data
		}))
		pl.outTasks[pu.name] = plug.task
		pl.tasks[pu.name] = plug.task
		pl.tasks[pu.name + "soc"] = pu.task
		PlPipe(pl, pu, plug)
	} else {
		pl.tasks[pu.name] = pu.task
	}

}

func PlPipe[T, R, S any](pl *Pipeline, from *PlUnit[T, R], to *PlUnit[R, S]) {
	go func() {
		defer to.task.Stop()
		for v := range from.task.Out() {
			to.task.In() <- v
		}
	}()
}
func PlMetricPipe[T, R, S any](pl *Pipeline, from *PlUnit[T, R], to *PlUnit[R, S], dur time.Duration) {
	ticker := time.NewTicker(dur)	
	var cnt int64 = 0
	go func() {
		defer to.task.Stop()
		for {
			select {
			case v, ok := <-from.task.Out():
				if !ok {
					return
				} else {
					to.task.In() <- v
					cnt += 1
				}
			case t := <-ticker.C:
				pl.ch <- PlMeta {
					MetaType: PL_MT_METRIC,
					Payload: fmt.Sprintf("[METRIC] DUR(%s) TIME(%s) THROUGHPUT: %d\n", dur.String(), t.String(), cnt),
				}
				cnt = 0	
			}
		}
	}()
}

func(pl *Pipeline) Run() {
	for _, task := range pl.tasks {
		task.Run()
	}
	go func() {
		for m := range pl.ch {
			switch m.MetaType {
			case PL_MT_ERROR:
				log.Printf("[PL_MT_ERROR] %v", m)
			case PL_MT_METRIC:
				log.Printf("[PL_MT_METRIC] %v", m)
			}
		}
	}()
}

func(pl *Pipeline) Stop() {
	for _, task := range pl.inTasks {
		task.Stop()
	}
	close(pl.ch)
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

func PlOutput[R any](pl *Pipeline, name string, ch chan<- R) error {
	t, ok := pl.outTasks[name]	
	if !ok {
		return errors.New("no output plug exist")
	}
	st, ok := t.(*task.Task[R, R])
	if !ok {
		return errors.New("output type not matches") 
	}
	go func() {
		defer close(ch)
		for v := range st.Out() {
			ch <- v
		}	
	}()

	return nil
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
	conv1 := NewPlUnit("in1", task.ConvTask(task.PL_EXCH_UPBIT))
	PlRegister(pl, conv1, INPUT)
	conv2 := NewPlUnit("in2", task.ConvTask(task.PL_EXCH_BITHUMB))
	PlRegister(pl, conv2, INPUT)
	logUnit := NewPlUnit("log", task.LogTask(true))
	PlRegister(pl, logUnit, INTERNAL)
	nullUnit := NewPlUnit("null", task.NullTask())
	PlRegister(pl, nullUnit, OUTPUT)

	PlPipe(pl, conv1, logUnit)
	PlPipe(pl, conv2, logUnit)
	PlMetricPipe(pl, logUnit, nullUnit, time.Second)

	return pl, nil
}

func CryptoProdPipeline() (*Pipeline, error) {
	pl := NewPipeline()
	conv1 := NewPlUnit("in1", task.ConvTask(task.PL_EXCH_UPBIT))
	PlRegister(pl, conv1, INPUT)
	conv2 := NewPlUnit("in2", task.ConvTask(task.PL_EXCH_BITHUMB))
	PlRegister(pl, conv2, INPUT)
	prodTask, err := kafka.ProduceTask(kafka.ProduceUnitConfig{
		Brokers: []string{os.Getenv("KAFKA_BROKER")},
	})
	if err != nil {
		return nil, err
	}
	prodUnit := NewPlUnit("pd", prodTask)
	PlRegister(pl, prodUnit, INTERNAL)
	logUnit := NewPlUnit("log", task.LogTask(true))
	PlRegister(pl, logUnit, INTERNAL)
	nullUnit := NewPlUnit("null", task.NullTask())
	PlRegister(pl, nullUnit, OUTPUT)

	PlPipe(pl, conv1, prodUnit)
	PlPipe(pl, conv2, prodUnit)
	PlPipe(pl, prodUnit, logUnit)
	PlMetricPipe(pl, logUnit, nullUnit, time.Second)

	return pl, nil
}

func DiffPipeline() (*Pipeline, error) {
	pl := NewPipeline()

	conv1 := NewPlUnit("in1", task.ConvTask(task.PL_EXCH_UPBIT))
	PlRegister(pl, conv1, INPUT)

	conv2 := NewPlUnit("in2", task.ConvTask(task.PL_EXCH_BITHUMB))
	PlRegister(pl, conv2, INPUT)

	logUnit := NewPlUnit("log", task.LogTask(false))
	PlRegister(pl, logUnit, INTERNAL)

	state := NewPlUnit("state", calc.StateTask(task.PL_EXCH_UPBIT, task.PL_EXCH_BITHUMB))
	PlRegister(pl, state, INTERNAL)

	cal := NewPlUnit("calc", calc.CalcTask(task.PL_EXCH_UPBIT, task.PL_EXCH_BITHUMB))
	PlRegister(pl, cal, INTERNAL)

	nullUnit := NewPlUnit("null", task.NullTask())
	PlRegister(pl, nullUnit, OUTPUT)

	PlPipe(pl, conv1, logUnit)
	PlPipe(pl, conv2, logUnit)
	PlPipe(pl, logUnit, state)
	PlPipe(pl, state, cal)

	return pl, nil
}


