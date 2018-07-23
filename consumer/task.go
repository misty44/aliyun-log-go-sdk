package consumer

import (
	"github.com/misty44/aliyun-log-go-sdk"
	"github.com/sirupsen/logrus"
)

type Task func() TaskResult

//======================================================================================================================

type TaskResult interface {
	getError() error
	handle(*ShardedConsumerWorker)
}

type TaskResultBase struct {
	err error
}

func (self *TaskResultBase) getError() error {
	return self.err
}

type InitTaskResult struct {
	TaskResultBase
	cursor             string
	isCursorPersistent bool
}

func (self *InitTaskResult) handle(context *ShardedConsumerWorker) {
	context.NextCursor = self.cursor
	context.CheckpointTracker.CachedCheckpoint = context.NextCursor

	if self.isCursorPersistent {
		context.LastFetchedLogGroup = nil
		//TODO cancel current tasks
		context.CheckpointTracker.LastPersistentCheckpoint = context.NextCursor
	}
}

type ProcessTaskResult struct {
	TaskResultBase
	rollbackCheckpoint string
}

func (self *ProcessTaskResult) handle(context *ShardedConsumerWorker) {
	if len(self.rollbackCheckpoint) > 0 {
		context.NextCursor = self.rollbackCheckpoint
	}
}

type FetchTaskResult struct {
	TaskResultBase
	Cursor           string
	FetchedLogGroups *sls.LogGroupList
}

func (self *FetchTaskResult) handle(context *ShardedConsumerWorker) {
	context.LastFetchedLogGroup = &FetchedLogGroup{
		Shard:               context.Shard,
		FetchedLogGroupList: self.FetchedLogGroups,
		EndCursor:           self.Cursor,
	}
	context.NextCursor = self.Cursor
	context.LastFetchedCount = len(self.FetchedLogGroups.LogGroups)
	logrus.Printf("Fetched, count = %v", context.LastFetchedCount)
}

type ShutdownTaskResult struct {
	TaskResultBase
}

func (self *ShutdownTaskResult) handle(context *ShardedConsumerWorker) {
	// do nothing
}

//======================================================================================================================

type TaskExecutor struct {
	shutFlag bool
	nWorkers uint
	tasks    chan Task
	results  chan TaskResult
}

func NewTaskExecutor(nWorkers uint) *TaskExecutor {
	p := &TaskExecutor{
		shutFlag: false,
		nWorkers: nWorkers,
		tasks:    make(chan Task, 64),
		results:  make(chan TaskResult, 64),
	}

	for w := uint(1); w <= nWorkers; w++ {
		go func() {
			for t := range p.tasks {
				p.results <- t()
			}
		}()
	}

	return p
}

func (self *TaskExecutor) submit(t Task) {
	if !self.shutFlag {
		self.tasks <- t
	}
}

func (self *TaskExecutor) getResult() TaskResult {
	if !self.shutFlag {
		result := <-self.results
		return result
	}
	return nil
}

func (self *TaskExecutor) shutdown() {
	self.shutFlag = true
	close(self.tasks)
}
