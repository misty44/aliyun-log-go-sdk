package consumer

import (
	"encoding/json"
	"time"

	"github.com/misty44/aliyun-log-go-sdk"
	logrus "github.com/sirupsen/logrus"
)

// The type of cached fetched log group
type FetchedLogGroup struct {
	Shard               int
	FetchedLogGroupList *sls.LogGroupList
	EndCursor           string
}

// The interface for user to implement their consumer processor
type ConsumerProcessor interface {
	// Initialize the processer
	Initialize(shard int) error
	// Major processing function
	Process(*sls.LogGroupList, *CheckpointTracker) (string, error)
	// Shutdown the processor
	Shutdown(*CheckpointTracker) error
}

type WorkerStatus int

const (
	STARTING = iota
	INITIALIZING
	PROCESSING
	SHUTTING_DOWN
	SHUTDONW_COMPLETE
)

type ShardedConsumerWorker struct {
	LogStore          *sls.LogStore
	CheckpointTracker *CheckpointTracker
	Processor         ConsumerProcessor
	executor          *TaskExecutor

	ConsumerGroup       string
	Consumer            string
	Shard               int
	NextCursor          string
	EndCursor           string
	LastFetchedLogGroup *FetchedLogGroup
	LastFetchedCount    int
	LastFetchTime       time.Time

	shutFlag bool
	status   WorkerStatus
}

func (self *ShardedConsumerWorker) consume() {
	logrus.Debugf("ShardedConsumerWorker start consuming, status = %v", self.status)

	taskSuccess := false

	// Handle the result of previous submit tasks
	if self.status != STARTING {
		result := self.executor.getResult()
		if result.getError() == nil {
			taskSuccess = true
			result.handle(self)
		}
	}

	// Update the consumer status
	if self.status == STARTING {
		self.status = INITIALIZING
	} else if self.status == SHUTTING_DOWN {
		// if no task or previous task suceeds, shutting-down -> shutdown complete
		if taskSuccess {
			self.status = SHUTDONW_COMPLETE
		}
	} else if self.shutFlag {
		// always set to shutting down (if flag is set)
		self.status = SHUTTING_DOWN
	} else if self.status == INITIALIZING {
		// if initing and has task succeed, init- > processing
		if taskSuccess {
			self.status = PROCESSING
		}
	}

	//===============================================================================
	switch self.status {
	case STARTING, INITIALIZING:
		{
			self.executor.submit(self.executeInit)
		}
	case PROCESSING:
		{
			if self.LastFetchedLogGroup != nil {
				self.CheckpointTracker.Cursor = self.LastFetchedLogGroup.EndCursor
				consumingLogList := self.LastFetchedLogGroup.FetchedLogGroupList
				logrus.Debugf("try processing fetched data, lastFetchedCount = %v", self.LastFetchedCount)
				self.LastFetchedLogGroup = nil

				if self.LastFetchedCount > 0 {
					logrus.Debugln("start processing fetched data ...")
					self.executor.submit(func() TaskResult {
						return self.executeProcess(consumingLogList)
					})
				} else {
					self.executor.submit(func() TaskResult {
						return &ProcessTaskResult{
							TaskResultBase: TaskResultBase{
								err: sls.NewClientError("Empty data fetched"),
							},
						}
					})
				}
			} else {
				triggerNewFetch := true

				if self.LastFetchedCount < 100 {
					triggerNewFetch = time.Since(self.LastFetchTime) > time.Millisecond*500
				} else if self.LastFetchedCount < 500 {
					triggerNewFetch = time.Since(self.LastFetchTime) > time.Millisecond*200
				} else if self.LastFetchedCount < 1000 {
					triggerNewFetch = time.Since(self.LastFetchTime) > time.Millisecond*50
				}
				logrus.Debugf("try fetching log data ... %v, %v, %v",
					self.LastFetchedCount,
					time.Since(self.LastFetchTime)/time.Second,
					triggerNewFetch)

				if triggerNewFetch {
					logrus.Debugln("start fetching new data ...")
					self.LastFetchTime = time.Now()
					self.executor.submit(self.executeFetch)
				}

			}
		}
	case SHUTTING_DOWN:
		{
			self.executor.submit(self.executeShutdown)
		}
	}
}

func (self *ShardedConsumerWorker) Shutdown() {
	self.shutFlag = true
}

func (self *ShardedConsumerWorker) isShutdown() bool {
	return self.status == SHUTDONW_COMPLETE
}

func (context *ShardedConsumerWorker) executeInit() TaskResult {
	var cursor string
	var err error
	var isCursorPersistent = false
	checkpoint, err := context.LogStore.GetCheckpoint(context.ConsumerGroup, context.Shard)

	if err != nil {
		return &InitTaskResult{
			TaskResultBase: TaskResultBase{
				err: err,
			},
		}
	}
	if len(*checkpoint) > 0 {
		isCursorPersistent = true
		cursor = *checkpoint
	} else {
		cursor, err = context.LogStore.GetCursor(context.Shard, "end")
		if err != nil {
			return &InitTaskResult{
				TaskResultBase: TaskResultBase{
					err: err,
				},
			}
		}
	}

	return &InitTaskResult{
		cursor:             cursor,
		isCursorPersistent: isCursorPersistent,
	}
}

func (context *ShardedConsumerWorker) executeProcess(fetchedLogGroupList *sls.LogGroupList) TaskResult {

	rollbackCheckpoint, err := context.Processor.Process(fetchedLogGroupList, context.CheckpointTracker)

	if err != nil {
		return &ProcessTaskResult{
			TaskResultBase: TaskResultBase{err},
		}
	}
	context.CheckpointTracker.flushCheck()

	return &ProcessTaskResult{
		rollbackCheckpoint: rollbackCheckpoint,
	}
}

func (context *ShardedConsumerWorker) executeFetch() TaskResult {
	var err error = nil
	var logGroupList *sls.LogGroupList
	var nextCursor string

	cursor := context.NextCursor
	for retry := 0; retry < 3; retry++ {
		logGroupList, nextCursor, err = context.LogStore.PullLogs(context.Shard, cursor, context.EndCursor, 1000)

		if err != nil {
			if retry == 0 {
				retryCursor, retryErr := context.LogStore.GetCursor(context.Shard, "end")
				if retryErr != nil {
					break
				}
				cursor = retryCursor
			} else {
				break
			}
		} else {
			if len(nextCursor) > 0 {
				cursor = nextCursor
			}
			return &FetchTaskResult{
				Cursor:           cursor,
				FetchedLogGroups: logGroupList,
			}
		}
	}
	return &FetchTaskResult{
		TaskResultBase: TaskResultBase{
			err: err,
		},
	}
}

func (context *ShardedConsumerWorker) executeShutdown() TaskResult {
	context.Processor.Shutdown(context.CheckpointTracker)
	return &ShutdownTaskResult{}
}

type ConsumerWorker struct {
	LogStore  *sls.LogStore
	Processor ConsumerProcessor

	config *ConsumerConfig

	shardedWorkers map[int]*ShardedConsumerWorker
	shutFlag       bool
	holdShards     []int
}

func NewConsumerWorker(logStore *sls.LogStore, processor ConsumerProcessor, config *ConsumerConfig) (*ConsumerWorker, error) {
	worker := ConsumerWorker{
		LogStore:  logStore,
		Processor: processor,
		config:    config,

		shardedWorkers: make(map[int]*ShardedConsumerWorker),
		shutFlag:       false,
		holdShards:     make([]int, 0),
	}

	err := logStore.CreateConsumerGroup(config.ConsumerGroup, config.HeartbeatInterval*2, false)
	if err != nil {
		nErr := &sls.Error{}
		inErr := &sls.Error{}
		json.Unmarshal([]byte(err.Error()), nErr)
		json.Unmarshal([]byte(nErr.Message), inErr)

		if inErr.Code != "ConsumerGroupAlreadyExist" {
			return nil, err
		}
	}

	return &worker, nil
}

func (self *ConsumerWorker) getShardedWorker(shardId int) *ShardedConsumerWorker {
	if worker, ok := self.shardedWorkers[shardId]; ok {
		return worker
	}

	worker := ShardedConsumerWorker{
		LogStore: self.LogStore,
		CheckpointTracker: &CheckpointTracker{
			LogStore: self.LogStore,

			ConsumerGroup: self.config.ConsumerGroup,
			Consumer:      self.config.Consumer,
			Shard:         shardId,
		},
		Processor: self.Processor,
		executor:  NewTaskExecutor(4),

		ConsumerGroup: self.config.ConsumerGroup,
		Consumer:      self.config.Consumer,
		Shard:         shardId,
		status:        STARTING,
	}
	self.shardedWorkers[shardId] = &worker
	return &worker
}

func (self *ConsumerWorker) cleanShardedWorker(ownedShards []int) {
	removeShards := []int{}

	ownedShardMap := map[int]int{}
	for _, shard := range ownedShards {
		ownedShardMap[shard] = 1
	}

	for shard, shardedWorker := range self.shardedWorkers {
		if _, existed := ownedShardMap[shard]; !existed {
			shardedWorker.Shutdown()
			logrus.Debugf("Try to shutdown unsiggned consumer shard: %v", shard)
		}
		if shardedWorker.isShutdown() {
			remainShards := []int{}
			// remove the unused shard
			for _, remainShard := range self.holdShards {
				if remainShard != shard {
					remainShards = append(remainShards, remainShard)
				}
			}
			removeShards = append(removeShards, shard)
			logrus.Debugf("Remove an unsigned consumer shard: %v", shard)
		}
	}

	for _, removeShard := range removeShards {
		delete(self.shardedWorkers, removeShard)
	}
}

func (self *ConsumerWorker) Shutdown() {
	self.shutFlag = true
	logrus.Debugln("ConsumerWorker shutdown")
}

func (self *ConsumerWorker) Startup() {
	logrus.Debugf("ConsumerWorker %s.%s startup\n", self.config.ConsumerGroup, self.config.Consumer)
	go func(self *ConsumerWorker) {
		go func(self *ConsumerWorker) {
			logrus.Debugln("ConsumerWorker %s.%s heartbeat started\n", self.config.ConsumerGroup, self.config.Consumer)
			for !self.shutFlag {
				logrus.Debugf("ConsumerWorker %s.%s heartbeat at %v\n", self.config.ConsumerGroup, self.config.Consumer, time.Now())
				newShards, err := self.LogStore.HeartBeat(self.config.ConsumerGroup, self.config.Consumer, self.holdShards)

				if err != nil {
					logrus.Fatalf("Heartbeat error: %v", err.Error())
				} else {
					self.holdShards = make([]int, len(newShards))
					copy(self.holdShards, newShards)
				}

				time.Sleep(self.config.HeartbeatInterval)
			}
			logrus.Debugf("ConsumerWorker %s.%s heartbeat stopped\n", self.config.ConsumerGroup, self.config.Consumer)
		}(self)

		for !self.shutFlag {
			copiedShards := self.holdShards

			for _, shard := range copiedShards {
				logrus.Debugf("ConsumerWorker shard %s\n", shard)

				shardedWorker := self.getShardedWorker(shard)
				shardedWorker.consume()
			}

			self.cleanShardedWorker(copiedShards)
			time.Sleep(self.config.DataFetchInterval)
		}
	}(self)
}

type ConsumerConfig struct {
	ConsumerGroup     string
	Consumer          string
	HeartbeatInterval time.Duration
	DataFetchInterval time.Duration
}
