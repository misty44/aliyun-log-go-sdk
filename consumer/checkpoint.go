package consumer

import (
	"time"

	"github.com/misty44/aliyun-log-go-sdk"
)

type CheckpointTracker struct {
	LogStore *sls.LogStore

	ConsumerGroup string
	Consumer      string
	Shard         int

	CachedCheckpoint         string
	LastPersistentCheckpoint string
	Cursor                   string
	LastFlushTime            time.Time
}

const defaultFlushCheckpointInterval time.Duration = 60

func (t *CheckpointTracker) SaveCheckpoint(persistent bool, cursor string) {
	if len(cursor) > 0 {
		t.CachedCheckpoint = cursor
	} else {
		t.CachedCheckpoint = t.Cursor
	}
	if persistent {
		t.flushCheckpoint()
	}
}

func (t *CheckpointTracker) flushCheckpoint() {
	if len(t.Cursor) > 0 && t.CachedCheckpoint != t.LastPersistentCheckpoint {
		t.LogStore.UpdateCheckpoint(t.ConsumerGroup, t.Consumer, t.Shard, t.CachedCheckpoint, true)
		t.LastPersistentCheckpoint = t.CachedCheckpoint
	}
}

func (t *CheckpointTracker) flushCheck() {
	currentTime := time.Now()
	if currentTime.After(t.LastFlushTime.Add(defaultFlushCheckpointInterval)) {
		t.flushCheckpoint()
		t.LastFlushTime = currentTime
	}
}
