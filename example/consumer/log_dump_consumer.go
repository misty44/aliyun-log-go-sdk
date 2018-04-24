package main

import (
	"os"
	"github.com/bailaohe/aliyun-log-go-sdk"
	"github.com/bailaohe/aliyun-log-go-sdk/consumer"
	"time"
	"log"
)

type DumpProcessor struct{}

// Initialize the processer
func (self *DumpProcessor) Initialize(shard int) error {
	return nil
}

// Major processing function
func (self *DumpProcessor) Process(logGroupList *sls.LogGroupList, tracker *consumer.CheckpointTracker) (string, error) {
	log.Printf("processing ... %v", len(logGroupList.LogGroups))

	for _, logGroup := range logGroupList.LogGroups {
		for _, logItem := range logGroup.Logs {
			log.Println("Get logItem ...")
			for _, logContent := range logItem.Contents {
				log.Printf("%v = %v", logContent.GetKey(), logContent.GetValue())
			}
		}
	}
	return "", nil
}

// Shutdown the processor
func (self *DumpProcessor) Shutdown(*consumer.CheckpointTracker) error {
	return nil
}

func main() {
	endpoint := os.Getenv("LOG_TEST_ENDPOINT")
	projectName := os.Getenv("LOG_TEST_PROJECT")
	logstoreName := os.Getenv("LOG_TEST_LOGSTORE")
	accessKeyID := os.Getenv("LOG_TEST_ACCESS_KEY_ID")
	accessKeySecret := os.Getenv("LOG_TEST_ACCESS_KEY_SECRET")

	slsProject, _ := sls.NewLogProject(projectName, endpoint, accessKeyID, accessKeySecret)
	slsLogstore, _ := slsProject.GetLogStore(logstoreName)

	consumer1, _ := consumer.NewConsumerWorker(slsLogstore, &DumpProcessor{}, &consumer.ConsumerConfig{
		ConsumerGroup:     "sdktest",
		Consumer:          "sdktest-1",
		HeartbeatInterval: 6 * time.Second,
		DataFetchInterval: time.Second,
	})

	consumer1.Startup()

	time.Sleep(12*time.Second)
	consumer1.Shutdown()
}
