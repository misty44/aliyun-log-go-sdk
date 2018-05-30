package main

import (
	"os"
	"github.com/bailaohe/aliyun-log-go-sdk"
	"github.com/bailaohe/aliyun-log-go-sdk/consumer"
	"time"
	"log"
	"fmt"
	"os/signal"
	"syscall"
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
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	endpoint := os.Getenv("LOG_TEST_ENDPOINT")
	projectName := os.Getenv("LOG_TEST_PROJECT")
	logstoreName := os.Getenv("LOG_TEST_LOGSTORE")
	accessKeyID := os.Getenv("LOG_TEST_ACCESS_KEY_ID")
	accessKeySecret := os.Getenv("LOG_TEST_ACCESS_KEY_SECRET")

	slsProject, _ := sls.NewLogProject(projectName, endpoint, accessKeyID, accessKeySecret)
	slsLogstore, _ := slsProject.GetLogStore(logstoreName)

	shards, err := slsLogstore.ListShards()
	if err != nil {
		log.Println(err.Error())
		return
	}

	var consumers []*consumer.ConsumerWorker
	for idx, _ := range shards {
		consumer, _ := consumer.NewConsumerWorker(slsLogstore,
			&DumpProcessor{}, &consumer.ConsumerConfig{
				ConsumerGroup:     "datasync",
				Consumer:          fmt.Sprintf("datasync-%d", idx),
				HeartbeatInterval: 6 * time.Second,
				DataFetchInterval: time.Second,
			})
		consumers = append(consumers, consumer)
	}

	done := make(chan struct{}, 1)
	go func() {
		for _, c := range consumers {
			go func(w *consumer.ConsumerWorker) {
				w.Startup()
			}(c)
		}
		done <- struct{}{}
	}()


	select {
	case n := <-sc:
		log.Printf("receive signal %v, closing", n)
	}

	for _, c := range consumers {
		c.Shutdown()
	}
	<-done
}
