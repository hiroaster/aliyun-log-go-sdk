package main

import (
	"fmt"
	"github.com/aliyun/aliyun-log-go-sdk"
	"github.com/aliyun/aliyun-log-go-sdk/consumer"
	"github.com/go-kit/kit/log/level"
	"math/rand"
	"os"
	"os/signal"
	"time"
)

// README :
// This is a very simple example of pulling data from your logstore and printing it for consumption.

func main() {
	option := consumerLibrary.LogHubConfig{
		Endpoint:          "",
		AccessKeyID:       "",
		AccessKeySecret:   "",
		Project:           "",
		Logstore:          "",
		ConsumerGroupName: "",
		ConsumerName:      "",
		// This options is used for initialization, will be ignored once consumer group is created and each shard has been started to be consumed.
		// Could be "begin", "end", "specific time format in time stamp", it's log receiving time.
		CursorPosition: consumerLibrary.BEGIN_CURSOR,
	}
	consumerWorker := consumerLibrary.InitConsumerWorker(option, process)
	ch := make(chan os.Signal)
	signal.Notify(ch)
	consumerWorker.Start()
	if _, ok := <-ch; ok {
		level.Info(consumerWorker.Logger).Log("msg", "get stop signal, start to stop consumer worker", "consumer worker name", option.ConsumerName)
		consumerWorker.StopAndWait()
	}
}

// resetTime 就是想重新消费的时间点，只能填入时间戳， begin 或者 end 三个值，例如每当消费到一个条件后，我想重置到昨天 2020.02.20 00:00 这个时间点开始消费，
// 就把这个时间的时间戳"1582128000"填进去，会得到那个时间点的 cursor
func getRestCursor(resetTime string, shardId int) string {
	client := sls.Client{
		Endpoint:"",
		AccessKeyID:"",
		AccessKeySecret:"",
	}
	cursor, err := client.GetCursor("project_name","logstore_name", shardId, resetTime)
	if err != nil {
		fmt.Println(err)
	}
	return cursor
	// 这个返回的cursor 就是填入时间的地方 ，获取的值为类似这种的  MTU4MjI*********5ODY3MA==
}

func process(shardId int, logGroupList *sls.LogGroupList) string {
	rand.Seed(time.Now().Unix())
	x := rand.Intn(10)
	// 例如当符合某种条件的时候（我这里是随机数等于5），重置消费位点，就将需要重置消费位点时间的cursor 填进去就可以
	// 这时候就会从你设定的消费时间开始重新消费。
	if x == 5 {
		return getRestCursor("1582128000", shardId)
	}
	return ""

}
