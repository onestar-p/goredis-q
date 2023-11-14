package goredisq

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/redis/go-redis/v9"
)

type GoredisQ struct {
	redisClient *redis.Client
	wg          sync.WaitGroup
}

const (
	redisQueueName = "messageQueue"
)

func (g *GoredisQ) NewGoredisQ(options *redis.Options) *GoredisQ {
	goredisQ := GoredisQ{}

	goredisQ.redisClient = redis.NewClient(options)

	return &goredisQ
}

func (g *GoredisQ) Ping(ctx context.Context) (err error) {

	_, err = g.redisClient.Ping(ctx).Result()
	if err != nil {
		return
	}

	fmt.Println("Connected to Redis")
	return
}

func (g *GoredisQ) Producer(ctx context.Context, message string) (err error) {
	defer g.wg.Done()
	err = g.redisClient.LPush(ctx, redisQueueName, message).Err()
	return
}

func (g *GoredisQ) consumer(ctx context.Context) {
	defer g.wg.Done()

	for {
		result, err := g.redisClient.BRPop(ctx, 0, redisQueueName).Result()
		if err != nil {
			log.Printf("Failed to pop message from Redis queue: %v", err)
		}

		// 处理接收到的消息
		if len(result) > 1 {
			message := result[1]
			fmt.Println("Received message:", message)
		}
	}
}

func (g *GoredisQ) Run(ctx context.Context) {

	// 启动两个消费者 Goroutine
	for i := 0; i < 2; i++ {
		g.wg.Add(1)
		go g.consumer(ctx)
	}

	// 启动生产者 Goroutine
	g.wg.Add(1)
	go g.Producer(ctx, "Hello, World!")

	// 等待生产者和消费者完成
	g.wg.Wait()

	// 关闭 Redis 连接
	err := g.redisClient.Close()
	if err != nil {
		log.Printf("Failed to close Redis connection: %v", err)
	}
}
