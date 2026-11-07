package app

import (
	"context"
	"errors"
	"time"

	"cpa-usage-keeper/internal/config"
	"cpa-usage-keeper/internal/poller"
	"gorm.io/gorm"
)

// newPricingBootstrapIngestRunner 在同一接收生命周期内先写实际旧 inbox 列，业务就绪后移交标准写列与通知。
func newPricingBootstrapIngestRunner(cfg config.Config, db *gorm.DB) (*poller.RedisIngestRunner, *UsageIngestBridge) {
	bridge := NewUsageIngestBridge(poller.NewPricingBootstrapInboxWriter(db))
	return newUsageIngestRunner(cfg, bridge, bridge), bridge
}

// newUsageIngestRunner 复用订阅、Redis pull 和 HTTP pull 的既有优先级及降级合同。
// writer 决定启动引导或正常 schema 的 inbox 写列；observer 只在业务运行阶段注入。
func newUsageIngestRunner(cfg config.Config, writer poller.RedisInboxWriter, observer poller.RedisControlMessageObserver) *poller.RedisIngestRunner {
	// Subscribe-only build: never pop the CPA usage queue, over RESP or HTTP. Another
	// collector owns the queue, and every record a pull takes is lost to it. Both pull
	// sources fail, so the runner keeps retrying SUBSCRIBE: backfill after a connect is
	// skipped (the runner tolerates a failed backfill), and a dropped subscription waits
	// for the next resubscribe instead of polling the queue.
	redisPullSource := disabledPullSource{}
	httpPullSource := disabledPullSource{}
	redisSubscribeSource := poller.NewRedisSubscribeSource(poller.RedisSubscribeOptions{
		BaseURL: cfg.CPABaseURL, RedisAddr: cfg.RedisQueueAddr, ManagementKey: cfg.CPAManagementKey,
		Timeout: cfg.RequestTimeout, TLS: cfg.RedisQueueTLS, TLSSkipVerify: cfg.TLSSkipVerify,
	})
	filteredWriter := poller.NewControlAwareRedisInboxWriter(writer, observer)
	runner := poller.NewRedisIngestRunner(redisSubscribeSource, redisPullSource, httpPullSource, filteredWriter, poller.RedisIngestRunnerConfig{
		IdleInterval: cfg.RedisQueueIdleInterval, BatchSize: cfg.RedisQueueBatchSize,
		HTTPBackoffInitial: time.Second, HTTPBackoffMax: 30 * time.Second,
	})
	if observer != nil {
		runner.SetControlMessageObserver(observer)
	}
	return runner
}

// errQueuePullDisabled is returned by every pull in the subscribe-only build.
var errQueuePullDisabled = errors.New("usage queue pull disabled: subscribe-only build")

// disabledPullSource stands in for the Redis and HTTP queue pulls so they never pop.
type disabledPullSource struct{}

func (disabledPullSource) Pull(context.Context) ([]string, error) {
	return nil, errQueuePullDisabled
}
