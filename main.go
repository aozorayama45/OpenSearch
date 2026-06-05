package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds the configuration settings for the adaptive merge throttling.
type Config struct {
	MaxMergeAtOnceExplicit int64
	SearchLatencyThreshold time.Duration
	MaxBytesPerSec         int64 // Max merge I/O rate limit in bytes/sec
	MinBytesPerSec         int64 // Min merge I/O rate limit in bytes/sec
}

// Metrics tracks node-level and index-level metrics.
type Metrics struct {
	SearchIOWaitTimeNs   int64 // Time search threads spent waiting for I/O due to merge operations
	CurrentMergeRateLimit int64 // Current merge throttling rate limit in bytes/sec
}

// AdaptiveMergeScheduler manages merge operations and throttling.
type AdaptiveMergeScheduler struct {
	config     Config
	metrics    Metrics
	rateLimit  int64 // atomic
	searchQueue int64 // atomic queue depth
	mu         sync.Mutex
}

func NewAdaptiveMergeScheduler(cfg Config) *AdaptiveMergeScheduler {
	return &AdaptiveMergeScheduler{
		config:    cfg,
		rateLimit: cfg.MaxBytesPerSec,
	}
}

// RecordSearchStart and RecordSearchEnd simulate search latency tracking.
func (ams *AdaptiveMergeScheduler) RecordSearchStart() time.Time {
	atomic.AddInt64(&ams.searchQueue, 1)
	return time.Now()
}

func (ams *AdaptiveMergeScheduler) RecordSearchEnd(start time.Time) {
	atomic.AddInt64(&ams.searchQueue, -1)
	latency := time.Since(start)

	// If search latency exceeds threshold, dynamically scale down merge rate limit
	if latency > ams.config.SearchLatencyThreshold {
		ams.adjustRateLimit(false) // Scale down
	} else {
		ams.adjustRateLimit(true)  // Scale up/recover
	}
}

func (ams *AdaptiveMergeScheduler) adjustRateLimit(scaleUp bool) {
	ams.mu.Lock()
	defer ams.mu.Unlock()

	current := atomic.LoadInt64(&ams.rateLimit)
	var next int64
	if scaleUp {
		// Gradually increase rate limit back to max
		next = current + (ams.config.MaxBytesPerSec / 10)
		if next > ams.config.MaxBytesPerSec {
			next = ams.config.MaxBytesPerSec
		}
	} else {
		// Scale down rate limit (e.g., halve it)
		next = current / 2
		if next < ams.config.MinBytesPerSec {
			next = ams.config.MinBytesPerSec
		}
	}
	atomic.StoreInt64(&ams.rateLimit, next)
	atomic.StoreInt64(&ams.metrics.CurrentMergeRateLimit, next)
}

// SimulateMergeIO simulates I/O operations for merging, respecting the rate limit.
func (ams *AdaptiveMergeScheduler) SimulateMergeIO(bytes int64) {
	start := time.Now()
	limit := atomic.LoadInt64(&ams.rateLimit)
	
	// Simple rate limiting sleep simulation
	if limit > 0 {
		sleepDuration := time.Duration(bytes) * time.Second / time.Duration(limit)
		if sleepDuration > 0 {
			time.Sleep(sleepDuration)
		}
	}
	
	// If search queue is high, search threads might wait for I/O.
	// We track simulated I/O wait time for search threads.
	queueDepth := atomic.LoadInt64(&ams.searchQueue)
	if queueDepth > 0 {
		// Simulate search threads waiting for I/O due to merge contention
		waitTime := time.Since(start) / 10 // simulated contention factor
		atomic.AddInt64(&ams.metrics.SearchIOWaitTimeNs, int64(waitTime))
	}
}

func (ams *AdaptiveMergeScheduler) GetMetrics() (int64, int64) {
	ioWait := atomic.LoadInt64(&ams.metrics.SearchIOWaitTimeNs)
	rateLimit := atomic.LoadInt64(&ams.metrics.CurrentMergeRateLimit)
	return ioWait, rateLimit
}

func main() { 
	fmt.Println("Starting Adaptive Merge Throttling Simulation...")

	cfg := Config{
		MaxMergeAtOnceExplicit: 3,
		SearchLatencyThreshold: 50 * time.Millisecond,
		MaxBytesPerSec:         100 * 1024 * 1024, // 100 MB/s
		MinBytesPerSec:         10 * 1024 * 1024,  // 10 MB/s
	}

	scheduler := NewAdaptiveMergeScheduler(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// Simulate Search Threads (High Priority)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				t := scheduler.RecordSearchStart()
				// Simulate search query processing
				time.Sleep(10 * time.Millisecond)
				// Simulate occasional high latency spike to trigger throttling
				if time.Now().UnixNano()%5 == 0 {
					time.Sleep(60 * time.Millisecond)
				}
				scheduler.RecordSearchEnd(t)
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	// Simulate Merge Threads (Low Priority / Throttled)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// Simulate a merge operation processing chunks of data
				scheduler.SimulateMergeIO(5 * 1024 * 1024) // 5 MB chunk
				time.Sleep(20 * time.Millisecond)
			}
		}
	}()

	// Monitor metrics
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ioWait, rateLimit := scheduler.GetMetrics()
				fmt.Printf("Metrics -> Current Merge Rate Limit: %.2f MB/s, Search I/O Wait Time: %v\n",
					float64(rateLimit)/(1024*1024), time.Duration(ioWait))
			}
		}
	}()

	wg.Wait()
	fmt.Println("Simulation completed successfully.")
}