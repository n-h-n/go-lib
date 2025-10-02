package main

import (
	"context"
	"fmt"
	"time"

	"github.com/n-h-n/go-lib/local/llim"
	"github.com/n-h-n/go-lib/local/llock"
)

func main() {
	ctx := context.Background()

	// Example 1: Local Mutex Usage
	fmt.Println("=== Local Mutex Example ===")

	mutex1 := llock.NewMutex("service:resource", "app1",
		llock.WithVerbose(),
		llock.WithConcurrency(2),
		llock.WithTTL(10*time.Second),
	)

	mutex2 := llock.NewMutex("service:resource", "app2",
		llock.WithVerbose(),
		llock.WithConcurrency(2),
		llock.WithTTL(10*time.Second),
	)

	// Both should be able to acquire (concurrency = 2)
	if err := mutex1.Lock(ctx); err != nil {
		fmt.Printf("Error acquiring lock 1: %v\n", err)
	} else {
		fmt.Println("App1 acquired lock")
	}

	if err := mutex2.Lock(ctx); err != nil {
		fmt.Printf("Error acquiring lock 2: %v\n", err)
	} else {
		fmt.Println("App2 acquired lock")
	}

	// Third attempt should fail (max concurrency reached)
	mutex3 := llock.NewMutex("service:resource", "app3",
		llock.WithWait(false), // Don't wait
		llock.WithConcurrency(2),
	)

	if err := mutex3.Lock(ctx); err != nil {
		fmt.Printf("App3 failed to acquire lock (expected): %v\n", err)
	}

	// Unlock
	mutex1.Unlock(ctx)
	mutex2.Unlock(ctx)

	// Example 2: Local Rate Limiter Usage
	fmt.Println("\n=== Local Rate Limiter Example ===")

	// Create a rate limiter: 5 requests per second with burst of 10
	limiter := llim.NewLimiter(
		llim.WithLocalLimiter("api:endpoint", 5, time.Second, 10),
	)

	// Test Allow method
	for i := 0; i < 15; i++ {
		if limiter.Allow(ctx) {
			fmt.Printf("Request %d: Allowed\n", i+1)
		} else {
			fmt.Printf("Request %d: Rate limited\n", i+1)
		}
	}

	// Test Wait method
	fmt.Println("\nTesting Wait method...")
	start := time.Now()

	for i := 0; i < 3; i++ {
		if err := limiter.Wait(ctx); err != nil {
			fmt.Printf("Wait error: %v\n", err)
		} else {
			elapsed := time.Since(start)
			fmt.Printf("Request %d processed after %v\n", i+1, elapsed)
		}
	}

	// Example 3: Using rate limit helpers
	fmt.Println("\n=== Rate Limit Helpers Example ===")

	// Create different rate limits
	perSecLimit := llim.PerSecond(10, 20)
	perMinLimit := llim.PerMinute(100, 150)

	limiter2 := llim.NewLimiterFromLimit("api:v2", perSecLimit)
	limiter3 := llim.NewLimiterFromLimit("api:v3", perMinLimit)

	fmt.Printf("Limiter2 (10/sec): %v\n", limiter2.Allow(ctx))
	fmt.Printf("Limiter3 (100/min): %v\n", limiter3.Allow(ctx))
}
