package rlim

import (
	"time"

	"github.com/go-redis/redis_rate/v10"
)

func PerSecond(rate, burst int, opts ...func(*redis_rate.Limit)) redis_rate.Limit {
	l := redis_rate.Limit{
		Rate:   rate,
		Period: time.Second,
		Burst:  burst,
	}

	for _, opt := range opts {
		opt(&l)
	}

	return l
}

func PerMinute(rate, burst int) redis_rate.Limit {
	return redis_rate.Limit{
		Rate:   rate,
		Period: time.Minute,
		Burst:  burst,
	}
}

func PerHour(rate, burst int) redis_rate.Limit {
	return redis_rate.Limit{
		Rate:   rate,
		Period: time.Hour,
		Burst:  burst,
	}
}

func PerDay(rate, burst int) redis_rate.Limit {
	return redis_rate.Limit{
		Rate:   rate,
		Period: 24 * time.Hour,
		Burst:  burst,
	}
}

func PerMonth(rate, burst int) redis_rate.Limit {
	return redis_rate.Limit{
		Rate:   rate,
		Period: 31 * 24 * time.Hour,
		Burst:  burst,
	}
}

func WithPeriod(period time.Duration) func(*redis_rate.Limit) {
	return func(o *redis_rate.Limit) {
		o.Period = period
	}
}
