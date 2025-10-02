package llim

import "time"

// Limit represents a rate limit configuration
type Limit struct {
	Rate   int           // requests per period
	Period time.Duration // time period
	Burst  int           // burst capacity
}

// Helper functions to create common rate limits
func PerSecond(rate, burst int, opts ...func(*Limit)) Limit {
	l := Limit{
		Rate:   rate,
		Period: time.Second,
		Burst:  burst,
	}

	for _, opt := range opts {
		opt(&l)
	}

	return l
}

func PerMinute(rate, burst int) Limit {
	return Limit{
		Rate:   rate,
		Period: time.Minute,
		Burst:  burst,
	}
}

func PerHour(rate, burst int) Limit {
	return Limit{
		Rate:   rate,
		Period: time.Hour,
		Burst:  burst,
	}
}

func PerDay(rate, burst int) Limit {
	return Limit{
		Rate:   rate,
		Period: 24 * time.Hour,
		Burst:  burst,
	}
}

func PerMonth(rate, burst int) Limit {
	return Limit{
		Rate:   rate,
		Period: 31 * 24 * time.Hour,
		Burst:  burst,
	}
}

func WithPeriod(period time.Duration) func(*Limit) {
	return func(l *Limit) {
		l.Period = period
	}
}

// Create limiter from Limit config
func NewLimiterFromLimit(keyspace string, limit Limit) Limiter {
	return NewLimiter(WithLocalLimiter(keyspace, limit.Rate, limit.Period, limit.Burst))
}
