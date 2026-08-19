package awslimit

import (
	"context"
	"fmt"

	"github.com/aws/smithy-go/middleware"
)

const middlewareID = "GoLibAWSRateLimit"

// StackOption returns an AWS SDK v2 API option that waits on rate before each
// operation. Attach it to a service client's Options.APIOptions, not a shared
// aws.Config used by more than one service.
func StackOption(rate RateConfig, opts ...Opt) func(*middleware.Stack) error {
	lim := NewLimiter(rate, opts...)
	return func(stack *middleware.Stack) error {
		return stack.Initialize.Add(
			middleware.InitializeMiddlewareFunc(middlewareID, func(
				ctx context.Context,
				in middleware.InitializeInput,
				next middleware.InitializeHandler,
			) (middleware.InitializeOutput, middleware.Metadata, error) {
				if err := Wait(ctx, lim); err != nil {
					return middleware.InitializeOutput{}, middleware.Metadata{}, err
				}
				return next.HandleInitialize(ctx, in)
			}),
			middleware.Before,
		)
	}
}

// Wait consumes one token, blocking if the bucket is empty.
func Wait(ctx context.Context, lim limiter) error {
	if lim == nil {
		return nil
	}
	if lim.Allow(ctx) {
		return nil
	}
	if err := lim.Wait(ctx); err != nil {
		return fmt.Errorf("aws: rate limit: %w", err)
	}
	return nil
}
