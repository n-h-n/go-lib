package log

import (
	"context"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/n-h-n/go-lib/env"
)

type ScopedLogger struct {
	ShowContext bool
	*zap.SugaredLogger
}

type DynamicLevel struct {
	zapcore.Core
	Levels       map[string]zapcore.Level `mapstructure:"levels"`
	DefaultLevel zap.AtomicLevel          `mapstructure:"default-level"`
}

var (
	Log            = ScopedLogger{}
	LogWithContext = ScopedLogger{}
	levels         = map[string]zapcore.Level{
		"debug": zap.DebugLevel,
		"info":  zap.InfoLevel,
		"warn":  zap.WarnLevel,
		"error": zap.ErrorLevel,
	}
)

func NewEnvLogger(options ...zap.Option) *zap.SugaredLogger {
	cfg := zap.NewProductionConfig()
	if env.E() == env.Local || env.E() == env.Dev {
		cfg = zap.NewDevelopmentConfig()
	}

	// Fix for ISS-8444 - Set the log level to the minimum so that dynamic-logger could work
	cfg.Level.SetLevel(zapcore.DebugLevel)

	logger, initErr := cfg.Build(options...)
	if initErr != nil {
		panic(initErr)
	}
	return logger.Sugar()
}

func Scope(z *zap.SugaredLogger, showContext bool) ScopedLogger {
	return ScopedLogger{showContext, z.Desugar().WithOptions(zap.AddCallerSkip(1)).Sugar()}
}

func NewEnvScopedLogger(showContext bool, options ...zap.Option) ScopedLogger {
	return Scope(NewEnvLogger(options...), showContext)
}

func DynamicLevelOption(dl *DynamicLevel) zap.Option {
	return zap.WrapCore(func(c zapcore.Core) zapcore.Core {
		dl.Core = c
		return dl
	})
}

func (w ScopedLogger) Info(ctx context.Context, args ...any) {
	if w.ShowContext {
		w.SugaredLogger.With("ctx", ctx).Info(args...)
	} else {
		w.SugaredLogger.Info(args...)
	}
}

func (w ScopedLogger) Infof(ctx context.Context, pattern string, args ...any) {
	if w.ShowContext {
		w.SugaredLogger.With("ctx", ctx).Infof(pattern, args...)
	} else {
		w.SugaredLogger.Infof(pattern, args...)
	}
}

func (w ScopedLogger) Debug(ctx context.Context, args ...any) {
	if w.ShowContext {
		w.SugaredLogger.With("ctx", ctx).Debug(args...)
	} else {
		w.SugaredLogger.Debug(args...)
	}
}

func (w ScopedLogger) Debugf(ctx context.Context, pattern string, args ...any) {
	if w.ShowContext {
		w.SugaredLogger.With("ctx", ctx).Debugf(pattern, args...)
	} else {
		w.SugaredLogger.Debugf(pattern, args...)
	}
}

func (w ScopedLogger) Warn(ctx context.Context, args ...any) {
	if w.ShowContext {
		w.SugaredLogger.With("ctx", ctx).Warn(args...)
	} else {
		w.SugaredLogger.Warn(args...)
	}
}

func (w ScopedLogger) Warnf(ctx context.Context, pattern string, args ...any) {
	if w.ShowContext {
		w.SugaredLogger.With("ctx", ctx).Warnf(pattern, args...)
	} else {
		w.SugaredLogger.Warnf(pattern, args...)
	}
}

func (w ScopedLogger) Error(ctx context.Context, args ...any) {
	if w.ShowContext {
		w.SugaredLogger.With("ctx", ctx).Error(args...)
	} else {
		w.SugaredLogger.Error(args...)
	}
}

func (w ScopedLogger) Errorf(ctx context.Context, pattern string, args ...any) {
	if w.ShowContext {
		w.SugaredLogger.With("ctx", ctx).Errorf(pattern, args...)
	} else {
		w.SugaredLogger.Errorf(pattern, args...)
	}
}

func (w ScopedLogger) Fatalf(ctx context.Context, pattern string, args ...any) {
	if w.ShowContext {
		w.SugaredLogger.With("ctx", ctx).Fatalf(pattern, args...)
	} else {
		w.SugaredLogger.Fatalf(pattern, args...)
	}
}

func (w ScopedLogger) Panicf(ctx context.Context, pattern string, args ...any) {
	if w.ShowContext {
		w.SugaredLogger.With("ctx", ctx).Panicf(pattern, args...)
	} else {
		w.SugaredLogger.Panicf(pattern, args...)
	}
}

func init() {
	dl := &DynamicLevel{
		Levels:       levels,
		DefaultLevel: zap.NewAtomicLevelAt(zap.WarnLevel),
	}

	// new scoped logger
	Log = NewEnvScopedLogger(false, DynamicLevelOption(dl))
	LogWithContext = NewEnvScopedLogger(true, DynamicLevelOption(dl))
}
