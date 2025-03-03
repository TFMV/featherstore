package logger

import (
	"fmt"
	"os"

	"github.com/TFMV/featherstore/pkg/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger wraps zap logger
type Logger struct {
	*zap.Logger
}

var (
	// Global logger instance
	globalLogger *Logger
)

// Initialize sets up the global logger with the provided configuration
func Initialize(cfg *config.LoggingConfig) (*Logger, error) {
	// Configure logging level
	level, err := zapcore.ParseLevel(cfg.Level)
	if err != nil {
		return nil, fmt.Errorf("invalid logging level: %w", err)
	}

	// Configure encoder
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	var encoder zapcore.Encoder
	if cfg.Format == "json" {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	} else {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}

	// Configure output
	var output zapcore.WriteSyncer
	switch cfg.OutputPath {
	case "stdout":
		output = zapcore.AddSync(os.Stdout)
	case "stderr":
		output = zapcore.AddSync(os.Stderr)
	default:
		file, err := os.OpenFile(cfg.OutputPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("could not open log file: %w", err)
		}
		output = zapcore.AddSync(file)
	}

	core := zapcore.NewCore(encoder, output, zap.NewAtomicLevelAt(level))
	logger := &Logger{
		Logger: zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel)),
	}

	globalLogger = logger
	return logger, nil
}

// NewContext creates a contextual logger with additional fields
func (l *Logger) NewContext(fields ...zap.Field) *Logger {
	return &Logger{
		Logger: l.With(fields...),
	}
}

// Global returns the global logger instance
func Global() *Logger {
	if globalLogger == nil {
		// Create a default logger if not initialized
		defaultLogger, _ := zap.NewProduction()
		globalLogger = &Logger{Logger: defaultLogger}
	}
	return globalLogger
}

// Info logs info level message with additional fields
func (l *Logger) Info(msg string, fields ...zap.Field) {
	l.Logger.Info(msg, fields...)
}

// Debug logs debug level message with additional fields
func (l *Logger) Debug(msg string, fields ...zap.Field) {
	l.Logger.Debug(msg, fields...)
}

// Error logs error level message with additional fields
func (l *Logger) Error(msg string, fields ...zap.Field) {
	l.Logger.Error(msg, fields...)
}

// Warn logs warning level message with additional fields
func (l *Logger) Warn(msg string, fields ...zap.Field) {
	l.Logger.Warn(msg, fields...)
}

// Fatal logs fatal level message with additional fields and exits
func (l *Logger) Fatal(msg string, fields ...zap.Field) {
	l.Logger.Fatal(msg, fields...)
}

// Close flushes any buffered log entries
func (l *Logger) Close() error {
	return l.Logger.Sync()
}
