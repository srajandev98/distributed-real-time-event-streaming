package logging

import (
	"log/slog"
	"os"
)

// logger is a shared JSON logger used by all modules.
var logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))

// Info writes structured logs for normal runtime events.
func Info(msg string, args ...any) {
	logger.Info(msg, args...)
}

// Warn writes structured logs for recoverable problems.
func Warn(msg string, args ...any) {
	logger.Warn(msg, args...)
}

// Error writes structured logs for failures that need attention.
func Error(msg string, args ...any) {
	logger.Error(msg, args...)
}
