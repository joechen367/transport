package asynq

import (
	log "log/slog"

	"github.com/hibiken/asynq"
)

type logger struct {
}

func newLogger() asynq.Logger {
	return &logger{}
}

func (l logger) Debug(args ...interface{}) {
	log.Default().Debug("server [asynq] Debug:", args...)
}

func (l logger) Info(args ...interface{}) {
	log.Default().Info("server [asynq] Info:", args...)
}

func (l logger) Warn(args ...interface{}) {
	log.Default().Warn("server [asynq] Warn:", args...)
}

func (l logger) Error(args ...interface{}) {
	log.Default().Error("server [asynq] Error:", args...)
}

func (l logger) Fatal(args ...interface{}) {
	log.Default().Error("server [asynq] Fatal:", args...)
}
