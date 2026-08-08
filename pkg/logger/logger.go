package logger


import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

type Task struct {
	ID      string
	Type    string
	Payload string
}

type Worker struct {
	logger *zap.Logger
	tasks  <-chan Task
}

func NewWorker(logger *zap.Logger, tasks <-chan Task) *Worker {
	return &Worker{
		// Добавляем к логгеру имя компонента, чтобы все логи воркера помечались автоматически
		logger: logger.With(zap.String("component", "order_worker")),
		tasks:  tasks,
	}
}

// Run запускает воркер с поддержкой Graceful Shutdown
func (w *Worker) Run(ctx context.Context) {
	w.logger.Info("background worker started")

	for {
		select {
		case <-ctx.Done(): // Сигнал от контекста приложения на остановку (SIGINT/SIGTERM)
			w.logger.Info("shutting down worker gracefully...")
			return

		case task, ok := <-w.tasks:
			if !ok {
				w.logger.Info("task channel closed, stopping worker")
				return
			}

			// Обрабатываем каждую задачу отдельно
			w.processTask(ctx, task)
		}
	}
}

func (w *Worker) processTask(parentCtx context.Context, task Task) {
	// 1. Задаем жесткий таймаут выполнения для одной задачи
	ctx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
	defer cancel()

	// 2. Создаем "дочерний" логгер с контекстом конкретной задачи.
	// Все последующие логи в этом методе автоматически будут содержать task_id и task_type!
	taskLogger := w.logger.With(
		zap.String("task_id", task.ID),
		zap.String("task_type", task.Type),
	)

	// 3. Защита от паник (Recover)
	defer func() {
		if r := recover(); r != nil {
			taskLogger.Error("panic recovered during task execution",
				zap.Any("panic_reason", r),
				zap.Stack("stacktrace"), // Zap умеет сам красиво дампить стек вызовов!
			)
		}
	}()

	taskLogger.Debug("started processing task")
	startTime := time.Now()

	// 4. Вызов бизнес-логики
	if err := w.executeBusinessLogic(ctx, task); err != nil {
		taskLogger.Error("task processing failed",
			zap.Error(err), // zap.Error сам вытащит err.Error()
			zap.Duration("elapsed", time.Since(startTime)),
		)
		// Здесь можно отправить таску в Dead Letter Queue (DLQ) или на ретрай
		return
	}

	taskLogger.Info("task completed successfully",
		zap.Duration("elapsed", time.Since(startTime)),
	)
}

func (w *Worker) executeBusinessLogic(ctx context.Context, task Task) error {
	// Симуляция работы
	select {
	case <-time.After(150 * time.Millisecond):
		if task.Payload == "invalid" {
			return fmt.Errorf("invalid payload format")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err() // Завершаем, если вышел таймаут
	}
}