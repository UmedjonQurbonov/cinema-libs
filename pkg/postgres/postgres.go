package postgres

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// NewPgxPool читает переменные окружения и инициализирует пул соединений PostgreSQL
func NewPgxPool(ctx context.Context, log *zap.Logger) (*pgxpool.Pool, error) {
	// 1. Формируем DSN из переменных окружения (с дефолтными значениями для локалки)
	dsn := getEnv("DATABASE_URL", "")
	if dsn == "" {
		dsn = fmt.Sprintf(
			"postgres://%s:%s@%s:%s/%s?sslmode=%s",
			getEnv("DB_USER", "postgres"),
			getEnv("DB_PASSWORD", "postgres"),
			getEnv("DB_HOST", "localhost"),
			getEnv("DB_PORT", "5432"),
			getEnv("DB_NAME", "cinema_movie"),
			getEnv("DB_SSLMODE", "disable"),
		)
	}

	// 2. Парсим конфиг для pgxpool
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn config: %w", err)
	}

	// 3. Настраиваем пул соединений
	poolConfig.MaxConns = 25
	poolConfig.MinConns = 5
	poolConfig.MaxConnLifetime = time.Hour
	poolConfig.MaxConnIdleTime = 30 * time.Minute

	// 4. Создаем пул
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}

	// 5. Проверяем связь через Ping
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres database: %w", err)
	}

	log.Info("connected to postgresql pool",
		zap.String("host", getEnv("DB_HOST", "localhost")),
		zap.String("database", getEnv("DB_NAME", "cinema_movie")),
	)

	return pool, nil
}

// Вспомогательная функция для безопасного чтения переменных окружения
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		return value
	}
	return fallback
}