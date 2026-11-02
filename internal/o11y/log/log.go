package log

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/sdk/log"

	"github.com/litesql/ha/internal/o11y"
)

type flusher func(context.Context) error

func Init(ctx context.Context, level slog.Level, enableOtel bool) (flusher, error) {
	opts := slog.HandlerOptions{
		AddSource: true,
		Level:     level,
	}
	var handlers []slog.Handler

	handlers = append(handlers, slog.NewJSONHandler(os.Stderr, &opts))

	var loggerProvider *log.LoggerProvider
	if enableOtel {
		var err error
		loggerProvider, err = newLoggerProvider(ctx)
		if err != nil {
			return nil, err
		}
		otel.SetLoggerProvider(loggerProvider)
		handlers = append(handlers, otelslog.NewHandler(""))
	}

	var flush flusher = func(ctx context.Context) error {
		if loggerProvider != nil {
			return loggerProvider.Shutdown(ctx)
		}
		return nil
	}

	var handler slog.Handler
	if len(handlers) > 0 {
		handler = slog.NewMultiHandler(handlers...)
	} else {
		handler = handlers[0]
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)

	return flush, nil
}

func newLoggerProvider(ctx context.Context) (*log.LoggerProvider, error) {
	exporter, err := otlploghttp.New(ctx)
	if err != nil {
		return nil, err
	}
	processor := log.NewBatchProcessor(exporter)
	loggerProvider := log.NewLoggerProvider(
		log.WithResource(o11y.Resource()),
		log.WithProcessor(processor),
	)
	return loggerProvider, nil
}
