package metric

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/sdk/metric"

	"github.com/litesql/ha/internal/o11y"
)

func Init(ctx context.Context, nodeName string, enableOtel bool) (func(context.Context) error, error) {
	var reader metric.Reader

	if !enableOtel {
		//noop
		return func(_ context.Context) error {
			return nil
		}, nil
	}
	metricExporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, err
	}
	reader = metric.NewPeriodicReader(
		metricExporter,
		metric.WithInterval(5*time.Second),
	)

	mp := metric.NewMeterProvider(
		metric.WithResource(o11y.Resource(nodeName)),
		metric.WithReader(reader))
	otel.SetMeterProvider(mp)

	return mp.Shutdown, nil
}
