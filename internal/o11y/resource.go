package o11y

import (
	"sync"

	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

const (
	serviceName = "ha"
)

var (
	version = "dev"
	res     *resource.Resource
	once    sync.Once
)

func SetVersion(v string) {
	if v != "" {
		version = v
	}
}

func Resource(nodeName string) *resource.Resource {
	once.Do(func() {
		attr := append(resource.Default().Attributes(),
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(version),
			semconv.ServiceInstanceID(nodeName))
		res = resource.NewWithAttributes(semconv.SchemaURL,
			attr...,
		)
	})
	return res
}
