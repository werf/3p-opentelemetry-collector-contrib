package telemetrywerfio

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	// The value of "type" key in configuration
	typeStr = "telemetrywerfio"
)

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		typeStr,
		createDefaultConfig,
		exporter.WithTraces(createExporter, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		TimeoutSettings: exporterhelper.NewDefaultTimeoutSettings(),
		QueueSettings:   QueueSettings{QueueSize: exporterhelper.NewDefaultQueueSettings().QueueSize},
		RetrySettings:   exporterhelper.NewDefaultRetrySettings(),
	}
}

func createExporter(
	ctx context.Context,
	set exporter.CreateSettings,
	cfg component.Config,
) (exporter.Traces, error) {
	c := cfg.(*Config)
	exporter, err := newExporter(set.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure telemetrywerfo exporter: %w", err)
	}

	return exporterhelper.NewTracesExporter(
		ctx,
		set,
		cfg,
		exporter.handleWerfTelemetry,
		exporterhelper.WithShutdown(exporter.Shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.enforcedQueueSettings()),
		exporterhelper.WithRetry(c.RetrySettings),
	)
}
