package telemetrywerfio

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

const (
	tableName = `events_new`

	// language=ClickHouse SQL
	createLogsTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
     ts DateTime,
     executionID String,
     userID String,
     projectID String,
     command String,
     attributes JSON,
     eventType String,
     eventData JSON,
     schemaVersion UInt32
) ENGINE MergeTree()
PARTITION BY toDate(ts)
ORDER BY ts;
`
	// language=ClickHouse SQL
	insertSQLTemplate = `INSERT INTO %s (
                        ts,
                		executionID,
                		userID,
                        projectID,
                		command,
                        attributes,
                        eventType,
                        eventData,
                        schemaVersion
                        ) VALUES (
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?
                                  )`

	// language=ClickHouse SQL
	setExperimentalModeSQL = "SET allow_experimental_object_type = 1"
)

type clickhouseExporter struct {
	conn      driver.Conn
	logger    *zap.Logger
	cfg       *Config
	insertSQL string
}

func newExporter(logger *zap.Logger, cfg *Config) (*clickhouseExporter, error) {
	ctx := context.Background()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	conn, err := newClickhouseConn(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to create clickhouse connection: %w", err)
	}

	for i := 0; i < 5; i++ {
		err = conn.Ping(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Waiting for clickhouse start: %s\n", err)
			time.Sleep(2 * time.Second)
			continue
		}
	}
	if err != nil {
		return nil, fmt.Errorf("timed out waiting for clickhouse startup: %w", err)
	}

	if err := conn.Exec(ctx, setExperimentalModeSQL); err != nil {
		return nil, fmt.Errorf("unable to set experimental object type: %w", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(createLogsTableSQL, tableName)); err != nil {
		return nil, fmt.Errorf("exec create table sql: %w", err)
	}

	return &clickhouseExporter{
		conn:      conn,
		logger:    logger,
		cfg:       cfg,
		insertSQL: fmt.Sprintf(insertSQLTemplate, tableName),
	}, nil
}

func newClickhouseConn(cfg *Config) (driver.Conn, error) {
	clickhouseOptions, err := clickhouse.ParseDSN(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("unable to parse dsn: %w", err)
	}
	conn, err := clickhouse.Open(clickhouseOptions)
	if err != nil {
		return nil, fmt.Errorf("unable to open clickhouse connection: %w", err)
	}
	return conn, nil
}

func (e *clickhouseExporter) Shutdown(_ context.Context) error {
	if e.conn != nil {
		return e.conn.Close()
	}
	return nil
}

func (e *clickhouseExporter) handleWerfTelemetry(ctx context.Context, td ptrace.Traces) error {
	fmt.Printf("clickhouseExporter.handleWerfTelemetry: received %d spans\n", td.SpanCount())

	start := time.Now()
	defer func() {
		duration := time.Since(start)
		e.logger.Debug("insert werf events",
			zap.Int("records", td.SpanCount()),
			zap.String("cost", duration.String()),
		)
	}()

	batch, err := e.conn.PrepareBatch(ctx, e.insertSQL)
	if err != nil {
		return fmt.Errorf("unable to prepare batch: %w", err)
	}

	resourceSpans := td.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		rs := resourceSpans.At(i)
		ilss := rs.ScopeSpans()

		for i := 0; i < ilss.Len(); i++ {
			ils := ilss.At(i)
			spans := ils.Spans()
			for j := 0; j < spans.Len(); j++ {
				span := spans.At(j)
				attrs := span.Attributes()

				if span.Name() != "telemetry.werf.io" {
					fmt.Printf("clickhouseExporter.handleWerfTelemetry: ignore span named %q\n", span.Name())
					continue
				}

				fmt.Printf("clickhouseExporter.handleWerfTelemetry: Received telemetry.werf.io attributes: %#v\n", attrs.AsRaw())

				tsVal, _ := attrs.Get("ts")
				ts := time.Unix(tsVal.IntVal()/1000, 1000_000*(tsVal.IntVal()%1000))

				executionIDVal, _ := attrs.Get("executionID")
				executionID := executionIDVal.StringVal()

				userIDVal, _ := attrs.Get("userID")
				userID := userIDVal.StringVal()

				projectIDVal, _ := attrs.Get("projectID")
				projectID := projectIDVal.StringVal()

				commandVal, _ := attrs.Get("command")
				command := commandVal.StringVal()

				attributesVal, _ := attrs.Get("attributes")
				attributes := attributesVal.StringVal()

				eventTypeVal, _ := attrs.Get("eventType")
				eventType := eventTypeVal.StringVal()

				eventDataVal, _ := attrs.Get("eventData")
				eventData := eventDataVal.StringVal()

				schemaVersionVal, _ := attrs.Get("schemaVersion")
				schemaVersion := schemaVersionVal.IntVal()

				if schemaVersion != 2 {
					fmt.Printf("clickhouseExporter.handleWerfTelemetry: ignore unsupported schemaVersion %d\n", schemaVersion)
					continue
				}

				if err := batch.Append(
					ts,
					executionID,
					userID,
					projectID,
					command,
					attributes,
					eventType,
					eventData,
					uint32(schemaVersion),
				); err != nil {
					return fmt.Errorf("unable to append batch data: %w", err)
				}
			}
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("unable to send clickhouse batch: %w", err)
	}

	return err
}
