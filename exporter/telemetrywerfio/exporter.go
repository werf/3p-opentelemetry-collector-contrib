package telemetrywerfio

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"

	_ "github.com/ClickHouse/clickhouse-go" // To register database driver
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
)

type clickhouseExporter struct {
	client    *sql.DB
	insertSQL string

	logger *zap.Logger
	cfg    *Config
}

func newExporter(logger *zap.Logger, cfg *Config) (*clickhouseExporter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	var client *sql.DB
	{
		var err error
		for i := 0; i < 5; i++ {
			client, err = newClickhouseClient(cfg)
			if err == nil {
				break
			}

			time.Sleep(2 * time.Second)
		}

		if err != nil {
			return nil, err
		}
	}

	insertSQL := renderinsertSQL(cfg)

	return &clickhouseExporter{
		client:    client,
		insertSQL: insertSQL,
		logger:    logger,
		cfg:       cfg,
	}, nil
}

// Shutdown will shutdown the exporter.
func (e *clickhouseExporter) Shutdown(_ context.Context) error {
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

func (e *clickhouseExporter) handleWerfTelemetry(ctx context.Context, td ptrace.Traces) error {
	fmt.Printf("RECEIVED SPANS: %d\n", td.SpanCount())

	start := time.Now()
	err := doWithTx(ctx, e.client, func(tx *sql.Tx) error {
		statement, err := tx.PrepareContext(ctx, e.insertSQL)
		if err != nil {
			return fmt.Errorf("PrepareContext:%w", err)
		}
		defer func() {
			_ = statement.Close()
		}()

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

					fmt.Printf("RECEIVED ATTRIBUTES: %#v\n", attrs.AsRaw())

					tsVal, _ := attrs.Get("ts")
					ts := time.Unix(tsVal.IntVal()/1000, 1000_000*(tsVal.IntVal()%1000))

					executionIDVal, _ := attrs.Get("executionID")
					executionID := executionIDVal.StringVal()

					projectIDVal, _ := attrs.Get("projectID")
					projectID := projectIDVal.StringVal()

					commandVal, _ := attrs.Get("command")
					command := commandVal.StringVal()

					eventTypeVal, _ := attrs.Get("eventType")
					eventType := eventTypeVal.StringVal()

					dataVal, _ := attrs.Get("data")
					data := dataVal.StringVal()

					if _, err = statement.ExecContext(ctx, ts, executionID, projectID, command, eventType, data); err != nil {
						return fmt.Errorf("ExecContext:%w", err)
					}
				}
			}
		}

		return nil
	})

	duration := time.Since(start)

	e.logger.Debug("insert werf events", zap.Int("records", td.SpanCount()),
		zap.String("cost", duration.String()))

	return err
}

func attributesToSlice(attributes pcommon.Map) ([]string, []string) {
	keys := make([]string, 0, attributes.Len())
	values := make([]string, 0, attributes.Len())
	attributes.Range(func(k string, v pcommon.Value) bool {
		keys = append(keys, formatKey(k))
		values = append(values, v.AsString())
		return true
	})
	return keys, values
}

func formatKey(k string) string {
	return strings.ReplaceAll(k, ".", "_")
}

const (
	tableName = `werf_telemetry_events`

	// language=ClickHouse SQL
	createLogsTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
     ts DateTime CODEC(Delta, ZSTD(1)),
     executionID String CODEC(ZSTD(1)),
     projectID String CODEC(ZSTD(1)),
     command String CODEC(ZSTD(1)),
     eventType String CODEC(ZSTD(1)),
     data String  CODEC(ZSTD(1))
) ENGINE MergeTree()
%s
PARTITION BY toDate(ts)
ORDER BY (toUnixTimestamp(ts));
`
	// language=ClickHouse SQL
	insertSQLTemplate = `INSERT INTO %s (
                        ts,
                		executionID,
                        projectID,
                		command,
                		eventType,
                		data
                        ) VALUES (
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?
                                  )`
)

var driverName = "clickhouse"

func newClickhouseClient(cfg *Config) (*sql.DB, error) {
	// use empty database to create database
	db, err := sql.Open(driverName, cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("sql.Open:%w", err)
	}
	// create table
	query := fmt.Sprintf(createLogsTableSQL, tableName, "")

	if _, err := db.Exec(query); err != nil {
		return nil, fmt.Errorf("exec create table sql: %w", err)
	}
	return db, nil
}

func renderinsertSQL(cfg *Config) string {
	return fmt.Sprintf(insertSQLTemplate, tableName)
}

func doWithTx(_ context.Context, db *sql.DB, fn func(tx *sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("db.Begin: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
