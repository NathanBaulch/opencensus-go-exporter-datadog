// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2018 Datadog, Inc.

package main

import (
	"context"
	"log"
	"time"

	datadog "github.com/DataDog/opencensus-go-exporter-datadog"
	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/label"
	"go.opentelemetry.io/otel/sdk/trace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
)

func main() {
	exporter, err := datadog.NewExporter(datadog.Options{Service: "my-app"})
	if err != nil {
		log.Fatal(err)
	}
	defer exporter.Stop()

	batcher := trace.WithBatcher(exporter)
	tp, err := trace.NewProvider(batcher)
	if err != nil {
		log.Fatal(err)
	}
	global.SetTraceProvider(tp)

	ctx, span := global.Tracer("example").Start(context.Background(), "/foo")
	time.Sleep(100*time.Millisecond)
	bar(ctx)
	time.Sleep(100*time.Millisecond)
	span.End()

	// Give the OTEL exporter time to flush
	time.Sleep(5*time.Second)
}

func bar(ctx context.Context) {
	ctx, span := global.Tracer("example").Start(ctx, "/bar")
	defer span.End()

	// Do bar...
	time.Sleep(100*time.Millisecond)

	// Set Datadog APM Trace Metadata
	span.SetAttributes(
		label.String(ext.ResourceName, "/foo/bar"),
		label.String(ext.SpanType, ext.SpanTypeWeb),
	)
}
