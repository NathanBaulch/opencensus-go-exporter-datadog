// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadog.com/).
// Copyright 2018 Datadog, Inc.

package datadog

import (
	"encoding/binary"
	"fmt"
	"net/http"
	"strconv"

	"go.opentelemetry.io/otel/api/trace"
	"go.opentelemetry.io/otel/label"
	export "go.opentelemetry.io/otel/sdk/export/trace"
	"google.golang.org/grpc/codes"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
)

// statusCodes maps (*trace.SpanData).Status.Code to their message and http status code. See:
// https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto.
var statusCodes = map[codes.Code]codeDetails{
	codes.OK:                 {message: "OK", status: http.StatusOK},
	codes.Canceled:           {message: "CANCELLED", status: 499},
	codes.Unknown:            {message: "UNKNOWN", status: http.StatusInternalServerError},
	codes.InvalidArgument:    {message: "INVALID_ARGUMENT", status: http.StatusBadRequest},
	codes.DeadlineExceeded:   {message: "DEADLINE_EXCEEDED", status: http.StatusGatewayTimeout},
	codes.NotFound:           {message: "NOT_FOUND", status: http.StatusNotFound},
	codes.AlreadyExists:      {message: "ALREADY_EXISTS", status: http.StatusConflict},
	codes.PermissionDenied:   {message: "PERMISSION_DENIED", status: http.StatusForbidden},
	codes.ResourceExhausted:  {message: "RESOURCE_EXHAUSTED", status: http.StatusTooManyRequests},
	codes.FailedPrecondition: {message: "FAILED_PRECONDITION", status: http.StatusBadRequest},
	codes.Aborted:            {message: "ABORTED", status: http.StatusConflict},
	codes.OutOfRange:         {message: "OUT_OF_RANGE", status: http.StatusBadRequest},
	codes.Unimplemented:      {message: "UNIMPLEMENTED", status: http.StatusNotImplemented},
	codes.Internal:           {message: "INTERNAL", status: http.StatusInternalServerError},
	codes.Unavailable:        {message: "UNAVAILABLE", status: http.StatusServiceUnavailable},
	codes.DataLoss:           {message: "DATA_LOSS", status: http.StatusNotImplemented},
	codes.Unauthenticated:    {message: "UNAUTHENTICATED", status: http.StatusUnauthorized},
}

// codeDetails specifies information about a trace status code.
type codeDetails struct {
	message string // status message
	status  int    // corresponding HTTP status code
}

// convertSpan takes an OpenTelemetry span and returns a Datadog span.
func (e *traceExporter) convertSpan(s *export.SpanData) *ddSpan {
	startNano := s.StartTime.UnixNano()
	span := &ddSpan{
		TraceID:  binary.BigEndian.Uint64(s.SpanContext.TraceID[8:]),
		SpanID:   binary.BigEndian.Uint64(s.SpanContext.SpanID[:]),
		Name:     "opentelemetry",
		Resource: s.Name,
		Service:  e.opts.Service,
		Start:    startNano,
		Duration: s.EndTime.UnixNano() - startNano,
		Metrics:  map[string]float64{},
		Meta:     map[string]string{},
	}
	if s.ParentSpanID.IsValid() {
		span.ParentID = binary.BigEndian.Uint64(s.ParentSpanID[:])
	}

	code, ok := statusCodes[s.StatusCode]
	if !ok {
		code = codeDetails{
			message: "ERR_CODE_" + strconv.FormatInt(int64(s.StatusCode), 10),
			status:  http.StatusInternalServerError,
		}
	}

	switch s.SpanKind {
	case trace.SpanKindClient:
		span.Type = "client"
		if code.status/100 == 4 {
			span.Error = 1
		}
	case trace.SpanKindServer:
		span.Type = "server"
		fallthrough
	default:
		if code.status/100 == 5 {
			span.Error = 1
		}
	}

	if span.Error == 1 {
		span.Meta[ext.ErrorType] = code.message
		if msg := s.StatusMessage; msg != "" {
			span.Meta[ext.ErrorMsg] = msg
		}
	}

	span.Meta[keyStatusCode] = strconv.Itoa(int(s.StatusCode))
	span.Meta[keyStatus] = code.message
	if msg := s.StatusMessage; msg != "" {
		span.Meta[keyStatusDescription] = msg
	}

	for _, attr := range e.opts.GlobalTags {
		setTag(span, string(attr.Key), attr.Value)
	}
	for _, attr := range s.Attributes {
		setTag(span, string(attr.Key), attr.Value)
	}
	return span
}

const (
	keySamplingPriority     = "_sampling_priority_v1"
	keyStatusDescription    = "opentelemetry.status_description"
	keyStatusCode           = "opentelemetry.status_code"
	keyStatus               = "opentelemetry.status"
	keySpanName             = "span.name"
	keySamplingPriorityRate = "_sampling_priority_rate_v1"
)

func setTag(s *ddSpan, key string, val label.Value) {
	if key == ext.Error {
		setError(s, val)
		return
	}
	switch val.Type() {
	case label.STRING:
		setStringTag(s, key, val.AsString())
	case label.BOOL:
		if val.AsBool() {
			setStringTag(s, key, "true")
		} else {
			setStringTag(s, key, "false")
		}
	case label.FLOAT32:
		setMetric(s, key, float64(val.AsFloat32()))
	case label.FLOAT64:
		setMetric(s, key, val.AsFloat64())
	case label.INT32:
		setMetric(s, key, float64(val.AsInt32()))
	case label.INT64:
		setMetric(s, key, float64(val.AsInt64()))
	case label.UINT32:
		setMetric(s, key, float64(val.AsUint32()))
	case label.UINT64:
		setMetric(s, key, float64(val.AsUint64()))
	case label.ARRAY:
		// should never happen according to docs, nevertheless
		// we should account for this to avoid exceptions
		setStringTag(s, key, fmt.Sprintf("%v", val.AsArray()))
	}
}

func setMetric(s *ddSpan, key string, v float64) {
	switch key {
	case ext.SamplingPriority:
		s.Metrics[keySamplingPriority] = v
	default:
		s.Metrics[key] = v
	}
}

func setStringTag(s *ddSpan, key, v string) {
	switch key {
	case ext.ServiceName:
		s.Service = v
	case ext.ResourceName:
		s.Resource = v
	case ext.SpanType:
		s.Type = v
	case ext.AnalyticsEvent:
		if v != "false" {
			setMetric(s, ext.EventSampleRate, 1)
		} else {
			setMetric(s, ext.EventSampleRate, 0)
		}
	case keySpanName:
		s.Name = v
	default:
		s.Meta[key] = v
	}
}

func setError(s *ddSpan, val label.Value) {
	switch val.Type() {
	case label.STRING:
		s.Error = 1
		s.Meta[ext.ErrorMsg] = val.AsString()
	case label.BOOL:
		if val.AsBool() {
			s.Error = 1
		} else {
			s.Error = 0
		}
	case label.INT32:
		if val.AsInt32() > 0 {
			s.Error = 1
		} else {
			s.Error = 0
		}
	case label.INT64:
		if val.AsInt64() > 0 {
			s.Error = 1
		} else {
			s.Error = 0
		}
	case label.UINT32:
		if val.AsUint32() > 0 {
			s.Error = 1
		} else {
			s.Error = 0
		}
	case label.UINT64:
		if val.AsUint64() > 0 {
			s.Error = 1
		} else {
			s.Error = 0
		}
	case label.INVALID:
		s.Error = 0
	default:
		s.Error = 1
	}
}
