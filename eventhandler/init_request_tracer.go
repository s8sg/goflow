package eventhandler

import (
	"fmt"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
)

// initRequestTracer init global trace with configuration
func initRequestTracer(flowName, traceURI string) (*TraceHandler, error) {
	tracerObj := &TraceHandler{}

	agentPort := traceURI

	cfg := config.Configuration{
		ServiceName: flowName,
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		Reporter: &config.ReporterConfig{
			LogSpans:            true,
			BufferFlushInterval: 1 * time.Second,
			LocalAgentHostPort:  agentPort,
		},
	}

	opentracer, traceCloser, err := cfg.NewTracer(
		config.Logger(jaeger.StdLogger),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to init Tracer, error %v", err.Error())
	}

	tracerObj.closer = traceCloser
	tracerObj.tracer = opentracer
	tracerObj.nodeSpans = make(map[string]opentracing.Span)
	tracerObj.operationSpans = make(map[string]map[string]opentracing.Span)

	return tracerObj, nil
}
