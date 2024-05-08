package internal

import (
	"context"

	"github.com/InfluxCommunity/flux"
	"github.com/InfluxCommunity/flux/memory"
	"github.com/InfluxCommunity/flux/runtime"
	"github.com/prometheus/client_golang/prometheus"
)

type FluxControllerMock struct {
	QueryFn func(ctx context.Context, compiler flux.Compiler) (flux.Query, error)
}

func NewFluxControllerMock() *FluxControllerMock {
	return &FluxControllerMock{
		QueryFn: func(ctx context.Context, compiler flux.Compiler) (query flux.Query, e error) {
			p, err := compiler.Compile(ctx, runtime.Default)
			if err != nil {
				return nil, err
			}
			alloc := &memory.ResourceAllocator{}
			return p.Start(ctx, alloc)
		},
	}
}

func (m *FluxControllerMock) Query(ctx context.Context, compiler flux.Compiler) (flux.Query, error) {
	return m.QueryFn(ctx, compiler)
}

func (m *FluxControllerMock) PrometheusCollectors() []prometheus.Collector { return nil }
