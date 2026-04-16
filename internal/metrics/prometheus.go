package metrics

import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

var (
    UpstreamSessionState = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "fix_gateway_upstream_session_state",
        Help: "Current state of the upstream FIX session (1=Active, 0=Down)",
    })

    DownstreamSessions = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "fix_gateway_downstream_sessions_active",
        Help: "Number of active downstream client sessions",
    })

    TicksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "fix_gateway_upstream_ticks_total",
        Help: "Total number of market data ticks received from upstream",
    }, []string{"symbol"})

    FanoutTotal = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "fix_gateway_fanout_messages_total",
        Help: "Total number of market data messages fanned out to clients",
    }, []string{"symbol"})

    SlowClientDrops = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "fix_gateway_slow_client_drops_total",
        Help: "Total number of messages dropped due to slow client buffers",
    }, []string{"client_id"})
)
