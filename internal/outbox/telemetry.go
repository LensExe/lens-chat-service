package outbox

import "sync/atomic"

type Telemetry struct {
	attempts        atomic.Int64
	published       atomic.Int64
	publishFailures atomic.Int64
	retries         atomic.Int64
	parked          atomic.Int64
	processingCount atomic.Int64
	processingNanos atomic.Int64
}

func NewTelemetry() *Telemetry { return &Telemetry{} }

func (t *Telemetry) recordAttempt()   { t.attempts.Add(1) }
func (t *Telemetry) recordPublished() { t.published.Add(1) }
func (t *Telemetry) recordFailure()   { t.publishFailures.Add(1) }
func (t *Telemetry) recordRetry()     { t.retries.Add(1) }
func (t *Telemetry) recordParked()    { t.parked.Add(1) }
func (t *Telemetry) recordProcessing(nanos int64) {
	t.processingCount.Add(1)
	t.processingNanos.Add(nanos)
}

type telemetrySnapshot struct {
	attempts        int64
	published       int64
	publishFailures int64
	retries         int64
	parked          int64
	processingCount int64
	processingNanos int64
}

func (t *Telemetry) snapshot() telemetrySnapshot {
	return telemetrySnapshot{
		attempts:        t.attempts.Load(),
		published:       t.published.Load(),
		publishFailures: t.publishFailures.Load(),
		retries:         t.retries.Load(),
		parked:          t.parked.Load(),
		processingCount: t.processingCount.Load(),
		processingNanos: t.processingNanos.Load(),
	}
}
