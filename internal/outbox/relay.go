package outbox

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"go-app/internal/schema"
)

const (
	maxPublishAttempts     = 3
	maxPendingChannelHeads = 1000
	maxParallelPublishes   = 16
)

type Relay struct {
	repository     *Repository
	publisher      Publisher
	pollInterval   time.Duration
	leaseDuration  time.Duration
	publishTimeout time.Duration
	telemetry      *Telemetry
}

func NewRelay(repository *Repository, publisher Publisher, pollInterval, leaseDuration, publishTimeout time.Duration, telemetry *Telemetry) *Relay {
	return &Relay{
		repository:     repository,
		publisher:      publisher,
		pollInterval:   pollInterval,
		leaseDuration:  leaseDuration,
		publishTimeout: publishTimeout,
		telemetry:      telemetry,
	}
}

func (r *Relay) Run(ctx context.Context) {
	owner := uuid.NewString()
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := r.repository.ReleaseRelayLease(releaseCtx, owner); err != nil {
			slog.Warn("release outbox relay lease failed", "error", err)
		}
	}()

	for ctx.Err() == nil {
		now := time.Now().UTC()
		leader, err := r.repository.AcquireRelayLease(ctx, owner, now, r.leaseDuration)
		if err != nil {
			slog.Warn("acquire outbox relay lease failed", "error", err)
			if !wait(ctx, r.pollInterval) {
				return
			}
			continue
		}
		if !leader {
			if !wait(ctx, r.pollInterval) {
				return
			}
			continue
		}

		events, err := r.repository.PendingChannelHeads(ctx, maxPendingChannelHeads)
		if err != nil {
			slog.Warn("read pending outbox channel heads failed", "error", err)
			if !wait(ctx, r.pollInterval) {
				return
			}
			continue
		}
		if len(events) == 0 {
			if !wait(ctx, r.pollInterval) {
				return
			}
			continue
		}

		now = time.Now().UTC()
		dueEvents := make([]*schema.OutboxEvent, 0, maxParallelPublishes)
		var nextAttempt time.Time
		for i := range events {
			candidate := &events[i]
			if !candidate.NextAttemptAt.After(now) {
				if len(dueEvents) < maxParallelPublishes {
					dueEvents = append(dueEvents, candidate)
				}
				continue
			}
			if nextAttempt.IsZero() || candidate.NextAttemptAt.Before(nextAttempt) {
				nextAttempt = candidate.NextAttemptAt
			}
		}
		if len(dueEvents) == 0 {
			delay := r.pollInterval
			if !nextAttempt.IsZero() && time.Until(nextAttempt) < delay {
				delay = time.Until(nextAttempt)
			}
			if delay < 0 {
				delay = 0
			}
			if !wait(ctx, delay) {
				return
			}
			continue
		}
		r.publishChannelHeads(ctx, dueEvents)
	}
}

func (r *Relay) publishChannelHeads(ctx context.Context, events []*schema.OutboxEvent) {
	var workers sync.WaitGroup
	for _, event := range events {
		event := event
		workers.Add(1)
		go func() {
			defer workers.Done()
			startedAt := time.Now()
			if r.telemetry != nil {
				r.telemetry.recordAttempt()
			}
			publishCtx, cancel := context.WithTimeout(ctx, r.publishTimeout)
			err := r.publisher.Publish(publishCtx, event.PartitionKey, event.Event)
			cancel()
			if err != nil {
				if r.telemetry != nil {
					r.telemetry.recordFailure()
					r.telemetry.recordProcessing(time.Since(startedAt).Nanoseconds())
				}
				attempt := event.AttemptCount + 1
				if attempt >= maxPublishAttempts {
					if stateErr := r.repository.MarkFailed(ctx, event.ID, time.Now().UTC(), attempt, err); stateErr != nil {
						slog.Error("mark outbox event failed after retry limit", "event_id", event.EventID, "channel_id", event.PartitionKey, "aggregate_version", event.AggregateVersion, "attempts", attempt, "error", stateErr)
						return
					}
					if r.telemetry != nil {
						r.telemetry.recordParked()
					}
					slog.Error("outbox event parked for operator review", "event_id", event.EventID, "channel_id", event.PartitionKey, "aggregate_version", event.AggregateVersion, "attempts", attempt, "error", err)
					return
				}
				retryAt := time.Now().UTC().Add(retryDelay(attempt))
				if r.telemetry != nil {
					r.telemetry.recordRetry()
				}
				if retryErr := r.repository.ScheduleRetry(ctx, event.ID, retryAt, err); retryErr != nil {
					slog.Error("schedule outbox retry failed", "event_id", event.EventID, "channel_id", event.PartitionKey, "attempt", attempt, "error", retryErr)
				} else {
					slog.Warn("publish outbox event failed; retry scheduled", "event_id", event.EventID, "channel_id", event.PartitionKey, "aggregate_version", event.AggregateVersion, "attempt", attempt, "max_attempts", maxPublishAttempts, "retry_at", retryAt, "error", err)
				}
				return
			}

			if err = r.repository.MarkPublished(ctx, event.ID, time.Now().UTC()); err != nil {
				if r.telemetry != nil {
					r.telemetry.recordProcessing(time.Since(startedAt).Nanoseconds())
				}
				// Kafka may already have accepted it. A later publish can duplicate it.
				slog.Error("mark outbox event published failed", "event_id", event.EventID, "channel_id", event.PartitionKey, "error", err)
				return
			}
			if r.telemetry != nil {
				r.telemetry.recordPublished()
				r.telemetry.recordProcessing(time.Since(startedAt).Nanoseconds())
			}
			slog.Info("published chat outbox event", "event_id", event.EventID, "event_type", event.Event.EventType, "aggregate_id", event.Event.AggregateID, "aggregate_version", event.AggregateVersion)
		}()
	}
	workers.Wait()
}

func retryDelay(attempt int64) time.Duration {
	delay := time.Second
	for i := int64(1); i < attempt && delay < 5*time.Minute; i++ {
		delay *= 2
	}
	if delay > 5*time.Minute {
		return 5 * time.Minute
	}
	return delay
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
