package eventclient

import (
	"context"
	"math/rand"
	"time"
)

func (c *EventClient) withRetry(ctx context.Context, fn func() error) error {
	var lastErr error

	for i := 0; i < c.config.MaxRetries; i++ {
		if !c.isConnected() {
			if err := c.reconnect(); err != nil {
				lastErr = err
				time.Sleep(c.calculateBackoff(i))
				continue
			}
		}

		if err := fn(); err != nil {
			lastErr = err
			time.Sleep(c.calculateBackoff(i))
			continue
		}

		return nil
	}

	return lastErr
}

func (c *EventClient) calculateBackoff(attempt int) time.Duration {
	base := float64(c.config.RetryDelay)
	max := float64(30 * time.Second)

	backoff := base * float64(attempt+1) * (1 + rand.Float64())
	if backoff > max {
		backoff = max
	}

	return time.Duration(backoff)
}

func (c *EventClient) isConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}
