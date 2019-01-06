package rabbit

import (
	"time"

	"github.com/streadway/amqp"
)

type QueueProcessor struct {
	Done   chan struct{}
	ticker *time.Ticker
	queue  string
}

type QueueFunc func(msg *amqp.Delivery) error

// NewIncrementalQueueProcessor will process jobs one at a time in the given polling interval
func (r *RabbitInstance) NewIncrementalQueueProcessor(name string, pollingInterval time.Duration, processor QueueFunc) {
	qp := &QueueProcessor{
		ticker: time.NewTicker(pollingInterval),
		queue:  name,
		Done:   make(chan struct{}),
	}

	go func(m *QueueProcessor) {
		for {
			select {
			case <-m.Done:
				m.ticker.Stop()
				return
			case <-m.ticker.C:
				msg, err := r.ConsumeOne(m.queue)
				switch err {
				case nil:
				// carry on
				case ErrNoMessageAvailable:
					continue
				default:
					// maybe get some logging in here
					msg.Nack(false, true)
					continue
				}
				err = processor(msg)
				if err != nil {
					// maybe get some logging in here
					msg.Nack(false, true)
					continue
				}
				msg.Ack(false)
			}
		}
	}(qp)

	r.Queues = append(r.Queues, qp)
}

// NewBurstQueueProcessor will all available jobs from a queue concurrently at the given polling interval
func (r *RabbitInstance) NewBurstQueueProcessor(name string, pollingInterval time.Duration, processor QueueFunc) {
	qp := &QueueProcessor{
		ticker: time.NewTicker(pollingInterval),
		queue:  name,
		Done:   make(chan struct{}),
	}

	go func(m *QueueProcessor) {
		for {
			select {
			case <-m.Done:
				m.ticker.Stop()
				return
			case <-m.ticker.C:
				msgs, err := r.ConsumeAll(m.queue)
				if err != nil {
					// errors in this function return before any messages are fetched.
					// continuing here without fear of dropping any messages.

					// maybe get some logging in here
					continue
				}
				for d := range msgs {
					err = processor(d)
					if err != nil {
						d.Nack(false, true)
						continue
					}
					d.Ack(false)
				}
			}
		}
	}(qp)

	r.Queues = append(r.Queues, qp)
}
