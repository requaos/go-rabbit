package worker

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type Manager interface {
	DeclareQueue(name string) error
	InspectQueue(queue string) (*amqp.Queue, error)
	ConsumeAll(queue string) (<-chan *amqp.Delivery, error)
	ConsumeOne(queue string) (*amqp.Delivery, error)
	Publish(queue string, msg []byte) error
	Close()
}

var ErrNoMessageAvailable = errors.New("no messages available")

type RabbitInstance struct {
	publisher *amqp.Connection
	consumer  *amqp.Connection

	Queues []*QueueProcessor
}

func (r *RabbitInstance) Close() {
	r.publisher.Close()
	r.consumer.Close()

	for _, v := range r.Queues {
		v.Done <- struct{}{}
	}
}

func (r *RabbitInstance) DeclareQueue(name string) error {
	//Create a channel
	ch, err := r.publisher.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	//Declare a queue
	_, err = ch.QueueDeclare(
		name,  // name of the queue
		true,  // should the message be persistent? also queue will survive if the cluster gets reset
		false, // autodelete if there's no consumers (like queues that have anonymous names, often used with fanout exchange)
		false, // exclusive means I should get an error if any other consumer subsribes to this queue
		false, // no-wait means I don't want RabbitMQ to wait if there's a queue successfully setup
		nil,   // arguments for more advanced configuration
	)
	if err != nil {
		return err
	}
	return nil
}

func (r *RabbitInstance) Publish(queue string, msg []byte) error {
	//Create a channel
	ch, err := r.publisher.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	err = ch.Publish(
		"",    // exchange
		queue, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			ContentType:  "application/json",
			Body:         msg,
		})
	if err != nil {
		return err
	}
	return nil
}

func (r *RabbitInstance) ConsumeOne(queue string) (*amqp.Delivery, error) {
	//Create a channel
	ch, err := r.consumer.Channel()
	if err != nil {
		return nil, err
	}
	defer ch.Close()

	msg, ok, err := ch.Get(queue, false)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNoMessageAvailable
	}
	return &msg, nil
}

func (r *RabbitInstance) ConsumeAll(queue string) (<-chan *amqp.Delivery, error) {
	//Create a channel
	ch, err := r.consumer.Channel()
	if err != nil {
		return nil, err
	}

	// Check how many messages are in the queue
	q, err := ch.QueueInspect(queue)
	if err != nil {
		return nil, err
	}

	msgs := make(chan *amqp.Delivery)

	go func(msgChan chan *amqp.Delivery, msgCount int) {
		defer ch.Close()
		for i := 0; i < msgCount; i++ {
			msg, ok, err := ch.Get(queue, false)
			if err != nil {
				msg.Nack(false, true)
			}
			if !ok {
				continue
			}
			msgChan <- &msg
		}
	}(msgs, q.Messages)

	return msgs, nil
}

func (r *RabbitInstance) InspectQueue(queue string) (*amqp.Queue, error) {
	//Create a channel
	ch, err := r.consumer.Channel()
	if err != nil {
		return nil, err
	}
	defer ch.Close()

	q, err := ch.QueueInspect(queue)
	if err != nil {
		return nil, err
	}
	return &q, nil
}

func InitializeRabbit(username string, password string, host string, port int) (Manager, error) {
	connectionString := fmt.Sprintf(
		"amqp://%s:%s@%s:%d/",
		username,
		password,
		host,
		port)
	//Make a connection
	publisher, err := amqp.Dial(connectionString)
	if err != nil {
		return nil, err
	}
	consumer, err := amqp.Dial(connectionString)
	if err != nil {
		return nil, err
	}
	return &RabbitInstance{
		publisher: publisher,
		consumer:  consumer,
	}, nil
}
