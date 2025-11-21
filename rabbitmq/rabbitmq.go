package rabbitmq

import (
	"context"
	"errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

func New(queueName, addr string, exclusive bool, autoDelete bool, passive bool) *Client {
	client := Client{
		m:         &sync.Mutex{},
		infoLog:   log.New(os.Stdout, "[INFO] ", log.LstdFlags|log.Lmsgprefix),
		errLog:    log.New(os.Stderr, "[ERROR] ", log.LstdFlags|log.Lmsgprefix),
		queueName: queueName,
		done:      make(chan bool),
	}
	go client.handleReconnect(addr, exclusive, autoDelete, passive)
	return &client
}

// Push will push data onto the queue, and wait for a confirmation.
// This will block until the server sends a confirmation. Errors are
// only returned if the push action itself fails, see UnsafePush.
func (client *Client) Push(data []byte, corrId string, replyTo string) error {
	log.Printf("Pushing %v bytes to queue '%v', corrId=%v, replyTo=%v", len(data), client.queueName, corrId, replyTo)

	client.m.Lock()
	if !client.isReady {
		client.m.Unlock()
		return errors.New("failed to push: not connected")
	}
	client.m.Unlock()
	for {
		err := client.UnsafePush(data, corrId, replyTo)
		if err != nil {
			client.errLog.Println("push failed. Retrying...")
			select {
			case <-client.done:
				return errShutdown
			case <-time.After(resendDelay):
			}
			continue
		}
		confirm := <-client.notifyConfirm
		if confirm.Ack {
			client.infoLog.Printf("push confirmed [deliveryTag=%v, corrId=%v]", confirm.DeliveryTag, corrId)
			return nil
		}
	}
}

// UnsafePush will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will
// receive the message.
func (client *Client) UnsafePush(data []byte, corrId string, replyTo string) error {
	client.m.Lock()
	if !client.isReady {
		client.m.Unlock()
		return errNotConnected
	}
	client.m.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return client.channel.PublishWithContext(
		ctx,
		"",               // Exchange
		client.queueName, // Routing key
		false,            // Mandatory
		false,            // Immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       replyTo,
			Body:          data,
		},
	)
}

// Close will cleanly shut down the channel and connection.
func (client *Client) Close() error {
	client.m.Lock()
	// we read and write isReady in two locations, so we grab the lock and hold onto
	// it until we are finished
	defer client.m.Unlock()

	if !client.isReady {
		return errAlreadyClosed
	}
	close(client.done)
	err := client.channel.Close()
	if err != nil {
		return err
	}
	err = client.connection.Close()
	if err != nil {
		return err
	}

	client.isReady = false
	return nil
}

type Client struct {
	m               *sync.Mutex
	queueName       string
	infoLog         *log.Logger
	errLog          *log.Logger
	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	isReady         bool
}

func (client *Client) QueueName() string {
	return client.queueName
}

// connect will create a new AMQP connection
func (client *Client) connect(addr string) (*amqp.Connection, error) {
	config := amqp.Config{Properties: amqp.NewConnectionProperties()}
	config.Properties.SetClientConnectionName(os.Args[0])
	conn, err := amqp.DialConfig(addr, config)
	if err != nil {
		return nil, err
	}

	client.changeConnection(conn)
	client.infoLog.Println("connected to mq")
	return conn, nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (client *Client) changeConnection(connection *amqp.Connection) {
	client.connection = connection
	client.notifyConnClose = make(chan *amqp.Error, 1)
	client.connection.NotifyClose(client.notifyConnClose)
}

const (
	// When reconnecting to the server after connection failure
	reconnectDelay = 5 * time.Second

	// When setting up the channel after a channel exception
	reInitDelay = 2 * time.Second

	// When resending messages the server didn't confirm
	resendDelay = 5 * time.Second
)

var (
	errNotConnected  = errors.New("not connected to a server")
	errAlreadyClosed = errors.New("already closed: not connected to the server")
	errShutdown      = errors.New("client is shutting down")
)

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (client *Client) handleReconnect(addr string, exclusive bool, autoDelete bool, passive bool) {
	for {
		client.m.Lock()
		client.isReady = false
		client.m.Unlock()

		client.infoLog.Println("attempting to connect mq")

		conn, err := client.connect(addr)
		if err != nil {
			client.errLog.Println("failed to connect mq. Retrying...")

			select {
			case <-client.done:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}

		if done := client.handleReInit(conn, exclusive, autoDelete, passive); done {
			break
		}
	}
}

// handleReInit will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (client *Client) handleReInit(conn *amqp.Connection, exclusive bool, autoDelete bool, passive bool) bool {
	for {
		client.m.Lock()
		client.isReady = false
		client.m.Unlock()

		err := client.init(conn, exclusive, autoDelete, passive)
		if err != nil {
			client.errLog.Println("failed to initialize channel, retrying...")

			if strings.HasPrefix(client.QueueName(), "amq.") && passive {
				// 다시 초기화하지 않고 그냥 포기한다.
				return false
			}

			select {
			case <-client.done:
				return true
			case <-client.notifyConnClose:
				client.infoLog.Println("connection closed, reconnecting...")
				return false
			case <-time.After(reInitDelay):
			}
			continue
		}

		select {
		case <-client.done:
			return true
		case <-client.notifyConnClose:
			client.infoLog.Println("connection closed, reconnecting...")
			return false
		case <-client.notifyChanClose:
			client.infoLog.Println("channel closed, re-running init...")
		}
	}
}

// init will initialize channel & declare queue
func (client *Client) init(conn *amqp.Connection, exclusive bool, autoDelete bool, passive bool) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	err = ch.Confirm(false)
	if err != nil {
		return err
	}
	if passive {
		_, err = ch.QueueDeclarePassive(
			client.queueName,
			!exclusive, // Durable
			autoDelete, // Delete when unused
			exclusive,  // Exclusive
			false,      // No-wait
			nil,        // Arguments
		)
	} else {
		_, err = ch.QueueDeclare(
			client.queueName,
			!exclusive, // Durable
			autoDelete, // Delete when unused
			exclusive,  // Exclusive
			false,      // No-wait
			nil,        // Arguments
		)
	}
	if err != nil {
		return err
	}

	client.changeChannel(ch)
	client.m.Lock()
	client.isReady = true
	client.m.Unlock()
	client.infoLog.Println("client init done")

	return nil
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (client *Client) changeChannel(channel *amqp.Channel) {
	client.channel = channel
	client.notifyChanClose = make(chan *amqp.Error, 1)
	client.notifyConfirm = make(chan amqp.Confirmation, 1)
	client.channel.NotifyClose(client.notifyChanClose)
	client.channel.NotifyPublish(client.notifyConfirm)
}

// Consume will continuously put queue items on the channel.
// It is required to call delivery.Ack when it has been
// successfully processed, or delivery.Nack when it fails.
// Ignoring this will cause data to build up on the server.
func (client *Client) Consume() (<-chan amqp.Delivery, error) {
	client.m.Lock()
	if !client.isReady {
		client.m.Unlock()
		return nil, errNotConnected
	}
	client.m.Unlock()

	if err := client.channel.Qos(
		1,     // prefetchCount
		0,     // prefetchSize
		false, // global
	); err != nil {
		return nil, err
	}

	return client.channel.Consume(
		client.queueName,
		"",    // Consumer
		false, // Auto-Ack
		false, // Exclusive
		false, // No-local
		false, // No-Wait
		nil,   // Args
	)
}
