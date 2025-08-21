package eventclient

import (
	"context"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"

	pb "test/gen/go" // укажите свой путь до protobuf
)

type Config struct {
	GatewayAddress string
	ServiceName    string
	MaxRetries     int
	RetryDelay     time.Duration
}

type EventClient struct {
	config    Config
	conn      *grpc.ClientConn
	client    pb.EventGatewayClient
	mu        sync.RWMutex
	connected bool
}

func New(config Config) (*EventClient, error) {
	client := &EventClient{
		config: config,
	}

	if err := client.connect(); err != nil {
		return nil, err
	}

	go client.healthCheck()
	return client, nil
}

func (c *EventClient) connect() error {
	conn, err := grpc.Dial(c.config.GatewayAddress,
		grpc.WithInsecure(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
	)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.conn = conn
	c.client = pb.NewEventGatewayClient(conn)
	c.connected = true
	c.mu.Unlock()

	return nil
}

func (c *EventClient) healthCheck() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		state := c.conn.GetState()
		if state != connectivity.Ready && state != connectivity.Idle {
			log.Printf("Connection lost, state: %s", state)
			c.mu.Lock()
			c.connected = false
			c.mu.Unlock()

			if err := c.reconnect(); err != nil {
				log.Printf("Reconnection failed: %v", err)
			}
		}
	}
}

func (c *EventClient) reconnect() error {
	c.conn.Close()
	return c.connect()
}

func (c *EventClient) Subscribe(ctx context.Context, eventTypes []string, handler func(*pb.Event) error) error {
	// TODO добавить retry логику
	req := &pb.SubscriptionRequest{
		ServiceName: c.config.ServiceName,
		EventTypes:  eventTypes,
	}

	stream, err := c.client.Subscribe(ctx, req)
	if err != nil {
		return err
	}

	go func() {
		for {
			event, err := stream.Recv()
			if err != nil {
				log.Printf("Subscription error: %v", err)
				time.Sleep(5 * time.Second)
				c.Subscribe(ctx, eventTypes, handler) // Переподписка
				return
			}

			if err := handler(event); err != nil {
				log.Printf("Event handling failed: %v", err)
			}
		}
	}()

	return nil
}

func (c *EventClient) Publish(ctx context.Context, event *pb.Event) (*pb.PublishResponse, error) {
	return c.client.Publish(ctx, event)
}

func (c *EventClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected = false
	return c.conn.Close()
}
