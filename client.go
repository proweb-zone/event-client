package eventclient

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	pb "event-getaway/gen/go"
)

type EventClient struct {
	gatewayConn *grpc.ClientConn
	gateway     pb.EventGatewayClient
	serviceName string
}

func New(addr, serviceName string) (*EventClient, error) {
	conn, err := grpc.Dial(addr,
		grpc.WithInsecure(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, err
	}

	return &EventClient{
		gatewayConn: conn,
		gateway:     pb.NewEventGatewayClient(conn),
		serviceName: serviceName,
	}, nil
}

func (c *EventClient) Subscribe(ctx context.Context, eventTypes []string, handler func(*pb.Event) error) error {
	req := &pb.SubscriptionRequest{
		ServiceName: c.serviceName,
		EventTypes:  eventTypes,
	}

	stream, err := c.gateway.Subscribe(ctx, req)
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
	return c.gateway.Publish(ctx, event)
}

func (c *EventClient) Close() error {
	return c.gatewayConn.Close()
}
