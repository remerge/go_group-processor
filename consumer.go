package groupprocessor

import (
	"context"

	"github.com/Shopify/sarama"
)

type ConsumerConfig struct {
	GroupID string
	Topics  []string
	Handler sarama.ConsumerGroupHandler
	OnError func(error) error
}

func NewConsumerConfig(handler sarama.ConsumerGroupHandler, onError func(error) error, groupID string, topics ...string) ConsumerConfig {
	return ConsumerConfig{
		GroupID: groupID,
		Topics:  topics,
		Handler: handler,
		OnError: onError,
	}
}

type Consumer struct {
	ctx      context.Context
	cancelFn context.CancelFunc

	closedCh   chan struct{}
	sessionErr error
	closeErr   error
}

func Consume(ctx context.Context, saramaConfig *sarama.Config, brokers []string, config ConsumerConfig) (c *Consumer, err error) {
	group, err := sarama.NewConsumerGroup(brokers, config.GroupID, saramaConfig)
	if err != nil {
		return nil, err
	}
	c = &Consumer{
		closedCh: make(chan struct{}),
	}
	c.ctx, c.cancelFn = context.WithCancel(ctx)

	// close watchdog
	go func() {
		<-c.ctx.Done()
		c.closeErr = group.Close()
		close(c.closedCh)
	}()

	// errors pipe
	if saramaConfig.Consumer.Return.Errors {
		go func() {
			for {
				select {
				case <-c.ctx.Done():
					return
				case consumeErr, ok := <-group.Errors():
					if !ok {
						c.cancelFn()
						return
					}
					if fnErr := config.OnError(consumeErr); fnErr != nil {
						c.cancelFn()
						return
					}
				}
			}
		}()
	}

	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			default:
			}
			sessErr := group.Consume(c.ctx, config.Topics, config.Handler)
			if sessErr != nil {
				c.sessionErr = sessErr
				c.cancelFn()
				return
			}
		}
	}()
	return c, nil
}

func (c *Consumer) Wait() error {
	<-c.closedCh
	return c.sessionErr
}

func (c *Consumer) Close() error {
	c.cancelFn()
	<-c.closedCh
	return c.closeErr
}
