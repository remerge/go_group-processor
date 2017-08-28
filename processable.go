package groupprocessor

import "github.com/Shopify/sarama"

// Processable is the message interface for the LoadSaver
type Processable interface {
	Msg() *sarama.ConsumerMessage
}

// DefaultProcessable provides a vanilla implementation of the interface
type DefaultProcessable struct {
	msg     *sarama.ConsumerMessage
	retries int
}

// Msg will return the enclosed saramaConsumerMessage
func (p *DefaultProcessable) Msg() *sarama.ConsumerMessage {
	return p.msg
}
