package groupprocessor

import "github.com/Shopify/sarama"

// Processable is the message interface for the LoadSaver
type Processable interface {
	Msg() *sarama.ConsumerMessage
}

// Retryable is an interface for messages where saving can be retried
type Retryable interface {
	Processable
	Retry()
	Retries() int
}

// DefaultProcessable provides a vanilla implementation of the interface
type DefaultProcessable struct {
	msg     *sarama.ConsumerMessage
	retries int
}

// Msg returns the enclosed saramaConsumerMessage
func (p *DefaultProcessable) Msg() *sarama.ConsumerMessage {
	return p.msg
}

// Retry increments the number of save retries attempted
func (p *DefaultProcessable) Retry() {
	p.retries++
}

// Retries returns the number of save retries attempted
func (p *DefaultProcessable) Retries() int {
	return p.retries
}
