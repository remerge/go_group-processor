package groupprocessor

import "github.com/Shopify/sarama"

type Processable interface {
	Msg() *sarama.ConsumerMessage
}

type DefaultProcessable struct {
	msg *sarama.ConsumerMessage
}

func (p *DefaultProcessable) Msg() *sarama.ConsumerMessage {
	return p.msg
}
