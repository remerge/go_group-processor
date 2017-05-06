package groupprocessor

import "github.com/Shopify/sarama"

type LoadSaver interface {
	Load(*sarama.ConsumerMessage) (Processable, error)
	Save(Processable) error
}

type DefaultLoadSaver struct {
}

func (ls *DefaultLoadSaver) Load(
	msg *sarama.ConsumerMessage,
) (Processable, error) {
	return &DefaultProcessable{
		msg: msg,
	}, nil
}

func (ls *DefaultLoadSaver) Save(p Processable) error {
	return nil
}
