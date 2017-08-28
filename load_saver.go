package groupprocessor

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/bobziuchkovski/cue"
)

type LoadSaver interface {
	Load(*sarama.ConsumerMessage) Processable
	Save(Processable) error
	Done(Processable) bool
	Fail(Processable, error) bool
}

type DefaultLoadSaver struct {
	Name       string
	Log        cue.Logger
	MaxRetries int
}

func (ls *DefaultLoadSaver) SetDefaults() {
	if ls.Name == "" {
		ls.Name = fmt.Sprintf("DefaultLoadSaver:%p", ls)
	}

	if ls.Log == nil {
		ls.Log = cue.NewLogger(ls.Name)
	}
}

func (ls *DefaultLoadSaver) Load(
	msg *sarama.ConsumerMessage,
) Processable {
	ls.SetDefaults()
	return &DefaultProcessable{
		msg: msg,
	}
}

func (ls *DefaultLoadSaver) Save(p Processable) error {
	return nil
}

func (ls *DefaultLoadSaver) Done(p Processable) bool {
	// do nothing and remove message from inflight
	// override this method to track metrics etc
	return true
}

func (ls *DefaultLoadSaver) Fail(p Processable, err error) bool {
	dp := p.(*DefaultProcessable)

	// TODO: add backoff handling?
	if dp.retries < ls.MaxRetries {
		dp.retries++

		ls.Log.WithFields(cue.Fields{
			"msg":     dp.msg,
			"error":   err,
			"retries": dp.retries,
		}).Warn("retrying message after failure")

		return false
	}

	ls.Log.WithFields(cue.Fields{
		"msg": dp.msg,
	}).Error(err, "skipping message after all retries")

	return true
}
