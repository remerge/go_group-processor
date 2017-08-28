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
	Name string
	Log  cue.Logger
}

func (ls *DefaultLoadSaver) setDefaults() {
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
	// log error and remove message from inflight
	// override this method to implement a custom retry logic
	ls.setDefaults()
	ls.Log.Error(err, "failed to process message")
	return true
}
