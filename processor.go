package groupprocessor

import (
	"github.com/remerge/cue"
)

type Processor interface {
	Messages() chan interface{}
	OnLoad(Processable)
	OnProcessed(Processable)
	OnRetry(Processable)
	OnSkip(Processable, error)
	Wait() error
	Close()
}

type DefaultProcessor struct {
	ID  string
	log cue.Logger
}

func (p *DefaultProcessor) New() (err error) {
	p.log = cue.NewLogger(p.ID)
	return nil
}

func (p *DefaultProcessor) OnLoad(processable Processable) {
	p.log.WithFields(cue.Fields{
		"value": processable.Value(),
	}).Debug("loaded message")
}

func (p *DefaultProcessor) OnProcessed(processable Processable) {
	p.log.WithFields(cue.Fields{
		"value": processable.Value(),
	}).Debug("processed message")
}

func (p *DefaultProcessor) OnRetry(processable Processable) {
	p.log.WithFields(cue.Fields{
		"value": processable.Value(),
		"key":   processable.Key(),
	}).Warn("retrying failed message")
}

func (p *DefaultProcessor) OnSkip(processable Processable, err error) {
	p.log.WithFields(cue.Fields{
		"error": err,
		"value": processable.Value(),
	}).Warn("skipping message after all retries")
}

func (p *DefaultProcessor) Close() {
}
