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
	OnTrack()
	Wait()
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
	// nolint: errcheck
	p.log.WithFields(cue.Fields{
		"value": processable.Value(),
	}).Error(err, "skipping message after all retries")
}

func (p *DefaultProcessor) OnTrack() {
}

func (p *DefaultProcessor) Close() {
}
