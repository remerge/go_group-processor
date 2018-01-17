package groupprocessor

import (
	"time"

	"github.com/cenkalti/backoff"
	"github.com/remerge/cue"
)

type Processor interface {
	Messages() chan interface{}
	OnLoad(Processable)
	OnProcessed(Processable)
	OnRetry(Processable)
	OnSkip(Processable, error)
	OnTrack()
	Close()
}

type DefaultProcessor struct {
	ebo *backoff.ExponentialBackOff
	log cue.Logger
}

func (p *DefaultProcessor) New() (err error) {
	p.ebo = backoff.NewExponentialBackOff()
	p.log = cue.NewLogger("DefaultProcessor")
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
	backoff := p.ebo.NextBackOff()

	p.log.WithFields(cue.Fields{
		"value":   processable.Value(),
		"backoff": backoff,
	}).Warn("retrying failed message")

	time.Sleep(backoff)
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
