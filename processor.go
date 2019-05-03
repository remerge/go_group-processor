package groupprocessor

import (
	"math/rand"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/remerge/cue"
)

type RandomBackoff struct {
	Max int64
}

func NewRandomBackoff(max time.Duration) backoff.BackOff {
	return &RandomBackoff{
		Max: int64(max),
	}
}

func (b *RandomBackoff) NextBackOff() time.Duration {
	return time.Duration(rand.Int63n(b.Max))
}

func (*RandomBackoff) Reset() {}

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
	ebo backoff.BackOff
	log cue.Logger
}

func (p *DefaultProcessor) New() (err error) {
	p.ebo = NewRandomBackoff(time.Millisecond * 500)
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
