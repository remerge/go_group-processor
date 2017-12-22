package groupprocessor

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cenkalti/backoff"
	"github.com/remerge/cue"
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
	ebo        *backoff.ExponentialBackOff
}

func (ls *DefaultLoadSaver) SetDefaults() {
	if ls.Name == "" {
		ls.Name = fmt.Sprintf("DefaultLoadSaver:%p", ls)
	}

	if ls.Log == nil {
		ls.Log = cue.NewLogger(ls.Name)
	}

	if ls.ebo == nil {
		ls.ebo = backoff.NewExponentialBackOff()
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
	if r, ok := p.(Retryable); ok {
		return ls.FailWithRetry(
			r,
			err,
			func(backoff time.Duration) {
				ls.Log.WithFields(cue.Fields{
					"msg":     r.Msg(),
					"error":   err,
					"retries": r.Retries(),
					"backoff": backoff,
				}).Warn("retrying message after failure")
			},
			func() {
				ls.Log.WithFields(cue.Fields{
					"msg": r.Msg(),
				}).Error(err, "skipping message after all retries")
			},
		)
	}

	ls.Log.WithFields(cue.Fields{
		"msg": p.Msg(),
	}).Error(err, "skipping non-retryable message")

	return true
}

func (ls *DefaultLoadSaver) FailWithRetry(r Retryable, err error, onRetry func(time.Duration), onSkip func()) bool {
	if r.Retries() < ls.MaxRetries {
		r.Retry()

		nextBackOff := ls.ebo.NextBackOff()
		if onRetry != nil {
			onRetry(nextBackOff)
		}
		time.Sleep(nextBackOff)

		// group processor will call save again
		return false
	}

	if onSkip != nil {
		onSkip()
	}

	// message was processed and will be removed from inflight
	return true
}
