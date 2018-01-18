package groupprocessor

import (
	"github.com/remerge/cue"
)

type LoadSaver interface {
	Load(msg interface{}) Processable
	Save(p Processable) error
	Done(p Processable) bool
	Fail(p Processable, err error) bool
}

type DefaultLoadSaver struct {
	Name string
	log  cue.Logger
}

func (ls *DefaultLoadSaver) New(name string) error {
	ls.Name = name
	ls.log = cue.NewLogger(ls.Name)
	return nil
}

func (ls *DefaultLoadSaver) Load(value interface{}) Processable {
	return &DefaultProcessable{
		value: value,
	}
}

func (ls *DefaultLoadSaver) Save(p Processable) error {
	return nil
}

func (ls *DefaultLoadSaver) Done(p Processable) bool {
	return true
}

func (ls *DefaultLoadSaver) Fail(p Processable, err error) bool {
	// nolint: errcheck
	ls.log.WithFields(cue.Fields{
		"value": p.Value(),
	}).Error(err, "failed to process message")

	return false
}
