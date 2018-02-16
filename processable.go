package groupprocessor

import (
	rand "github.com/remerge/go-xorshift"
)

// Processable is the message interface for the LoadSaver
type Processable interface {
	Key() int
	Value() interface{}
}

// DefaultProcessable provides a vanilla implementation of the interface
type DefaultProcessable struct {
	value interface{}
}

// Key returns the message key
func (p *DefaultProcessable) Key() int {
	return rand.Int()
}

// Value returns the enclosed data/message
func (p *DefaultProcessable) Value() interface{} {
	return p.value
}
