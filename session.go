package groupprocessor

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
)

var (
	ErrSessionIsAlreadyAttached = errors.New("session is already attached")
	ErrSessionIsNotAttached     = errors.New("session is not attached")
	ErrBadSession               = errors.New("bad session")
	ErrNotDeclared              = errors.New("message is not declared")
)

type SequenceSessionManager struct {
	sess     sarama.ConsumerGroupSession
	declared map[partitionKey][]*managedMessage
	mu       sync.Mutex
}

func NewSequenceSessionManager() *SequenceSessionManager {
	return &SequenceSessionManager{}
}

// AttachSession attaches given Kafka session to manager. Use in `Setup()` method of Sarama CG handler.
func (m *SequenceSessionManager) AttachSession(sess sarama.ConsumerGroupSession) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.sess != nil {
		return ErrSessionIsAlreadyAttached
	}
	m.sess = sess
	m.declared = map[partitionKey][]*managedMessage{}
	return nil
}

// ReleaseSession detaches given session from manager. Use in `Cleanup()` method of Sarama CG handler. To force detach use nil as argument.
func (m *SequenceSessionManager) ReleaseSession(sess sarama.ConsumerGroupSession) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if sess != nil {
		if m.sess == nil {
			return ErrSessionIsNotAttached
		}
		if m.sess.MemberID() != sess.MemberID() || m.sess.GenerationID() != m.sess.GenerationID() {
			return ErrBadSession
		}
	}

	m.sess = nil
	m.declared = nil
	return nil
}

// DeclareMessage declares message. This defines commit sequence and should be called only once for each message in correct order. Use in `ConsumeClaim()` method of Sarama CG handler.
func (m *SequenceSessionManager) DeclareMessage(sess sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.sess == nil {
		return ErrSessionIsNotAttached
	}
	if m.sess.MemberID() != sess.MemberID() || m.sess.GenerationID() != m.sess.GenerationID() {
		return ErrBadSession
	}

	key := partitionKey{
		topic:     msg.Topic,
		partition: msg.Partition,
	}

	if len(m.declared[key]) > 0 && m.declared[key][len(m.declared[key])-1].message.Offset > msg.Offset {
		return fmt.Errorf("message offset is less than last: declared=%d offset=%d", m.declared[key][len(m.declared[key])-1].message.Offset, msg.Offset)
	}

	m.declared[key] = append(m.declared[key], &managedMessage{
		message: msg,
	})
	return nil
}

// ConfirmMessage confirms that given message is processed and offset can be committed to consumer group. Message must be declared.
func (m *SequenceSessionManager) ConfirmMessage(msg *sarama.ConsumerMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.sess == nil {
		return ErrSessionIsNotAttached
	}

	key := partitionKey{
		topic:     msg.Topic,
		partition: msg.Partition,
	}
	partition, ok := m.declared[key]
	if !ok {
		return ErrNotDeclared
	}

	var found bool
	for _, declared := range partition {
		if declared.message.Offset == msg.Offset {
			declared.confirmed = true
			found = true
			break
		}
	}
	if !found {
		return ErrNotDeclared
	}

	var confirmed int
	for _, declared := range partition {
		if !declared.confirmed {
			break
		}
		m.sess.MarkMessage(declared.message, "")
		confirmed++
	}
	if confirmed > 0 {
		m.declared[key] = partition[confirmed:]
	}
	return nil
}

type partitionKey struct {
	topic     string
	partition int32
}

type managedMessage struct {
	message   *sarama.ConsumerMessage
	confirmed bool
}
