package groupprocessor

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

type testingSession struct {
	mu    sync.Mutex
	marks []string
}

func (*testingSession) Claims() map[string][]int32 {
	return nil
}

func (*testingSession) MemberID() string {
	return "test"
}

func (*testingSession) GenerationID() int32 {
	return 0
}

func (*testingSession) MarkOffset(_ string, _ int32, _ int64, _ string) {}

func (*testingSession) ResetOffset(_ string, _ int32, _ int64, _ string) {}

func (s *testingSession) MarkMessage(msg *sarama.ConsumerMessage, _ string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marks = append(s.marks, fmt.Sprintf("%s-%d-%d", msg.Topic, msg.Partition, msg.Offset))
}

func (*testingSession) Context() context.Context {
	return nil
}

func (s *testingSession) Results() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.marks[:]
}

func TestNewSimpleSessionManager(t *testing.T) {
	sess := &testingSession{}
	m := NewSequenceSessionManager()
	assert.NoError(t, m.AttachSession(sess))

	var declared []*sarama.ConsumerMessage

	for i := 0; i < 12; i++ {
		msg := &sarama.ConsumerMessage{
			Topic:     "test",
			Partition: 0,
			Offset:    int64(i),
		}
		declared = append(declared, msg)
		assert.NoError(t, m.DeclareMessage(sess, msg))
	}

	// confirm 10
	assert.NoError(t, m.ConfirmMessage(declared[10]))

	assert.Empty(t, sess.Results())

	for i := 0; i < 10; i++ {
		assert.NoError(t, m.ConfirmMessage(declared[i]))
	}

	assert.Equal(t, []string{"test-0-0", "test-0-1", "test-0-2", "test-0-3", "test-0-4", "test-0-5", "test-0-6", "test-0-7", "test-0-8", "test-0-9", "test-0-10"}, sess.Results())
}
