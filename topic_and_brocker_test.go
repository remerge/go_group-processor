package groupprocessor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimpleTopicAndBrokerUsage(t *testing.T) {
	subject := TopicAndBroker{}
	err := subject.Set("test:localhost:9092")
	require.Nil(t, err)
	err = subject.Set("test2:localhost:9092")
	require.Nil(t, err)

	require.True(t, subject.Available())
	require.False(t, subject.IsEmpty())
	require.Equal(t, subject.FirstBrokers(), "localhost:9092")
	require.Equal(t, len(subject), 2)
}
