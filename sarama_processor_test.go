package groupprocessor

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
)

type testProcessable struct {
	SaramaProcessable
	retries int
}

type testLoadSaver struct {
	SaramaLoadSaver
	channel chan string
}

func TestProcessorInit(t *testing.T) {
	tls := &testLoadSaver{}

	if err := tls.New("testLoadSaver"); err != nil {
		t.Errorf("Unexpected error in tls.New: %v", err)
		return
	}

	p, err := NewSaramaProcessor(&SaramaProcessorConfig{
		Name:    "processor",
		Brokers: "localhost:9092",
		Topic:   "test",
	})
	require.True(t, p.Config.Group.Return.Notifications)
	require.Nil(t, err)
	var ok, channelClosed bool
	select {
	case _, ok = <-p.firstRebalanceDoneCh:
		channelClosed = true
	default:
		channelClosed = false
	}
	require.True(t, channelClosed)
	require.False(t, ok)
	require.NotPanics(t, p.Close)
}

func (ls *testLoadSaver) New(name string) error {
	if err := ls.SaramaLoadSaver.New(name); err != nil {
		return err
	}

	ls.channel = make(chan string)

	return nil
}

func (ls *testLoadSaver) Load(value interface{}) Processable {
	return &testProcessable{
		SaramaProcessable: SaramaProcessable{
			value: value.(*sarama.ConsumerMessage),
		},
	}
}

func (ls *testLoadSaver) Save(p Processable) error {
	ls.channel <- string(p.Value().(*sarama.ConsumerMessage).Value)
	return ls.SaramaLoadSaver.Save(p)
}

func (ls *testLoadSaver) Done(p Processable) bool {
	ls.Log.Infof("processed msg=%v", p.Value())
	return ls.SaramaLoadSaver.Done(p)
}

func (ls *testLoadSaver) Fail(p Processable, err error) bool {
	tp := p.(*testProcessable)
	tp.retries++
	return ls.SaramaLoadSaver.Fail(p, err)
}

func assertEqual(t *testing.T, a interface{}, b interface{}, message string, args ...interface{}) {
	if a != b {
		t.Fatalf(message, args)
	}
}

func TestGroupProcessor(t *testing.T) {
	tls := &testLoadSaver{}

	if err := tls.New("testLoadSaver"); err != nil {
		t.Errorf("Unexpected error in tls.New: %v", err)
		return
	}

	p, err := NewSaramaProcessor(&SaramaProcessorConfig{
		Name:    "processor",
		Brokers: "localhost:9092",
		Topic:   "test",
	})

	if err != nil {
		t.Errorf("Unexpected error in p.New: %v", err)
		return
	}

	gp, err := New(&Config{
		Name:          "gp",
		Processor:     p,
		NumLoadWorker: 4,
		NumSaveWorker: 4,
		TrackInterval: 1 * time.Second,
		LoadSaver:     tls,
	})

	if err != nil {
		t.Errorf("Unexpected error in gp.New: %v", err)
		return
	}

	gp.Run()

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	assertEqual(t, err, nil, "Unexpected error in NewSyncProducer: %v", err)

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: "test",
		Value: sarama.StringEncoder("test"),
	})

	assertEqual(t, err, nil, "Unexpected error in SendMessage: %v", err)

	timeout := time.After(1 * time.Second)
	var msg string

L:
	for {
		select {
		// drain channel
		case msg = <-tls.channel:
			fmt.Printf("msg=%#v\n", msg)
		case <-timeout:
			gp.Close()
			break L
		}
	}

	assertEqual(t, msg, "test", "expected message to equal \"test\", got %#v", msg)
}

type testLoadErrorSaver struct {
	testLoadSaver
	channel chan *testProcessable
}

func (ls *testLoadErrorSaver) New(name string) error {
	if err := ls.SaramaLoadSaver.New(name); err != nil {
		return err
	}

	ls.channel = make(chan *testProcessable)

	return nil
}

func (ls *testLoadErrorSaver) Save(p Processable) error {
	tp := p.(*testProcessable)
	if tp.retries > 0 {
		ls.channel <- tp
	} else {
		return errors.New("test error")
	}
	return nil
}

func TestGroupProcessorWithErrorRetry(t *testing.T) {
	tls := &testLoadErrorSaver{}

	if err := tls.New("testLoadErrorSaver"); err != nil {
		t.Errorf("Unexpected error in tls.New: %v", err)
		return
	}

	p, err := NewSaramaProcessor(&SaramaProcessorConfig{
		Name:    "processor",
		Brokers: "localhost:9092",
		Topic:   "test",
	})

	if err != nil {
		t.Errorf("Unexpected error in p.New: %v", err)
		return
	}

	gp, err := New(&Config{
		Name:          "gp",
		Processor:     p,
		MaxRetries:    1,
		NumLoadWorker: 4,
		NumSaveWorker: 4,
		TrackInterval: 1 * time.Second,
		LoadSaver:     tls,
	})

	if err != nil {
		t.Errorf("Unexpected error in gp.New: %v", err)
		return
	}

	gp.Run()

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	assertEqual(t, err, nil, "Unexpected error in NewSyncProducer: %v", err)

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: "test",
		Value: sarama.StringEncoder("test"),
	})

	assertEqual(t, err, nil, "Unexpected error in SendMessage: %v", err)

	timeout := time.After(1 * time.Second)
	var tp *testProcessable

L:
	for {
		select {
		// drain channel
		case tp = <-tls.channel:
			fmt.Printf("msg=%#v\n", string(tp.Value().(*sarama.ConsumerMessage).Value))
		case <-timeout:
			gp.Close()
			break L
		}
	}

	assertEqual(t, tp.retries, 1, "expected message to be retried once, got %#v", tp.retries)
}
