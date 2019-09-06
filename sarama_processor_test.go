package groupprocessor

import (
	"errors"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

type testProcessable struct {
	SaramaProcessable
	retries int
}

type testLoadSaver struct {
	SaramaLoadSaver
	saveFn func(Processable) error
}

func (ls *testLoadSaver) New(saveFn func(Processable) error) error {
	if err := ls.SaramaLoadSaver.New("testLoadSaver"); err != nil {
		return err
	}
	ls.saveFn = saveFn
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
	return ls.saveFn(p)
}

func (ls *testLoadSaver) Fail(p Processable, err error) bool {
	tp := p.(*testProcessable)
	tp.retries++
	return ls.SaramaLoadSaver.Fail(p, err)
}

func assertEqual(t *testing.T, a interface{}, b interface{}, message string, args ...interface{}) {
	t.Helper()
	if a != b {
		t.Fatalf(message, args...)
	}
}

func createTestSaramaGroupProcessor(t *testing.T, loadSaver LoadSaver) *GroupProcessor {
	t.Helper()

	p, err := NewSaramaProcessor(&SaramaProcessorConfig{
		Name:    "processor",
		Brokers: "localhost:9092",
		Topic:   "test",
	})
	if err != nil {
		t.Fatalf("couldn't create sarama processor: %v", err)
	}

	gp, err := New(&Config{
		Name:          "gp",
		Processor:     p,
		MaxRetries:    1,
		NumLoadWorker: 1,
		NumSaveWorker: 1,
		LoadSaver:     loadSaver,
	})
	if err != nil {
		t.Fatalf("couldn't create group processor: %v", err)
	}

	return gp
}

func produceTestMessage(t *testing.T, msg string) {
	t.Helper()

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		t.Fatalf("couldn't create producer: %v", err)
	}

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: "test",
		Value: sarama.StringEncoder(msg),
	})
	if err != nil {
		t.Fatalf("couldn't produce test message: %v", err)
	}
}

func TestGroupProcessor(t *testing.T) {
	tls := &testLoadSaver{}
	msgs := make(chan string)
	err := tls.New(func(p Processable) error {
		msgs <- string(p.Value().(*sarama.ConsumerMessage).Value)
		return nil
	})
	if err != nil {
		t.Fatalf("couldn't create test load saver: %v", err)
	}

	gp := createTestSaramaGroupProcessor(t, tls)
	gp.Run()

	produceTestMessage(t, "test")

	timeout := time.After(5 * time.Second)
	var msg string

L:
	for {
		select {
		case msg = <-msgs:
		case <-timeout:
			gp.Close()
			break L
		}
	}
	assertEqual(t, msg, "test", "expected message to equal \"test\", got %#v", msg)

}

func TestGroupProcessorWithErrorRetry(t *testing.T) {
	tls := &testLoadSaver{}
	processables := make(chan *testProcessable)
	err := tls.New(func(p Processable) error {
		tp := p.(*testProcessable)
		if tp.retries > 0 {
			processables <- tp
		} else {
			return errors.New("test error")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("couldn't create test load saver: %v", err)
	}

	gp := createTestSaramaGroupProcessor(t, tls)
	gp.Run()

	produceTestMessage(t, "test")

	timeout := time.After(2 * time.Second)
	var tp *testProcessable

L:
	for {
		select {
		case tp = <-processables:
		case <-timeout:
			gp.Close()
			break L
		}
	}
	assertEqual(t, tp.retries, 1, "expected message to be retried once, got %#v", tp.retries)

}

func TestGroupProcessorCommitOffsetsAfterSkip(t *testing.T) {
	tls := &testLoadSaver{}
	msgs := make(chan string)
	err := tls.New(func(p Processable) error {
		msgs <- string(p.Value().(*sarama.ConsumerMessage).Value)
		return errors.New("BOOM")
	})
	if err != nil {
		t.Fatalf("couldn't create test load saver: %v", err)
	}

	gp := createTestSaramaGroupProcessor(t, tls)
	gp.Run()

	produceTestMessage(t, "test1")
	produceTestMessage(t, "test2")
	produceTestMessage(t, "test3")

	timeout := time.After(6 * time.Second)
	var msg string

L:
	for {
		select {
		case msg = <-msgs:
		case <-timeout:
			gp.Close()
			break L
		}
	}

	assertEqual(t, msg, "test3", "expected message to equal \"test3\", got %#v", msg)

	gp = createTestSaramaGroupProcessor(t, tls)
	gp.Run()

	timeout = time.After(1 * time.Second)
	msg = ""

L2:
	for {
		select {
		case msg = <-msgs:
		case <-timeout:
			gp.Close()
			break L2
		}
	}

	assertEqual(t, msg, "", "expected message to equal \"\", got %#v", msg)
}
