package groupprocessor

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

type testProcessable struct {
	DefaultProcessable
	retries int
}

type testLoadSaver struct {
	DefaultLoadSaver
	channel chan string
}

func (ls *testLoadSaver) New(name string) error {
	if err := ls.DefaultLoadSaver.New(name); err != nil {
		return err
	}

	ls.channel = make(chan string)

	return nil
}

func (ls *testLoadSaver) Load(value interface{}) Processable {
	return &testProcessable{
		DefaultProcessable: DefaultProcessable{
			value: value,
		},
	}
}

func (ls *testLoadSaver) Save(p Processable) error {
	ls.channel <- string(p.Value().(*sarama.ConsumerMessage).Value)
	return ls.DefaultLoadSaver.Save(p)
}

func (ls *testLoadSaver) Done(p Processable) bool {
	ls.log.Infof("processed msg=%v", p.Value())
	return ls.DefaultLoadSaver.Done(p)
}

func (ls *testLoadSaver) Fail(p Processable, err error) bool {
	tp := p.(*testProcessable)
	tp.retries++
	return ls.DefaultLoadSaver.Fail(p, err)
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

	p := &SaramaProcessor{
		Name:    "processor",
		Brokers: "localhost:9092",
		Topic:   "test",
	}

	if err := p.New(); err != nil {
		t.Errorf("Unexpected error in p.New: %v", err)
		return
	}

	gp := &GroupProcessor{
		Name:          "gp",
		Processor:     p,
		NumLoadWorker: 4,
		NumSaveWorker: 4,
		TrackInterval: 10,
		LoadSaver:     tls,
	}

	if err := gp.New(); err != nil {
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

	timeout := time.After(time.Second * 5)
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
	if err := ls.DefaultLoadSaver.New(name); err != nil {
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

	p := &SaramaProcessor{
		Name:    "processor",
		Brokers: "localhost:9092",
		Topic:   "test",
	}

	if err := p.New(); err != nil {
		t.Errorf("Unexpected error in p.New: %v", err)
		return
	}

	gp := &GroupProcessor{
		Name:          "gp",
		Processor:     p,
		MaxRetries:    1,
		NumLoadWorker: 4,
		NumSaveWorker: 4,
		TrackInterval: 10,
		LoadSaver:     tls,
	}

	if err := gp.New(); err != nil {
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

	timeout := time.After(time.Second * 5)
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

func TestGroupProcessor_with_CustomConfig(t *testing.T) {
	tls := &testLoadSaver{}

	if err := tls.New("testLoadSaver"); err != nil {
		t.Errorf("Unexpected error in tls.New: %v", err)
		return
	}

	config := sarama.NewConfig()
	config.ClientID = "TEST"                             // ClientID will always be overridden
	config.Version = sarama.V0_8_2_0                     // Version will always be overridden
	config.Consumer.MaxProcessingTime = 30 * time.Second // everything will be set
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	p := &SaramaProcessor{
		Name:    "p",
		Brokers: "localhost:9092",
		Topic:   "test",
		Config:  config,
	}

	if err := p.New(); err != nil {
		t.Errorf("Unexpected error in p.New: %v", err)
		return
	}

	gp := &GroupProcessor{
		Name:          "gp",
		Processor:     p,
		NumLoadWorker: 4,
		NumSaveWorker: 4,
		TrackInterval: 10,
		LoadSaver:     tls,
	}

	if err := gp.New(); err != nil {
		t.Errorf("Unexpected error in gp.New: %v", err)
		return
	}

	assertEqual(t, p.Config.ClientID, "p.test", "ClientID should always be created as <Name>.<Topic>")
	assertEqual(t, p.Config.Version, sarama.V0_10_0_0, "Version will always be set to V0_10_0_0")
	assertEqual(t, p.Config.Consumer.MaxProcessingTime, 30*time.Second, "MaxProcessingTime should be 30s")
	assertEqual(t, p.Config.Consumer.Offsets.Initial, sarama.OffsetNewest, "Offsets.Initial should be OffsetNewest")

	gp.Run()

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	assertEqual(t, err, nil, "Unexpected error in NewSyncProducer: %v", err)

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: "test",
		Value: sarama.StringEncoder("test"),
	})

	assertEqual(t, err, nil, "Unexpected error in SendMessage: %v", err)

	timeout := time.After(time.Second * 5)
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
