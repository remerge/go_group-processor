package groupprocessor

import (
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

type testProcessable struct {
	msg *sarama.ConsumerMessage
}

func (p *testProcessable) Msg() *sarama.ConsumerMessage {
	return p.msg
}

type testLoadSaver struct {
	channel chan string
}

func (ls *testLoadSaver) Load(
	msg *sarama.ConsumerMessage,
) (GroupProcessable, error) {
	return &testProcessable{
		msg: msg,
	}, nil
}

func (ls *testLoadSaver) Save(p GroupProcessable) error {
	tp := p.(*testProcessable)
	ls.channel <- string(tp.Msg().Value)
	return nil
}

func TestGroupProcessor(t *testing.T) {
	tls := &testLoadSaver{
		channel: make(chan string),
	}

	gp := &GroupProcessor{
		Name:          "gp",
		Brokers:       "localhost:9092",
		Topic:         "test",
		NumLoadWorker: 4,
		NumSaveWorker: 4,
		LoadSaver:     tls,
	}

	if err := gp.New(); err != nil {
		t.Errorf("Unexpected error in gp.New: %v", err)
		return
	}

	gp.Run()

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		t.Errorf("Unexpected error in NewSyncProducer: %v", err)
		return
	}

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: "test",
		Value: sarama.StringEncoder("test"),
	})

	if err != nil {
		t.Errorf("Unexpected error in SendMessage: %v", err)
		return
	}

	var msg string

L:
	for {
		select {
		// drain channel
		case msg = <-tls.channel:
			fmt.Printf("msg=%#v\n", msg)
		case <-time.After(100 * time.Millisecond):
			gp.Close()
			break L
		}
	}

	if msg != "test" {
		t.Errorf("expected message to equal \"true\", got %#v", msg)
	}
}
