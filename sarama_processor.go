package groupprocessor

import (
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/remerge/cue"
	wp "github.com/remerge/go-worker_pool"
	rand "github.com/remerge/go-xorshift"
)

type SaramaProcessable struct {
	value *sarama.ConsumerMessage
}

func NewSaramaProcessable(value *sarama.ConsumerMessage) *SaramaProcessable {
	return &SaramaProcessable{value: value}
}

func (p *SaramaProcessable) Key() int {
	key := p.value.Key

	if len(key) > 0 {
		hash := fnv.New64a()
		// nolint: errcheck
		hash.Write(key)
		return int(hash.Sum64())
	}

	return rand.Int()
}

func (p *SaramaProcessable) Value() interface{} {
	return p.value
}

func (p *SaramaProcessable) Msg() *sarama.ConsumerMessage {
	return p.value
}

type SaramaLoadSaver struct {
	DefaultLoadSaver
}

func (ls *SaramaLoadSaver) Load(value interface{}) Processable {
	return &SaramaProcessable{
		value: value.(*sarama.ConsumerMessage),
	}
}

// SaramaGroupProcessor is a Processor that reads messages from a Kafka topic with a
// group consumer and tracks offsets of processed messages
type SaramaGroupProcessor struct {
	DefaultProcessor

	*SaramaProcessorConfig

	sync.RWMutex

	client   sarama.Client
	consumer sarama.ConsumerGroup

	messages    chan interface{}
	messagePool *wp.Pool

	loadedOffsets   map[int32]int64
	inFlightOffsets map[int32]map[int64]*sarama.ConsumerMessage
}

type SaramaProcessorConfig struct {
	Name     string
	Brokers  string
	Topic    string
	GroupGen int

	Config *sarama.Config
}

// NewSaramaProcessor create a new SaramaProcessor which includes a Kafka group consumer as source
// for messages for this processor look
func NewSaramaGroupProcessor(config *SaramaProcessorConfig) (p *SaramaGroupProcessor, err error) {
	p = &SaramaGroupProcessor{SaramaProcessorConfig: config}
	if err = p.init(); err != nil {
		return nil, err
	}
	return p, nil
}

// New initializes the SaramaProcessor once it's instantiated
func (p *SaramaGroupProcessor) init() (err error) {
	p.ID = fmt.Sprintf("%v.%v", p.Name, p.Topic)

	if err = p.DefaultProcessor.New(); err != nil {
		return err
	}

	if p.Config == nil {
		p.Config = sarama.NewConfig()
		p.Config.Consumer.MaxProcessingTime = 30 * time.Second
		p.Config.Consumer.Offsets.Initial = sarama.OffsetOldest
		p.Config.Group.Return.Notifications = true
	}

	p.Config.ClientID = p.ID
	p.Config.Version = sarama.V0_10_0_0

	p.client, err = sarama.NewClient(
		strings.Split(p.Brokers, ","),
		p.Config,
	)

	if err != nil {
		return err
	}

	group := fmt.Sprintf("%s.%s.%d", p.Name, p.Topic, p.GroupGen)
	p.consumer, err = sarama.NewConsumerGroupFromClient(
		p.client,
		group,
		[]string{p.Topic},
	)

	if err != nil {
		return err
	}

	p.messages = make(chan interface{})
	p.messagePool = wp.NewPool(p.ID+".messages", 1, p.messageWorker)
	p.messagePool.Run()

	p.loadedOffsets = make(map[int32]int64)
	p.inFlightOffsets = make(map[int32]map[int64]*sarama.ConsumerMessage)

	return nil
}

func (p *SaramaGroupProcessor) messageWorker(w *wp.Worker) {
	for {
		select {
		case <-w.Closer():
			w.Done()
			return
		case msg, ok := <-p.consumer.Messages():
			if ok {
				p.messages <- msg
			} else {
				w.Done()
				return
			}
		case n, ok := <-p.consumer.Notifications():
			if !ok {
				break
			}
			p.log.WithFields(cue.Fields{
				"added":    n.Claimed,
				"current":  n.Current,
				"released": n.Released,
			}).Infof("group rebalance")
		}
	}
}

func (p *SaramaGroupProcessor) Messages() chan interface{} {
	return p.messages
}

func (p *SaramaGroupProcessor) Wait() {
	p.messagePool.Wait()
}

func (p *SaramaGroupProcessor) OnLoad(processable Processable) {
	msg := processable.Value().(*sarama.ConsumerMessage)

	p.Lock()
	defer p.Unlock()

	if p.loadedOffsets[msg.Partition] < msg.Offset {
		p.loadedOffsets[msg.Partition] = msg.Offset
	}

	if p.inFlightOffsets[msg.Partition] == nil {
		p.inFlightOffsets[msg.Partition] = make(
			map[int64]*sarama.ConsumerMessage,
		)
	}

	p.inFlightOffsets[msg.Partition][msg.Offset] = msg
}

func (p *SaramaGroupProcessor) OnProcessed(processable Processable) {
	msg := processable.Value().(*sarama.ConsumerMessage)

	p.Lock()
	defer p.Unlock()

	delete(p.inFlightOffsets[msg.Partition], msg.Offset)
}

func (p *SaramaGroupProcessor) OnTrack() {
	p.RLock()
	defer p.RUnlock()

	offsets := make(map[int32]int64)

	// when all messages have been processed p.inFlightOffsets is empty, so we
	// use the latest loaded offset for commit instead
	for partition, offset := range p.loadedOffsets {
		offsets[partition] = offset
	}

	for partition, offsetMap := range p.inFlightOffsets {
		for offset := range offsetMap {
			if offsets[partition] == 0 || offset < offsets[partition] {
				offsets[partition] = offset
			}
		}
	}

	for partition, offset := range offsets {
		p.consumer.MarkOffset(p.Topic, partition, offset+1, "")
	}
}

// Close all pools, save offsets and close Kafka-connections
func (p *SaramaGroupProcessor) Close() {
	p.log.Info("save consumer offsets")
	p.OnTrack()

	p.log.Info("consumer group shutdown")
	// nolint: errcheck
	p.log.Error(p.consumer.Close(), "consumer group shutdown failed")

	p.log.Info("kafka client shutdown")
	// nolint: errcheck
	p.log.Error(p.client.Close(), "kafka client shutdown failed")

	p.log.Info("message pool shutdown")
	p.messagePool.Close()

	p.log.Infof("processor shutdown done")
}

type SaramaTopicProcessor struct {
	DefaultProcessor

	*SaramaProcessorConfig

	sync.RWMutex

	client   sarama.Client
	consumer sarama.TopicConsumer

	messages    chan interface{}
	messagePool *wp.Pool

	initialOffsets map[int32]int64
}

func NewSaramaTopicProcessor(offsets map[int32]int64, config *SaramaProcessorConfig) (p *SaramaTopicProcessor, err error) {
	p = &SaramaTopicProcessor{SaramaProcessorConfig: config, initialOffsets: offsets}
	if err = p.init(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *SaramaTopicProcessor) init() (err error) {
	p.ID = fmt.Sprintf("%v.%v", p.Name, p.Topic)

	if err = p.DefaultProcessor.New(); err != nil {
		return err
	}

	if p.Config == nil {
		p.Config = sarama.NewConfig()
		p.Config.Consumer.MaxProcessingTime = 30 * time.Second
		p.Config.Consumer.Offsets.Initial = sarama.OffsetOldest
		p.Config.Group.Return.Notifications = true
	}

	p.Config.ClientID = p.ID
	p.Config.Version = sarama.V0_10_0_0

	p.client, err = sarama.NewClient(
		strings.Split(p.Brokers, ","),
		p.Config,
	)

	if err != nil {
		return err
	}

	p.consumer, err = sarama.NewTopicConsumer(p.client, p.Topic, p.initialOffsets)

	if err != nil {
		return err
	}

	p.messages = make(chan interface{})
	p.messagePool = wp.NewPool(p.ID+".messages", 1, p.messageWorker)
	p.messagePool.Run()

	return nil
}

func (p *SaramaTopicProcessor) messageWorker(w *wp.Worker) {
	for {
		select {
		case <-w.Closer():
			w.Done()
			return
		case msg, ok := <-p.consumer.Messages():
			if ok {
				p.messages <- msg
			} else {
				w.Done()
				return
			}
		}
	}
}

func (p *SaramaTopicProcessor) Messages() chan interface{} {
	return p.messages
}

func (p *SaramaTopicProcessor) Wait() {
	p.messagePool.Wait()
}

// Close all pools, save offsets and close Kafka-connections
func (p *SaramaTopicProcessor) Close() {
	p.log.Info("message pool shutdown")
	p.messagePool.Close()

	p.log.Info("consumer shutdown")
	p.log.Error(p.consumer.Close(), "consumer shutdown failed")

	p.log.Info("sarama client shutdown")
	p.client.Close()

	p.log.Infof("processor shutdown done")
}
