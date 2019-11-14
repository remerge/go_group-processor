package groupprocessor

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
	rand "github.com/remerge/go-xorshift"
)

type SaramaProcessable struct {
	value *sarama.ConsumerMessage
}

func NewSaramaProcessable(value *sarama.ConsumerMessage) *SaramaProcessable {
	return &SaramaProcessable{
		value: value,
	}
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

type SaramaProcessorConfig struct {
	Name     string
	Brokers  string
	Topic    string
	GroupGen int
	Config   *sarama.Config
}

// SaramaProcessor is a Processor that reads messages from a Kafka topic with a
// group consumer and tracks offsets of processed messages
type SaramaProcessor struct {
	*SaramaProcessorConfig

	DefaultProcessor

	handler  *ProcessorConsumerGroupHandler
	consumer *Consumer

	messages chan interface{}
}

// NewSaramaProcessor create a new SaramaProcessor which includes a Kafka group consumer as source
// for messages for this processor look
func NewSaramaProcessor(config *SaramaProcessorConfig) (p *SaramaProcessor, err error) {
	p = &SaramaProcessor{
		SaramaProcessorConfig: config,
		messages:              make(chan interface{}),
	}
	p.ID = fmt.Sprintf("%v.%v", p.Name, p.Topic)
	if err = p.DefaultProcessor.New(); err != nil {
		return nil, err
	}
	p.handler = NewProcessorConsumerGroupHandler(p.messages)

	if p.Config == nil {
		p.Config = sarama.NewConfig()
		p.Config.Consumer.MaxProcessingTime = 30 * time.Second
		p.Config.Consumer.Offsets.Initial = sarama.OffsetOldest
		p.Config.Consumer.Return.Errors = true
	}

	p.Config.ClientID = p.ID
	p.Config.Version = sarama.V0_10_2_0

	p.consumer, err = Consume(
		context.Background(),
		p.Config,
		strings.Split(p.Brokers, ","),
		ConsumerConfig{
			GroupID: fmt.Sprintf("%s.%s.%d", p.Name, p.Topic, p.GroupGen),
			Topics:  []string{p.Topic},
			Handler: p.handler,
			OnError: func(e error) error {
				switch err1 := e.(type) {
				case *sarama.ConsumerError:
					if kafkaErr, isKafkaErr := err1.Err.(sarama.KError); isKafkaErr {
						switch kafkaErr {
						case sarama.ErrRequestTimedOut:
							return nil
						default:
							p.log.Warnf("unrecoverable consume error %v", kafkaErr)
							return e
						}
					}
					return e
				default:
					return e
				}
			},
		})
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (p *SaramaProcessor) Messages() chan interface{} {
	return p.messages
}

func (p *SaramaProcessor) OnLoad(_ Processable) {}

func (p *SaramaProcessor) OnProcessed(processable Processable) {
	msg := processable.Value().(*sarama.ConsumerMessage)
	_ = p.handler.manager.ConfirmMessage(msg)
	p.DefaultProcessor.OnProcessed(processable)
}

func (p *SaramaProcessor) OnSkip(processable Processable, err error) {
	msg := processable.Value().(*sarama.ConsumerMessage)
	_ = p.handler.manager.ConfirmMessage(msg)
	p.DefaultProcessor.OnSkip(processable, err)
}

// Close all pools, save offsets and close Kafka-connections
func (p *SaramaProcessor) Close() {
	p.log.Info("consumer group shutdown")
	p.log.Warnf("consumer group shutdown failed: %v", p.consumer.Close())

	p.log.Infof("processor shutdown done")
}

func (p *SaramaProcessor) Wait() error {
	if err := p.consumer.Wait(); err != nil {
		p.log.Warnf("consumer group failed: %v", err)
		return err
	}
	return nil
}

type ProcessorConsumerGroupHandler struct {
	messageChan     chan interface{}
	manager         *SequenceSessionManager
	metricsRegistry metrics.Registry
}

func NewProcessorConsumerGroupHandler(ch chan interface{}) *ProcessorConsumerGroupHandler {
	return &ProcessorConsumerGroupHandler{
		messageChan: ch,
		manager:     NewSequenceSessionManager(),
	}
}

func (h *ProcessorConsumerGroupHandler) Setup(sess sarama.ConsumerGroupSession) error {
	return h.manager.AttachSession(sess)
}

func (h *ProcessorConsumerGroupHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	return h.manager.ReleaseSession(sess)
}

func (h *ProcessorConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	g := metrics.GetOrRegisterGauge(fmt.Sprintf(
		"ggp,topic=%s,partition=%d,member=%s session", claim.Topic(), claim.Partition(), sess.MemberID()), nil)
	defer g.Update(0)
	g.Update(int64(sess.GenerationID()))
	for msg := range claim.Messages() {
		err := h.manager.DeclareMessage(sess, msg)
		if err != nil {
			return nil
		}
		h.messageChan <- msg
	}
	return nil
}
