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

// GroupProcessor is a Kafka helper to read and process a Kafka topic
type GroupProcessor struct {
	sync.RWMutex

	Name     string
	Brokers  string
	Topic    string
	GroupGen int

	NumLoadWorker int
	NumSaveWorker int

	LoadSaver LoadSaver
	loadPool  *wp.Pool
	savePool  *wp.Pool
	trackPool *wp.Pool

	loadedOffsets   map[int32]int64
	inFlightOffsets map[int32]map[int64]*sarama.ConsumerMessage

	log cue.Logger

	kafka struct {
		config   *sarama.Config
		client   sarama.Client
		consumer sarama.ConsumerGroup
	}

	CustomKafkaConfig *sarama.Config
}

// New initializes the GroupProcessor once it's instantiated
func (gp *GroupProcessor) New() (err error) {
	id := fmt.Sprintf("%v.%v", gp.Name, gp.Topic)

	gp.log = cue.NewLogger(id)

	gp.kafka.config = gp.CustomKafkaConfig
	if gp.kafka.config == nil {
		gp.kafka.config = sarama.NewConfig()
		gp.kafka.config.Consumer.MaxProcessingTime = 30 * time.Second
		gp.kafka.config.Consumer.Offsets.Initial = sarama.OffsetOldest
		gp.kafka.config.Group.Return.Notifications = true
	}

	gp.kafka.config.ClientID = id
	gp.kafka.config.Version = sarama.V0_10_0_0

	gp.kafka.client, err = sarama.NewClient(
		strings.Split(gp.Brokers, ","),
		gp.kafka.config,
	)

	if err != nil {
		return err
	}

	group := fmt.Sprintf("%s.%s.%d", gp.Name, gp.Topic, gp.GroupGen)
	gp.kafka.consumer, err = sarama.NewConsumerGroupFromClient(
		gp.kafka.client,
		group,
		[]string{gp.Topic},
	)

	if err != nil {
		return err
	}

	gp.loadPool = wp.NewPool(id+".load", gp.NumLoadWorker, gp.loadWorker)
	gp.savePool = wp.NewPool(id+".save", gp.NumSaveWorker, gp.saveWorker)
	gp.trackPool = wp.NewPool(id+".track", 1, gp.trackWorker)

	gp.loadedOffsets = make(map[int32]int64)
	gp.inFlightOffsets = make(map[int32]map[int64]*sarama.ConsumerMessage)

	return nil
}

func (gp *GroupProcessor) SetOffsets(offsets map[int32]int64) {
	gp.RLock()
	defer gp.RUnlock()

	for partition, offset := range offsets {
		oldest, err := gp.kafka.client.GetOffset(gp.Topic, partition, sarama.OffsetOldest)

		if err != nil {
			gp.log.Panicf(err, "Couldn't get offsets for %s/%d. Resuming from default offset", gp.Topic, partition)
			continue
		}

		newest, err := gp.kafka.client.GetOffset(gp.Topic, partition, sarama.OffsetNewest)

		if err != nil {
			gp.log.Panicf(err, "Couldn't get offsets for %s/%d. Resuming from default offset", gp.Topic, partition)
			continue
		}

		if offset < oldest {
			gp.log.Warnf("Offset of %s/%d out of range. Corrected offset from %d to %d.", gp.Topic, partition, offset, oldest)
			offset = oldest
		}

		if offset > newest {
			gp.log.Warnf("Offset of %s/%d out of range. Corrected offset from %d to %d.", gp.Topic, partition, offset, newest)
			offset = newest
		}

		gp.kafka.consumer.ResetOffset(gp.Topic, partition, offset, "")
		gp.kafka.consumer.MarkOffset(gp.Topic, partition, offset, "")
	}
}

func (gp *GroupProcessor) saveOffsets() {
	gp.RLock()
	defer gp.RUnlock()

	offsets := make(map[int32]int64)

	// when all messages have been processed gp.inFlightOffsets is empty, so we
	// use the latest loaded offset for commit instead
	for partition, offset := range gp.loadedOffsets {
		offsets[partition] = offset
	}

	for partition, offsetMap := range gp.inFlightOffsets {
		for offset := range offsetMap {
			if offsets[partition] == 0 || offset < offsets[partition] {
				offsets[partition] = offset
			}
		}
	}

	for partition, offset := range offsets {
		gp.kafka.consumer.MarkOffset(gp.Topic, partition, offset, "")
	}
}

func (gp *GroupProcessor) trackWorker(w *wp.Worker) {
	t := time.NewTicker(gp.kafka.config.Consumer.Offsets.CommitInterval)

	for {
		select {
		case <-w.Closer():
			t.Stop()
			w.Done()
			return
		case n, ok := <-gp.kafka.consumer.Notifications():
			if !ok {
				gp.log.Warn("trying to read from closed notification channel")
				continue
			}
			gp.log.WithFields(cue.Fields{
				"added":    n.Claimed,
				"current":  n.Current,
				"released": n.Released,
			}).Infof("group rebalance")
		case <-t.C:
			gp.saveOffsets()
		}
	}
}

func msgID(processable Processable) int {
	key := processable.Msg().Key

	if key != nil && len(key) > 0 {
		hash := fnv.New64a()
		// #nosec
		hash.Write(key)
		return int(hash.Sum64())
	}

	return rand.Int()
}

func (gp *GroupProcessor) storeLoadedOffset(msg *sarama.ConsumerMessage) {
	gp.Lock()
	defer gp.Unlock()

	if gp.loadedOffsets[msg.Partition] < msg.Offset {
		gp.loadedOffsets[msg.Partition] = msg.Offset
	}

	if gp.inFlightOffsets[msg.Partition] == nil {
		gp.inFlightOffsets[msg.Partition] = make(
			map[int64]*sarama.ConsumerMessage,
		)
	}

	gp.inFlightOffsets[msg.Partition][msg.Offset] = msg
}

func (gp *GroupProcessor) loadMsg(msg *sarama.ConsumerMessage) {
	processable := gp.LoadSaver.Load(msg)

	if processable != nil {
		gp.storeLoadedOffset(msg)
		gp.savePool.Send(msgID(processable), processable)
	}
}

func (gp *GroupProcessor) loadWorker(w *wp.Worker) {
	for {
		select {
		case <-w.Closer():
			w.Done()
			return
		case msg, ok := <-gp.kafka.consumer.Messages():
			if !ok {
				gp.log.Warn("trying to read from closed consumer channel")
				continue
			}

			gp.loadMsg(msg)
		}
	}
}

func (gp *GroupProcessor) removeLoadedOffset(processable Processable) {
	gp.Lock()
	defer gp.Unlock()

	msg := processable.Msg()
	delete(gp.inFlightOffsets[msg.Partition], msg.Offset)
}

func (gp *GroupProcessor) saveMsg(value interface{}) {
	processable := value.(Processable)
	err := gp.LoadSaver.Save(processable)

	var processed bool
	if err != nil {
		processed = gp.LoadSaver.Fail(processable, err)
	} else {
		processed = gp.LoadSaver.Done(processable)
	}

	if processed {
		gp.removeLoadedOffset(processable)
	} else {
		// retry
		gp.saveMsg(value)
	}
}

func (gp *GroupProcessor) saveWorker(w *wp.Worker) {
	for {
		select {
		case <-w.Closer():
			w.Done()
			return
		case msg, ok := <-w.Channel():
			if !ok {
				gp.log.Warn("trying to read from closed worker channel")
				continue
			}

			gp.saveMsg(msg)
		}
	}
}

// Run the GroupProcessor consisting of trackPool, savePool and loadPool
func (gp *GroupProcessor) Run() {
	gp.trackPool.Run()
	gp.savePool.Run()
	gp.loadPool.Run()
}

// Close all pools, save offsets and close Kafka-connections
func (gp *GroupProcessor) Close() {
	gp.log.Info("load pool shutdown")
	gp.loadPool.Close()

	gp.log.Info("save pool shutdown")
	gp.savePool.Close()

	gp.log.Info("track pool shutdown")
	gp.trackPool.Close()

	gp.log.Info("save consumer offsets")
	gp.saveOffsets()

	gp.log.Info("consumer group shutdown")
	// #nosec
	gp.log.Error(gp.kafka.consumer.Close(), "consumer group shutdown failed")

	gp.log.Info("kafka client shutdown")
	// #nosec
	gp.log.Error(gp.kafka.client.Close(), "kafka client shutdown failed")

	gp.log.Infof("group processor shutdown done")
}
