package groupprocessor

import (
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bobziuchkovski/cue"
	metrics "github.com/rcrowley/go-metrics"
	wp "github.com/remerge/go-worker_pool"
	rand "github.com/remerge/go-xorshift"
)

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

	processed  metrics.Counter
	loadErrors metrics.Counter
	saveErrors metrics.Counter
}

func (gp *GroupProcessor) New() (err error) {
	id := fmt.Sprintf("%v.%v", gp.Name, gp.Topic)

	gp.log = cue.NewLogger(id)

	gp.kafka.config = sarama.NewConfig()
	gp.kafka.config.ClientID = id
	gp.kafka.config.Version = sarama.V0_10_0_0
	gp.kafka.config.Consumer.MaxProcessingTime = 30 * time.Second
	gp.kafka.config.Consumer.Offsets.Initial = sarama.OffsetOldest
	gp.kafka.config.Group.Return.Notifications = true

	gp.kafka.client, err = sarama.NewClient(
		strings.Split(gp.Brokers, ","), gp.kafka.config)

	if err != nil {
		return err
	}

	group := fmt.Sprintf("%s.%s.%d", gp.Name, gp.Topic, gp.GroupGen)
	gp.kafka.consumer, err = sarama.NewConsumerGroupFromClient(
		gp.kafka.client, group, []string{gp.Topic})

	if err != nil {
		return err
	}

	gp.loadPool = wp.NewPool(id+".load", gp.NumLoadWorker, gp.loadWorker)
	gp.savePool = wp.NewPool(id+".save", gp.NumSaveWorker, gp.saveWorker)
	gp.trackPool = wp.NewPool(id+".track", 1, gp.trackWorker)

	gp.loadedOffsets = make(map[int32]int64)
	gp.inFlightOffsets = make(map[int32]map[int64]*sarama.ConsumerMessage)

	prefix := fmt.Sprintf("group_processor,name=%s,topic=%s ",
		gp.Name, gp.Topic)

	gp.processed = metrics.GetOrRegisterCounter(prefix+"msg", nil)
	gp.loadErrors = metrics.GetOrRegisterCounter(prefix+"load_error", nil)
	gp.saveErrors = metrics.GetOrRegisterCounter(prefix+"save_error", nil)

	return nil
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
	for {
		select {
		case <-w.Closer():
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
		case <-time.After(10 * time.Second):
			gp.saveOffsets()
		}
	}
}

func msgId(processable Processable) int {
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

func (gp *GroupProcessor) loadMsg(msg *sarama.ConsumerMessage) error {
	processable, err := gp.LoadSaver.Load(msg)
	if err != nil {
		return err
	}

	gp.storeLoadedOffset(msg)
	gp.savePool.Send(msgId(processable), processable)

	return nil
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

			if err := gp.loadMsg(msg); err != nil {
				// #nosec
				gp.log.Error(err, "load failed")
				gp.loadErrors.Inc(1)
			}
		}
	}
}

func (gp *GroupProcessor) saveMsg(value interface{}) error {
	processable := value.(Processable)

	err := gp.LoadSaver.Save(processable)
	if err != nil {
		return err
	}

	gp.processed.Inc(1)

	gp.Lock()
	defer gp.Unlock()

	msg := processable.Msg()
	delete(gp.inFlightOffsets[msg.Partition], msg.Offset)

	return nil
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

			if err := gp.saveMsg(msg); err != nil {
				// #nosec
				gp.log.Error(err, "save failed")
				gp.saveErrors.Inc(1)
			}
		}
	}
}

func (gp *GroupProcessor) Run() {
	gp.trackPool.Run()
	gp.savePool.Run()
	gp.loadPool.Run()
}

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
