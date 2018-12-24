package groupprocessor

import (
	"errors"
	"time"

	metrics "github.com/rcrowley/go-metrics"
	"github.com/remerge/cue"
	wp "github.com/remerge/go-worker_pool"
)

// GroupProcessor can process messages in parallel using a LoadSaver and a
// Processor implementation
type GroupProcessor struct {
	*Config

	loadPool  *wp.Pool
	savePool  *wp.Pool
	trackPool *wp.Pool

	loaded    metrics.Timer
	processed metrics.Timer
	retries   metrics.Counter
	skipped   metrics.Counter

	log cue.Logger
}

// Config is the configuration for a GroupProcessor
type Config struct {
	Name string

	MaxRetries    int
	NumLoadWorker int
	NumSaveWorker int
	TrackInterval time.Duration

	Processor Processor
	LoadSaver LoadSaver
}

// New creates a new GroupProcessor
func New(config *Config) (gp *GroupProcessor, err error) {
	gp = &GroupProcessor{Config: config}
	if err = gp.init(); err != nil {
		return nil, err
	}
	return gp, nil
}

func (gp *GroupProcessor) init() (err error) {
	if gp.Processor == nil {
		return errors.New("Processor does not exist")
	}
	if gp.LoadSaver == nil {
		return errors.New("LoadSaver does not exist")
	}

	gp.log = cue.NewLogger(gp.Name)

	gp.loadPool = wp.NewPool(gp.Name+".load", gp.NumLoadWorker, gp.loadWorker)
	gp.savePool = wp.NewPool(gp.Name+".save", gp.NumSaveWorker, gp.saveWorker)

	if gp.TrackInterval > 0 {
		gp.trackPool = wp.NewPool(gp.Name+".track", 1, gp.trackWorker)
	}

	gp.loaded = metrics.GetOrRegisterTimer(gp.Name+" loaded", nil)
	gp.processed = metrics.GetOrRegisterTimer(gp.Name+" processed", nil)
	gp.retries = metrics.GetOrRegisterCounter(gp.Name+" retry", nil)
	gp.skipped = metrics.GetOrRegisterCounter(gp.Name+" skip", nil)

	return nil
}

func (gp *GroupProcessor) logMetrics() {
	gp.log.WithFields(cue.Fields{
		"loaded":      gp.loaded.Count(),
		"load_p95":    time.Duration(int64(gp.loaded.Percentile(0.95))),
		"load_m1":     int64(gp.loaded.Rate1()),
		"processed":   gp.processed.Count(),
		"process_p95": time.Duration(int64(gp.processed.Percentile(0.95))),
		"process_m1":  int64(gp.processed.Rate1()),
		"retries":     gp.retries.Count(),
		"skipped":     gp.skipped.Count(),
	}).Debug("messages")
}

func (gp *GroupProcessor) trackWorker(w *wp.Worker) {
	t := time.NewTicker(gp.TrackInterval)

	for {
		select {
		case <-w.Closer():
			t.Stop()
			w.Done()
			return
		case <-t.C:
			gp.logMetrics()
			gp.Processor.OnTrack()
		}
	}
}

func (gp *GroupProcessor) loadMsg(msg interface{}) {
	start := time.Now()
	defer gp.loaded.UpdateSince(start)

	processable := gp.LoadSaver.Load(msg)

	if processable != nil {
		gp.Processor.OnLoad(processable)
		gp.savePool.Send(processable.Key(), processable)
	}
}

func (gp *GroupProcessor) loadWorker(w *wp.Worker) {
	for {
		select {
		case <-w.Closer():
			w.Done()
			return
		case msg, ok := <-gp.Processor.Messages():
			if ok {
				gp.loadMsg(msg)
			} else {
				gp.log.Warn("trying to read from closed channel")
				w.Done()
				return
			}
		}
	}
}

func (gp *GroupProcessor) trySaveMsg(processable Processable) (err error) {
	start := time.Now()

	err = gp.LoadSaver.Save(processable)

	var processed bool
	if err != nil {
		processed = gp.LoadSaver.Fail(processable, err)
	} else {
		processed = gp.LoadSaver.Done(processable)
	}

	if processed {
		gp.Processor.OnProcessed(processable)
		gp.processed.UpdateSince(start)
		return nil
	}

	return err
}

func (gp *GroupProcessor) saveMsg(processable Processable) {
	var err error

	if err = gp.trySaveMsg(processable); err == nil {
		return
	}

	for i := 0; i < gp.MaxRetries; i++ {
		gp.Processor.OnRetry(processable)
		gp.retries.Inc(1)

		if err = gp.trySaveMsg(processable); err == nil {
			return
		}
	}

	gp.Processor.OnSkip(processable, err)
	gp.skipped.Inc(1)
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

			gp.saveMsg(msg.(Processable))
		}
	}
}

// Run the GroupProcessor consisting of trackPool, savePool and loadPool
func (gp *GroupProcessor) Run() {
	if gp.trackPool != nil {
		gp.trackPool.Run()
	}
	gp.savePool.Run()
	gp.loadPool.Run()
}

// Close all pools
func (gp *GroupProcessor) Close() {
	gp.log.Info("processor shutdown")
	gp.Processor.Close()

	gp.log.Info("load pool shutdown")
	gp.loadPool.Close()

	gp.log.Info("save pool shutdown")
	gp.savePool.Close()

	if gp.trackPool != nil {
		gp.log.Info("track pool shutdown")
		gp.trackPool.Close()
		gp.trackPool = nil
	}

	gp.log.Infof("group processor shutdown done")
}
