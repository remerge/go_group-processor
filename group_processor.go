package groupprocessor

import (
	"time"

	"github.com/remerge/cue"
	wp "github.com/remerge/go-worker_pool"
)

// GroupProcessor can process messages in paralell using a LoadSaver and a
// Processor implementation
type GroupProcessor struct {
	Name string

	Processor Processor
	LoadSaver LoadSaver

	MaxRetries    int
	NumLoadWorker int
	NumSaveWorker int
	TrackInterval time.Duration

	loadPool  *wp.Pool
	savePool  *wp.Pool
	trackPool *wp.Pool

	log cue.Logger
}

// New initializes the GroupProcessor once it's instantiated
func (gp *GroupProcessor) New() (err error) {
	gp.log = cue.NewLogger(gp.Name)

	gp.loadPool = wp.NewPool(gp.Name+".load", gp.NumLoadWorker, gp.loadWorker)
	gp.savePool = wp.NewPool(gp.Name+".save", gp.NumSaveWorker, gp.saveWorker)

	if gp.TrackInterval > 0 {
		gp.trackPool = wp.NewPool(gp.Name+".track", 1, gp.trackWorker)
	}

	return nil
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
			gp.Processor.OnTrack()
		}
	}
}

func (gp *GroupProcessor) loadMsg(msg interface{}) {
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
	err = gp.LoadSaver.Save(processable)

	var processed bool
	if err != nil {
		processed = gp.LoadSaver.Fail(processable, err)
	} else {
		processed = gp.LoadSaver.Done(processable)
	}

	if processed {
		gp.Processor.OnProcessed(processable)
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

		if err = gp.trySaveMsg(processable); err == nil {
			return
		}
	}

	gp.Processor.OnSkip(processable, err)
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
	gp.trackPool.Run()
	gp.savePool.Run()
	gp.loadPool.Run()
}

// Close all pools
func (gp *GroupProcessor) Close() {
	gp.log.Info("load pool shutdown")
	gp.loadPool.Close()

	gp.log.Info("save pool shutdown")
	gp.savePool.Close()

	gp.log.Info("track pool shutdown")
	gp.trackPool.Close()

	gp.log.Info("processor shutdown")
	gp.Processor.Close()

	gp.log.Infof("group processor shutdown done")
}
