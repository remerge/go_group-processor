package groupprocessor

import (
	aerospike "github.com/aerospike/aerospike-client-go"
	wp "github.com/remerge/go-worker_pool"
)

type AerospikeProcessor struct {
	DefaultProcessor

	Name      string
	Host      *aerospike.Host
	Namespace string
	Set       string
	Bins      []string

	client  *aerospike.Client
	scanner *aerospike.Recordset

	messages    chan interface{}
	messagePool *wp.Pool
}

func (p *AerospikeProcessor) New() (err error) {
	p.ID = p.Name

	if err = p.DefaultProcessor.New(); err != nil {
		return err
	}

	policy := aerospike.NewClientPolicy()
	policy.UseServicesAlternate = true

	p.client, err = aerospike.NewClientWithPolicyAndHost(policy, p.Host)
	if err != nil {
		return err
	}

	scanPolicy := aerospike.NewScanPolicy()
	scanPolicy.FailOnClusterChange = false

	p.scanner, err = p.client.ScanAll(scanPolicy, p.Namespace, p.Set, p.Bins...)
	if err != nil {
		return err
	}

	p.messages = make(chan interface{})
	p.messagePool = wp.NewPool(p.Name+".messages", 1, p.messageWorker)
	p.messagePool.Run()

	return nil
}

func (p *AerospikeProcessor) messageWorker(w *wp.Worker) {
	for {
		select {
		case <-w.Closer():
			w.Done()
			return
		case msg, ok := <-p.scanner.Records:
			if ok {
				p.messages <- msg
			} else {
				w.Done()
				return
			}
		case err, ok := <-p.scanner.Errors:
			if !ok {
				break
			}
			// nolint: errcheck
			p.log.Error(err, "scan error")
			w.Done()
			return
		}
	}
}

func (p *AerospikeProcessor) Messages() chan interface{} {
	return p.messages
}

func (p *AerospikeProcessor) Wait() {
	p.messagePool.Wait()
}

// Close all pools, save offsets and close Kafka-connections
func (p *AerospikeProcessor) Close() {
	p.log.Info("message pool shutdown")
	p.messagePool.Close()

	p.log.Info("scanner shutdown")
	// nolint: errcheck
	p.log.Error(p.scanner.Close(), "scanner shutdown failed")

	p.log.Info("aerospike client shutdown")
	p.client.Close()

	p.log.Infof("processor shutdown done")
}
