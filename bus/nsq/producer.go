package nsq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/nsqio/go-hostpool"
	gonsq "github.com/nsqio/go-nsq"
)

func NewProducer(opt *Option) (*Producer, error) {
	if opt == nil {
		opt = &Option{}
	}

	// context for clean shutdown
	ctx, cncl := context.WithCancel(context.Background())

	p := &Producer{
		opt:      opt,
		nsqConf:  gonsq.NewConfig(),
		numConns: 1,
		ctx:      ctx,
		cncl:     cncl,
	}

	// setup and test connection
	if err := p.connect(); err != nil {
		return nil, err
	}

	return p, nil
}

type Producer struct {
	opt       *Option
	nsqConf   *gonsq.Config
	producers map[string]*gonsq.Producer
	hostPool  hostpool.HostPool

	// number connections to each nsqd
	numConns int

	// mutex for hostpool access
	sync.Mutex

	ctx  context.Context
	cncl context.CancelFunc
}

// connect will connect to all the nsqds
// specified in Config.
func (p *Producer) connect() error {
	// make the producers
	producers := make(map[string]*gonsq.Producer)
	for _, host := range p.opt.NSQdAddrs {
		for i := 0; i < p.numConns; i++ {
			producer, err := gonsq.NewProducer(host, p.nsqConf)
			if err != nil {
				return err
			}

			// set custom logger
			if p.opt.Logger != nil {
				producer.SetLogger(p.opt.Logger, p.opt.LogLvl)
			}

			// check that producer has good host
			err = producer.Ping()
			if err != nil {
				return err
			}

			pk := strconv.Itoa(i) + "|" + host
			producers[pk] = producer
		}
	}

	if len(producers) == 0 {
		return errors.New("no nsqd hosts provided")
	}

	keys := make([]string, 0)
	for key := range producers {
		keys = append(keys, key)
	}

	p.hostPool = hostpool.New(keys)
	p.producers = producers

	return nil
}

func (p *Producer) Send(topic string, msg []byte) error {
	p.Lock()
	defer p.Unlock()

	// should not attempt to send if producer already stopped.
	if p.ctx.Err() != nil {
		errMsg := fmt.Sprintf("unable to send '%v'; producer already stopped", string(msg))
		return errors.New(errMsg)
	}

	if p.producers == nil || len(p.producers) == 0 {
		return errors.New("no producers to send to")
	}

	if len(msg) == 0 {
		return nil
	}

	if len(topic) == 0 {
		return errors.New("no topic provided")
	}

	r := p.hostPool.Get()
	producer := p.producers[r.Host()]

	return producer.Publish(topic, msg)
}

func (p *Producer) Stop() error {
	if p.ctx.Err() != nil {
		return nil
	}
	p.cncl()

	if p.hostPool != nil {
		p.hostPool.Close()
	}

	// close up all producers for in-flight produced messages
	// Will tell all producers stop at once and then wait
	// for them all to stop.
	wg := sync.WaitGroup{}
	for _, producer := range p.producers {
		wg.Add(1)
		go func() {
			producer.Stop()
			wg.Done()
		}()
	}

	return nil
}
