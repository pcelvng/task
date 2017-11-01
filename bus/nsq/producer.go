package nsq

import (
	"errors"
	"strconv"
	"sync"

	"github.com/bitly/go-hostpool"
	gonsq "github.com/bitly/go-nsq"
)

func NewProducer(c *Config) *Producer {
	return &Producer{
		conf:     c,
		nsqConf:  gonsq.NewConfig(),
		numConns: 1,
	}
}

type Producer struct {
	conf      *Config
	nsqConf   *gonsq.Config
	producers map[string]*gonsq.Producer
	hostPool  hostpool.HostPool

	// number connections to each nsqd
	numConns int

	// mutex for hostpool access
	sync.Mutex
}

// Connect will connect to all the nsqds
// specified in Config.
func (p *Producer) Connect() error {
	// make the producers
	producers := make(map[string]*gonsq.Producer)
	for _, host := range p.conf.NSQdAddrs {
		for i := 0; i < p.numConns; i++ {
			producer, err := gonsq.NewProducer(host, p.nsqConf)
			if err != nil {
				return err
			}

			// set custom logger
			if p.conf.Logger != nil {
				producer.SetLogger(p.conf.Logger, p.conf.LogLvl)
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
	if p.producers == nil || len(p.producers) == 0 {
		return errors.New("no producers to send to")
	}

	r := p.hostPool.Get()
	producer := p.producers[r.Host()]

	if len(msg) == 0 {
		return nil
	}

	if len(topic) == 0 {
		return errors.New("no topic provided")
	}

	return producer.Publish(topic, msg)
}

func (p *Producer) Stop() error {
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
