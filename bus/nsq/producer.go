package nsqbus

import (
	"errors"
	"strconv"

	"github.com/bitly/go-hostpool"
	nsq "github.com/bitly/go-nsq"
)

func NewProducer(c *Config) *Producer {
	return &Producer{
		conf:     c,
		nsqConf:  nsq.NewConfig(),
		numConns: 3,
	}
}

type Producer struct {
	conf      *Config
	nsqConf   *nsq.Config
	producers map[string]*nsq.Producer
	hostPool  hostpool.HostPool

	// number connections to each nsqd
	numConns int
}

// Connect will connect to all the nsqds
// specified in Config.
func (p *Producer) Connect() error {
	// make the producers
	producers := make(map[string]*nsq.Producer)
	for _, host := range p.conf.NSQdAddrs {
		for i := 0; i < p.numConns; i++ {
			producer, err := nsq.NewProducer(host, p.nsqConf)
			if err != nil {
				return err
			}

			// check that producer has good host
			//err = producer.Ping()
			//if err != nil {
			//	return err
			//}

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

func (p *Producer) Close() error {
	if p.hostPool != nil {
		p.hostPool.Close()
	}

	return nil
}
