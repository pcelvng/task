package main

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
	"github.com/pcelvng/task/util"
)

func NewRetryer(conf *Config) (*Retryer, error) {
	// make consumer
	c, err := util.NewConsumer(conf.BusesConfig)
	if err != nil {
		return nil, err
	}

	// make producer
	p, err := util.NewProducer(conf.BusesConfig)
	if err != nil {
		return nil, err
	}

	return &Retryer{
		conf:       conf,
		consumer:   c,
		producer:   p,
		rules:      conf.RetryRules,
		closeChan:  make(chan interface{}),
		retryCache: make(map[string]int),
		rulesMap:   make(map[string]*RetryRule),
	}, nil
}

type Retryer struct {
	conf       *Config
	consumer   bus.Consumer
	producer   bus.Producer
	rules      []*RetryRule
	rulesMap   map[string]*RetryRule // key is the task type
	closeChan  chan interface{}
	retryCache map[string]int // holds retry counts
	sync.Mutex                // mutex for updating the retryCache
}

// Start will:
// - load the retry rules
// - connect the consumer
// - connect the producer
// - begin listening for error tasks
func (r *Retryer) Start() error {
	if r.consumer == nil {
		return errors.New("unable to start - no consumer")
	}

	if r.producer == nil {
		return errors.New("unable to start - no producer")
	}

	// start consumer
	if err := r.consumer.Connect(r.conf.DoneTopic, r.conf.DoneChannel); err != nil {
		return err
	}

	// start producer
	if err := r.producer.Connect(); err != nil {
		return err
	}

	// load rules into rules map
	r.LoadRules()

	// TODO: ability to load retry state from a file
	// r.LoadRetries() // for now will log the retry state

	// start listening for error tasks
	r.Listen()

	return nil
}

// LoadRules will load all the retry rules into
// a local map for easier access.
func (r *Retryer) LoadRules() {
	for _, rule := range r.rules {
		key := rule.TaskType
		r.rulesMap[key] = rule
	}
}

// Listen will start the listen loop to listen
// for failed tasks and then handle those failed
// tasks.
func (r *Retryer) Listen() {
	go r.listen()
}

func (r *Retryer) listen() {
	for {
		// give the closeChan a change
		// to break the loop.
		select {
		case <-r.closeChan:
			return
		default:
		}

		// wait for a task
		msg, done, err := r.consumer.Msg()
		if err != nil {
			log.Println(err.Error())
			if done {
				return
			}
			continue
		}

		// attempt to create task
		var tsk *task.Task
		if len(msg) > 0 {
			tsk, err = task.NewFromBytes(msg)
			if err != nil {
				log.Println(err.Error())
				if done {
					return
				}
				continue
			}

			// evaluate task
			r.applyRule(tsk)
		}

		if done {
			return
		}
	}
}

// applyRule will
// - discover if the task is an error
// - look for a retry rule to apply
// - if the task needs to be retried then it is returned
// - if the task does not need to be retried the nil is returned
func (r *Retryer) applyRule(tsk *task.Task) {
	rule, ok := r.rulesMap[tsk.Type]
	if !ok {
		return
	}

	key := makeCacheKey(tsk)
	r.Lock()
	defer r.Unlock()
	cnt, _ := r.retryCache[key]

	if tsk.IsErr() {
		if cnt < rule.Retries {
			r.retryCache[key] = cnt + 1
			go r.doRetry(tsk, rule)
		}
	} else if cnt > 0 {
		delete(r.retryCache, key)
	}
}

// doRetry will wait (if requested by the rule)
// and then send the task to the outgoing channel
func (r *Retryer) doRetry(tsk *task.Task, rule *RetryRule) {
	time.Sleep(time.Minute * rule.Wait.Duration)

	// create a new task just like the old one
	// and send it out.
	nTsk := task.New(tsk.Type, tsk.Task)

	topic := rule.TaskType
	if rule.Topic != "" {
		topic = rule.Topic
	}
	msg, err := nTsk.Bytes()
	if err != nil {
		log.Println(err.Error())
		return
	}

	err = r.producer.Send(topic, msg)
	if err != nil {
		log.Println(err.Error())
		return
	}
}

func (r *Retryer) Close() error {
	// send close signal
	close(r.closeChan)

	// close the consumer
	if err := r.consumer.Close(); err != nil {
		return err
	}

	// close the producer
	if err := r.producer.Close(); err != nil {
		return err
	}

	return nil
}

// makeCacheKey will make a key string of the format:
// "task.Type" + "task.Task"
func makeCacheKey(tsk *task.Task) string {
	return tsk.Type + tsk.Task
}
