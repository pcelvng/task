package nsq

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sort"

	gonsq "github.com/nsqio/go-nsq"
)

// Option is used for instantiating an NSQ Consumer or
// Producer. The Producer will ignore the Topic value.
type Option struct {
	NSQdAddrs    []string // connects via TCP only
	LookupdAddrs []string // connects via HTTP only

	// if nil then the default nsq logger is used
	Logger *log.Logger

	// default is nsq.LogLevelInfo. Only set if a
	// custom logger is provided.
	LogLvl gonsq.LogLevel
}

func Topics(hosts []string) ([]string, error) {
	topicMap := make(map[string]struct{})
	for _, host := range hosts {
		topics, err := getTopics(host)
		if err != nil {
			return nil, err
		}
		// dedup same topics from different hosts
		for _, t := range topics {
			topicMap[t] = struct{}{}
		}
	}
	topics := make([]string, 0, len(topicMap))
	for t := range topicMap {
		topics = append(topics, t)
	}
	sort.Strings(topics)
	return topics, nil
}

func getTopics(host string) ([]string, error) {
	resp, err := http.Get(fmt.Sprintf("http://%v/topics", host))
	if err != nil {
		return nil, err
	}
	body, _ := ioutil.ReadAll(resp.Body)
	r := &topicResponse{}
	err = json.Unmarshal(body, r)
	if len(r.Topics) != 0 {
		return r.Topics, err
	}
	return r.Data.Topics, err
}

type topicResponse struct {
	Topics []string `json:"topics"`
	Data   struct {
		Topics []string `json:"topics"`
	} `json:"data"`
}

func (o *Option) createTopicIfNotFound(topic string) error {
	if len(o.LookupdAddrs) == 0 {
		return fmt.Errorf("lookupd host required")
	}
	resp, err := http.Get(fmt.Sprintf("http://%v/lookup?topic=%s", o.LookupdAddrs[0], topic))
	if err != nil || resp.StatusCode == http.StatusOK {
		return err // topic found or issue with request
	}
	// lookup nodes
	type Producer struct {
		Address string `json:"broadcast_address"`
		Port    int    `json:"http_port"`
	}
	data := struct {
		Producer []Producer `json:"producers"`
	}{}
	resp, _ = http.Get(fmt.Sprintf("http://%v/nodes", o.LookupdAddrs[0]))
	b, _ := ioutil.ReadAll(resp.Body)
	json.Unmarshal(b, &data)
	for _, nsq := range data.Producer {
		resp, _ := http.Post(fmt.Sprintf("http://%s:%d/topic/create?topic=%s", nsq.Address, nsq.Port, topic), "", nil)
		if resp.StatusCode != http.StatusOK {
			err = fmt.Errorf("%v: %v", nsq, resp.Status)
			log.Println(err)
		}
	}

	return err
}
