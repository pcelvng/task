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
