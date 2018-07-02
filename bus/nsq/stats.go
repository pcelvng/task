package nsq

import (
	"io/ioutil"
	"net/http"

	"encoding/json"
	"fmt"
)

func getDepth(lookupd []string, topic, channel string) int {
	hosts := getHosts(lookupd)
	var depth int
	for _, v := range hosts {
		if sts := getStats(v, topic, channel); sts != nil {
			if len(sts.Data.Topics) == 0 {
				continue
			}
			if len(sts.Data.Topics[0].Channels) == 0 {
				continue
			}
			depth += sts.Data.Topics[0].Channels[0].Depth
		}
	}
	return depth
}

func getStats(host, topic, channel string) *nsqStats {
	resp, err := http.Get(fmt.Sprintf("http://%s/stats?format=json&topic=%s&channel=%s", host, topic, channel))
	if err != nil {
		return nil
	}
	body, _ := ioutil.ReadAll(resp.Body)
	sts := &nsqStats{}
	json.Unmarshal(body, sts)
	if sts.Code == 0 {
		json.Unmarshal(body, &sts.Data)
	}
	return sts
}

type nsqStats struct {
	Code int `json:"status_code"`
	Data struct {
		Topics []struct {
			Channels []struct {
				Depth int `json:"depth"`
			} `json:"channels"`
		} `json:"topics"`
	} `json:"data"`
}

type nodes struct {
	Producers []struct {
		Address string `json:"broadcast_address"`
		Port    int    `json:"http_port"`
	} `json:"producers"`
}

func getHosts(lookupds []string) []string {
	data := make(map[string]struct{})
	for _, host := range lookupds {
		resp, err := http.Get("http://" + host + "/nodes")
		if err != nil {
			continue
		}
		body, _ := ioutil.ReadAll(resp.Body)
		node := &nodes{}
		json.Unmarshal(body, node)
		for _, p := range node.Producers {
			data[fmt.Sprintf("%s:%d", p.Address, p.Port)] = struct{}{}
		}
	}

	hosts := make([]string, 0)
	for v := range data {
		hosts = append(hosts, v)
	}
	return hosts
}
