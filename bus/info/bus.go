package info

type Consumer struct {
	Bus      string `json:"bus,omitempty"`
	Topic    string `json:"topic,omitempty"`
	Channel  string `json:"channel,omitempty"`
	Received int    `json:"received"`
}

type Producer struct {
	Bus  string         `json:"bus,omitempty"`
	Sent map[string]int `json:"sent,omitempty"`
}
