package eventbus

import "time"

type Config struct {
	URLs               []string
	ClientName         string
	StreamName         string
	Subjects           []string
	Storage            string
	MaxAge             time.Duration
	DuplicateWindow    time.Duration
	ConnectTimeout     time.Duration
	ReconnectWait      time.Duration
	PublishTimeout     time.Duration
	ConsumerAckWait    time.Duration
	ConsumerMaxDeliver int
}

func (c *Config) defaults() {
	if len(c.URLs) == 0 {
		c.URLs = []string{"nats://127.0.0.1:4222"}
	}
	if c.ClientName == "" {
		c.ClientName = "platform-service"
	}
	if c.StreamName == "" {
		c.StreamName = "PLATFORM_EVENTS"
	}
	if len(c.Subjects) == 0 {
		c.Subjects = []string{"platform.>"}
	}
	if c.Storage == "" {
		c.Storage = "file"
	}
	if c.MaxAge <= 0 {
		c.MaxAge = 7 * 24 * time.Hour
	}
	if c.DuplicateWindow <= 0 {
		c.DuplicateWindow = 10 * time.Minute
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 5 * time.Second
	}
	if c.ReconnectWait <= 0 {
		c.ReconnectWait = time.Second
	}
	if c.PublishTimeout <= 0 {
		c.PublishTimeout = 5 * time.Second
	}
	if c.ConsumerAckWait <= 0 {
		c.ConsumerAckWait = 30 * time.Second
	}
	if c.ConsumerMaxDeliver <= 0 {
		c.ConsumerMaxDeliver = 10
	}
}
