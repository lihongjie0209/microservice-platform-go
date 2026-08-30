package eventbus

import (
	"errors"
	"time"
)

type Config struct {
	URLs                   []string
	ClientName             string
	StreamName             string
	Subjects               []string
	Storage                string
	MaxAge                 time.Duration
	DuplicateWindow        time.Duration
	ConnectTimeout         time.Duration
	ReconnectWait          time.Duration
	PublishTimeout         time.Duration
	ConsumerAckWait        time.Duration
	ConsumerAckTimeout     time.Duration
	ConsumerHandlerTimeout time.Duration
	ConsumerRetryDelay     time.Duration
	ConsumerMaxRetryDelay  time.Duration
	ConsumerMaxDeliver     int
	ConsumerMaxAckPending  int
	DeadLetterSubject      string
	DeadLetterMaxDataBytes int
}

func (c Config) validate() error {
	if c.ConsumerHandlerTimeout >= c.ConsumerAckWait {
		return errors.New("consumer handler timeout must be shorter than ack wait")
	}
	if c.ConsumerRetryDelay > c.ConsumerMaxRetryDelay {
		return errors.New("consumer retry delay must not exceed maximum retry delay")
	}
	return nil
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
	if c.ConsumerAckTimeout <= 0 {
		c.ConsumerAckTimeout = 5 * time.Second
	}
	if c.ConsumerHandlerTimeout <= 0 {
		c.ConsumerHandlerTimeout = 25 * time.Second
	}
	if c.ConsumerRetryDelay <= 0 {
		c.ConsumerRetryDelay = time.Second
	}
	if c.ConsumerMaxRetryDelay <= 0 {
		c.ConsumerMaxRetryDelay = time.Minute
	}
	if c.ConsumerMaxDeliver <= 0 {
		c.ConsumerMaxDeliver = 10
	}
	if c.ConsumerMaxAckPending <= 0 {
		c.ConsumerMaxAckPending = 64
	}
	if c.DeadLetterSubject == "" {
		c.DeadLetterSubject = DeadLetterSubject
	}
	if c.DeadLetterMaxDataBytes <= 0 {
		c.DeadLetterMaxDataBytes = 1 << 20
	}
}
