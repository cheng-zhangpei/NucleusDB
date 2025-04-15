package ntp

import (
	"github.com/beevik/ntp"
	"time"
)

type ntpClient struct {
	server string
	offset time.Duration
}

func NewNTPClient(server string) *ntpClient {
	return &ntpClient{
		server: server,
	}
}

func (c *ntpClient) Now() (time.Time, error) {
	t, err := ntp.Time(c.server)
	if err != nil {
		return time.Now(), err
	}
	c.offset = t.Sub(time.Now())
	return t, nil
}

func (c *ntpClient) Sync() (time.Duration, error) {
	t, err := ntp.Time(c.server)
	if err != nil {
		return 0, err
	}
	newOffset := t.Sub(time.Now())
	oldOffset := c.offset
	c.offset = newOffset
	return newOffset - oldOffset, nil
}

func (c *ntpClient) Offset() time.Duration {
	return c.offset
}
