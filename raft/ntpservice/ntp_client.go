package ntpservice

import (
	"github.com/beevik/ntp"
	"time"
)

type NtpClient struct {
	server string
	offset time.Duration
}

func NewNtpClient(server string) *NtpClient {
	return &NtpClient{
		server: server,
	}
}

func (c *NtpClient) Now() (time.Time, error) {
	t, err := ntp.Time(c.server)
	if err != nil {
		return time.Now(), err
	}
	c.offset = t.Sub(time.Now())
	return t, nil
}

func (c *NtpClient) Sync() (time.Duration, error) {
	t, err := ntp.Time(c.server)
	if err != nil {
		return 0, err
	}
	newOffset := t.Sub(time.Now())
	oldOffset := c.offset
	c.offset = newOffset
	return newOffset - oldOffset, nil
}

func (c *NtpClient) Offset() time.Duration {
	return c.offset
}
