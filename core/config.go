package core

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
)

type Config struct {
	Servers   []string
	Auth      *Auth
	TLSConfig *tls.Config

	SlientLog bool
}

func NewConfig(Servers []string, slientLog bool) *Config {
	return &Config{
		Servers:   Servers,
		SlientLog: slientLog,
	}
}

type Auth struct {
	Scheme  string
	Payload []byte
}

func NewAuth(scheme, auth string) *Auth {
	return &Auth{
		Scheme:  scheme,
		Payload: []byte(auth),
	}
}

type emptyLogger struct{}

func (emptyLogger) Printf(format string, a ...interface{}) {
	// do nothing
}

func (c *Config) Connect() (*zk.Conn, error) {
	logger := zk.WithLogger(zk.DefaultLogger)
	if c.SlientLog {
		logger = zk.WithLogger(emptyLogger{})
	}
	var conn *zk.Conn
	var e <-chan zk.Event
	var err error

	if c.TLSConfig == nil {
		conn, e, err = zk.Connect(c.Servers, time.Second, logger)
	} else {
		dialer := func(network, address string, timeout time.Duration) (net.Conn, error) {
			return tls.DialWithDialer(&net.Dialer{Timeout: timeout}, network, address, c.TLSConfig)
		}
		conn, e, err = zk.Connect(c.Servers, time.Second, logger, zk.WithDialer(dialer))
	}
	if err != nil {
		return nil, err
	}
	if c.Auth != nil {
		auth := c.Auth
		err = conn.AddAuth(auth.Scheme, auth.Payload)
		if err != nil {
			return nil, err
		}
	}
	n := 0
	failed := false
loop:
	for {
		select {
		case event, ok := <-e:
			n += 1
			if ok && event.State == zk.StateConnected {
				break loop
			} else if n > 3 {
				failed = true
				break loop
			}
		}
	}
	if failed {
		err = errors.New(
			fmt.Sprintf("Failed to connect to %s!", strings.Join(c.Servers, ",")),
		)
	}
	return conn, err
}
