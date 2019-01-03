package natsstreaming

import (
	"time"

	redigo "github.com/gomodule/redigo/redis"
	"github.com/jpillora/backoff"
	log "github.com/sirupsen/logrus"

	"github.com/kumparan/go-lib/utils"
	"github.com/nats-io/go-nats-streaming"
)

type (
	// EventType :nodoc:
	EventType string

	// NatsCallback :nodoc:
	NatsCallback func(conn stan.Conn)

	// NATS :nodoc:
	NATS struct {
		conn      stan.Conn
		redisConn *redigo.Pool
		testing   bool

		publishRetryAttempts int
		publishRetryInterval time.Duration
	}

	// NatsMessage :nodoc:
	NatsMessage struct {
		ID     int64     `json:"id"`
		UserID int64     `json:"user_id"`
		Type   EventType `json:"type"`
		Body   string    `json:"body,omitempty"`
		Time   string    `json:"time"`
	}

	NatsMessageWithSubject struct {
		Subject string      `json:"subject"`
		Message interface{} `json:"message"`
	}
)

const (
	failedMessagesRedisKey      = "nats:failed-messages"
	defaultPublishRetryAttempts = 5
	defaultPublishRetryInterval = 100 * time.Millisecond
	defaultConnectRetryAttempts = 50
)

// NewNATS :nodoc:
func NewNATS(clusterID, clientID, url string, options ...stan.Option) (*NATS, error) {
	options = append(options, stan.NatsURL(url))
	nc, err := stan.Connect(clusterID, clientID, options...)
	if err != nil {
		return nil, err
	}

	return &NATS{conn: nc, publishRetryAttempts: defaultPublishRetryAttempts, publishRetryInterval: defaultPublishRetryInterval}, nil
}

// NewNATSWithCallback IMPORTANT! Not to send any stan.NatsURL or stan.SetConnectionLostHandler as options
func NewNATSWithCallback(clusterID, clientID, url string, fn NatsCallback, options ...stan.Option) {
	c := make(chan int)
	options = append(options, stan.NatsURL(url))
	options = append(options, stan.SetConnectionLostHandler(func(conn stan.Conn, reason error) {
		log.Errorf("Nats connection lost! Reason: %v", reason)

		conn.Close()
		c <- 1
	}))
	var nc stan.Conn
	var err error

	retryAttempts := 1
	backoffer := &backoff.Backoff{
		Min:    200 * time.Millisecond,
		Max:    1 * time.Second,
		Jitter: true,
	}

	for {
		if retryAttempts >= defaultConnectRetryAttempts {
			log.WithFields(log.Fields{
				"clusterID": clusterID,
				"clientID":  clientID,
			}).Fatalf("Connect failed count reaches limit of %d. Shutting down the server...", defaultConnectRetryAttempts)
		}

		nc, err = stan.Connect(clusterID, clientID, options...)
		if err != nil {
			log.WithFields(log.Fields{
				"clusterID":     clusterID,
				"clientID":      clientID,
				"retryAttempts": retryAttempts,
			}).Error(err)
			retryAttempts++

			time.Sleep(backoffer.Duration())
			continue
		}

		log.WithFields(log.Fields{
			"clusterID": clusterID,
			"clientID":  clientID,
		}).Info("Nats connection made...")
		retryAttempts = 1

		// Run callback function
		fn(nc)

		<-c
	}
}

// NewTestNATS :nodoc:
func NewTestNATS() *NATS {
	return &NATS{testing: true}
}

// SetConn :nodoc:
func (n *NATS) SetConn(conn stan.Conn) {
	n.conn = conn
}

// SetRedisConn :nodoc:
func (n *NATS) SetRedisConn(conn *redigo.Pool) {
	n.redisConn = conn
}

// SetPublishRetryAttempts :nodoc:
func (n *NATS) SetPublishRetryAttempts(i int) {
	n.publishRetryAttempts = i
}

// SetPublishRetryInterval :nodoc:
func (n *NATS) SetPublishRetryInterval(d time.Duration) {
	n.publishRetryInterval = d
}

// GetPublishRetryAttempts :nodoc:
func (n *NATS) GetPublishRetryAttempts() int {
	if n.publishRetryAttempts <= 0 {
		return defaultPublishRetryAttempts
	}
	return n.publishRetryAttempts
}

// GetPublishRetryInterval :nodoc:
func (n *NATS) GetPublishRetryInterval() time.Duration {
	if n.publishRetryInterval <= 1*time.Millisecond {
		return defaultPublishRetryInterval
	}
	return n.publishRetryInterval
}

// Close NatsConnection :nodoc:
func (n *NATS) Close() {
	n.conn.Close()
}

// Publish :nodoc:
func (n *NATS) Publish(subject string, v interface{}) error {
	if n.testing {
		return nil
	}

	giveUpErr := utils.Retry(n.GetPublishRetryAttempts(), n.GetPublishRetryInterval(), func() error {
		err := n.conn.Publish(subject, utils.ToByte(v))
		if err != nil {
			client := n.redisConn.Get()
			defer client.Close()

			client.Do("RPUSH", failedMessagesRedisKey, utils.ToByte(NatsMessageWithSubject{
				Subject: subject,
				Message: v,
			}))
		}

		return err
	})
	if giveUpErr != nil {
		log.WithField("type", "give up").Error(giveUpErr)
	}

	return nil
}

// QueueSubscribe :nodoc:
func (n *NATS) QueueSubscribe(subject, qgroup string, cb stan.MsgHandler, opts ...stan.SubscriptionOption) (stan.Subscription, error) {
	if n.testing {
		return nil, nil
	}

	return n.conn.QueueSubscribe(subject, qgroup, cb, opts...)
}

// Subscribe :nodoc:
func (n *NATS) Subscribe(subject string, cb stan.MsgHandler, opts ...stan.SubscriptionOption) (stan.Subscription, error) {
	if n.testing {
		return nil, nil
	}
	return n.conn.Subscribe(subject, cb, opts...)
}
