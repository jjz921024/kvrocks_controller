package kvrocks

import (
	"context"
	"crypto/tls"
	"errors"
	"time"

	"github.com/RocksLabs/kvrocks_controller/metadata"
	"github.com/RocksLabs/kvrocks_controller/storage/persistence"
	"github.com/go-redis/redis/v8"
	"go.uber.org/atomic"
)

const (
	defaultDailTimeout   = 5 * time.Second
	defaultSocketTimeout = 2 * time.Second
)

type Config struct {
	Addrs    []string `yaml:"addrs"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
	TLS      struct {
		Enable        bool   `yaml:"enable"`
		CertFile      string `yaml:"cert_file"`
		KeyFile       string `yaml:"key_file"`
		TrustedCAFile string `yaml:"ca_file"`
	} `yaml:"tls"`
}

type KvRocks struct {
	client *redis.ClusterClient

	myID    string
	isReady atomic.Bool

	quitCh chan struct{}
}

func New(id string, cfg *Config) (*KvRocks, error) {
	if len(id) == 0 {
		return nil, errors.New("id must NOT be a empty string")
	}

	opt := &redis.ClusterOptions{
		Addrs:        cfg.Addrs,
		Username:     cfg.Username,
		Password:     cfg.Password,
		DialTimeout:  defaultDailTimeout,
		ReadTimeout:  defaultSocketTimeout,
		WriteTimeout: defaultSocketTimeout,
	}

	if cfg.TLS.Enable {
		opt.TLSConfig = &tls.Config{
			//Certificates: cfg.TLS.CertFile,
			/* CertFile:      cfg.TLS.CertFile,
			KeyFile:       cfg.TLS.KeyFile,
			TrustedCAFile: cfg.TLS.TrustedCAFile, */
		}
	}

	client := redis.NewClusterClient(opt)

	kvr := &KvRocks{
		myID:   id,
		client: client,
		quitCh: make(chan struct{}),
	}

	kvr.isReady.Store(true)
	return kvr, nil
}

func (kvr *KvRocks) ID() string {
	return kvr.myID
}

func (kvr *KvRocks) Leader() string {
	// mock this method
	return kvr.myID
}

func (kvr *KvRocks) LeaderChange() <-chan bool {
	// mock this method
	return make(<-chan bool)
}

func (kvr *KvRocks) IsReady(ctx context.Context) bool {
	for {
		select {
		case <-kvr.quitCh:
			return false
		case <-ctx.Done():
			return kvr.isReady.Load()
		default:
			return kvr.isReady.Load()
		}
	}
}

func (kvr *KvRocks) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := kvr.client.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if len(resp) == 0 {
		return nil, metadata.ErrEntryNoExists
	}
	return []byte(resp), nil
}

func (kvr *KvRocks) Exists(ctx context.Context, key string) (bool, error) {
	resp, err := kvr.client.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return resp == 0, nil
}

func (kvr *KvRocks) Set(ctx context.Context, key string, value []byte) error {
	_, err := kvr.client.Set(ctx, key, value, 0).Result()
	return err
}

func (kvr *KvRocks) Delete(ctx context.Context, key string) error {
	_, err := kvr.client.Del(ctx, key).Result()
	return err
}

func (kvr *KvRocks) List(ctx context.Context, prefix string) ([]persistence.Entry, error) {
	return nil, nil
}

func (kvr *KvRocks) Close() error {
	close(kvr.quitCh)
	return kvr.client.Close()
}
