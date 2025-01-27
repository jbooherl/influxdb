package udp

import (
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultBindAddress is the default binding interface if none is specified.
	DefaultBindAddress = ":8089"

	// DefaultDatabase is the default database for UDP traffic.
	DefaultDatabase = "udp"

	// DefaultRetentionPolicy is the default retention policy used for writes.
	DefaultRetentionPolicy = ""

	// DefaultBatchSize is the default UDP batch size.
	DefaultBatchSize = 5000

	// DefaultBatchPending is the default number of pending UDP batches.
	DefaultBatchPending = 10

	// DefaultBatchTimeout is the default UDP batch timeout.
	DefaultBatchTimeout = time.Second

	// DefaultPrecision is the default time precision used for UDP services.
	DefaultPrecision = "n"

	// DefaultReadBuffer is the default buffer size for the UDP listener.
	// Sets the size of the operating system's receive buffer associated with
	// the UDP traffic. Keep in mind that the OS must be able
	// to handle the number set here or the UDP listener will error and exit.
	//
	// DefaultReadBuffer = 0 means to use the OS default, which is usually too
	// small for high UDP performance.
	//
	// Increasing OS buffer limits:
	//     Linux:      sudo sysctl -w net.core.rmem_max=<read-buffer>
	//     BSD/Darwin: sudo sysctl -w kern.ipc.maxsockbuf=<read-buffer>
	DefaultReadBuffer = 0

	// DefaultWriters is the default number of writers.
	DefaultWriters = 1
)

// Config holds various configuration settings for the UDP listener.
type Config struct {
	Enabled     bool   `toml:"enabled"`
	BindAddress string `toml:"bind-address"`

	Database        string        `toml:"database"`
	RetentionPolicy string        `toml:"retention-policy"`
	BatchSize       int           `toml:"batch-size"`
	BatchPending    int           `toml:"batch-pending"`
	ReadBuffer      int           `toml:"read-buffer"`
	BatchTimeout    toml.Duration `toml:"batch-timeout"`
	Precision       string        `toml:"precision"`
	Writers         int           `toml:"writers"`
}

// NewConfig returns a new instance of Config with defaults.
func NewConfig() Config {
	return Config{
		BindAddress:     DefaultBindAddress,
		Database:        DefaultDatabase,
		RetentionPolicy: DefaultRetentionPolicy,
		BatchSize:       DefaultBatchSize,
		BatchPending:    DefaultBatchPending,
		BatchTimeout:    toml.Duration(DefaultBatchTimeout),
		Writers:         DefaultWriters,
	}
}

// WithDefaults takes the given config and returns a new config with any required
// default values set.
func (c *Config) WithDefaults() *Config {
	d := *c
	if d.Database == "" {
		d.Database = DefaultDatabase
	}
	if d.BatchSize == 0 {
		d.BatchSize = DefaultBatchSize
	}
	if d.BatchPending == 0 {
		d.BatchPending = DefaultBatchPending
	}
	if d.BatchTimeout == 0 {
		d.BatchTimeout = toml.Duration(DefaultBatchTimeout)
	}
	if d.Precision == "" {
		d.Precision = DefaultPrecision
	}
	if d.ReadBuffer == 0 {
		d.ReadBuffer = DefaultReadBuffer
	}
	if d.Writers == 0 {
		d.Writers = DefaultWriters
	}
	return &d
}

// Configs wraps a slice of Config to aggregate diagnostics.
type Configs []Config

// Diagnostics returns one set of diagnostics for all of the Configs.
func (c Configs) Diagnostics() (*diagnostics.Diagnostics, error) {
	d := &diagnostics.Diagnostics{
		Columns: []string{"enabled", "bind-address", "database", "retention-policy", "batch-size", "batch-pending", "batch-timeout", "precision", "writers"},
	}

	for _, cc := range c {
		if !cc.Enabled {
			d.AddRow([]interface{}{false})
			continue
		}

		r := []interface{}{true, cc.BindAddress, cc.Database, cc.RetentionPolicy, cc.BatchSize, cc.BatchPending, cc.BatchTimeout, cc.Precision, cc.Writers}
		d.AddRow(r)
	}

	return d, nil
}

// Enabled returns true if any underlying Config is Enabled.
func (c Configs) Enabled() bool {
	for _, cc := range c {
		if cc.Enabled {
			return true
		}
	}
	return false
}
