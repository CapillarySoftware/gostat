package repo

import (
	"github.com/CapillarySoftware/gostat/stat"
	log "github.com/cihub/seelog"
	"github.com/gocql/gocql"
	"time"
)

type StatRepo struct {
	rawStats <-chan *stat.Stat // Stats to be persisted are read from this channel
	shutdown <-chan bool       // signals a graceful shutdown
}

// NewStatRepo constructs a StatRepo
func NewStatRepo(rawStats <-chan *stat.Stat, shutdown <-chan bool) *StatRepo {
	return &StatRepo{
		rawStats: rawStats,
		shutdown: shutdown,
	}
}

// Run is a goroutine that writes stats from the input channel, placing them into
// the appropriate bucket. Buckets are published on the output channel at the
// specified interval
func (s *StatRepo) Run() {
	done := false

	for !done {
		select {
		case stat := <-s.rawStats:
			log.Debugf("StatRepo got %+v", *stat)
			s.insertRawStat(stat)
		case done = <-s.shutdown:
			log.Debug("StatRepo shutting down ", time.Now())
		case <-time.After(time.Second * 1):
			log.Debug("StatRepo Run() timeout ", time.Now())
		}
	}

	log.Info("StatRepo InsertRawStats() exiting ", time.Now())
}

func (s *StatRepo) insertRawStat(stat *stat.Stat) {
	// connect to the cluster
	cluster := gocql.NewCluster("localhost")
	cluster.Keyspace = "gostat"
	cluster.Consistency = gocql.Quorum
	session, _ := cluster.CreateSession()
	defer session.Close()

	// insert a tweet
	if err := session.Query(`INSERT INTO raw_stats (name, ts, value) VALUES (?, ?, ?)`,
		stat.Name, stat.Timestamp, stat.Value).Exec(); err != nil {
		log.Error(err)
	}
}
