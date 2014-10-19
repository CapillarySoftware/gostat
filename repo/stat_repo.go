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

func createSession() (session *gocql.Session, err error) {
	cluster := gocql.NewCluster("localhost")
	cluster.Keyspace = "gostat"
	cluster.Consistency = gocql.Quorum

	return cluster.CreateSession()
}

func (s *StatRepo) insertRawStat(stat *stat.Stat) {
	var session *gocql.Session
	var err error

	if session, err = createSession(); err != nil {
		log.Error("error connecting to Cassandra to insert raw stat: ", err)
		return
	}
	defer closeSession(session)

	if err := session.Query(`INSERT INTO raw_stats (name, ts, value) VALUES (?, ?, ?)`,
		stat.Name, stat.Timestamp, stat.Value).Exec(); err != nil {
		log.Error("error inserting raw stat: ", err)
	}
}

func closeSession(session *gocql.Session) {
	if session != nil {
		session.Close()
	}
}

func GetRawStats(name string, start, end time.Time) ([]stat.Stat, error) {
	var session *gocql.Session
	var err error
	rawStats := make([]stat.Stat, 0)

	if session, err = createSession(); err != nil {
		log.Error("failed to connect to Cassandra to query raw stats: ", err)
		return make([]stat.Stat, 0), err
	}
	defer closeSession(session)

	iter := session.Query(`SELECT ts, value FROM raw_stats WHERE name = ? AND ts >= ? AND ts <= ?`, name, start, end).Iter()
	var ts time.Time
	var value float64
	for iter.Scan(&ts, &value) {
		stat := stat.Stat{Name: name, Timestamp: ts, Value: value}
		rawStats = append(rawStats, stat)
	}

	if err := iter.Close(); err != nil {
		log.Error("error transforming raw stats query results: ", err)
		return make([]stat.Stat, 0), err
	}

	return rawStats, nil
}

func GetLastNRawStats(name string, last int) ([]stat.Stat, error) {
	var session *gocql.Session
	var err error
	rawStats := make([]stat.Stat, 0)
	tmp := make([]stat.Stat, 1)

	if session, err = createSession(); err != nil {
		log.Error("failed to connect to Cassandra to query last n raw stats: ", err)
		return make([]stat.Stat, 0), err
	}
	defer session.Close()

	iter := session.Query(`SELECT ts, value FROM raw_stats WHERE name = ? ORDER BY ts DESC LIMIT ?`, name, last).Iter()
	var ts time.Time
	var value float64
	for iter.Scan(&ts, &value) {
		stat := stat.Stat{Name: name, Timestamp: ts, Value: value}
		tmp[0] = stat
		rawStats = append(tmp, rawStats...)
	}

	if err := iter.Close(); err != nil {
		log.Error("error transforming last n raw stats query results: ", err)
		return make([]stat.Stat, 0), err
	}

	return rawStats, nil
}
