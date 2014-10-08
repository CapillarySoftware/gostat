package socketApi

import (
	"encoding/json"
	"github.com/CapillarySoftware/gostat/repo"
	"github.com/CapillarySoftware/gostat/stat"
	log "github.com/cihub/seelog"
	"github.com/googollee/go-socket.io"
	"net/http"
	"time"
)

type rawStat struct {
	// UNIX EPOCH Timestamp specifies the moment in time the statistic is applicable to
	Ts int64 `json:"ts"`

	// Value is the numeric representation of the statistic
	Value float64 `json:"value"`
}

type rawStatsRequest struct {
	Tracker   string `json:"tracker"`
	Name      string `json:"name"`
	StartDate int64  `json:"startDate"`
	EndDate   int64  `json:"endDate"`
}

func SocketApiServer() {
	server, err := socketio.NewServer(nil)
	if err != nil {
		log.Error(err)
	}

	server.On("connection", func(so socketio.Socket) {
		log.Debug("on connection (rawStats)")
		so.On("rawStats", func(msg string) {

			log.Debug("rawStats request: ", msg)
			so.Emit("echo", "reply: "+msg)

			rawStats, _ := runRawLogQuery(msg)

			so.Emit("rawStats", toJson(rawStats))
		})
		so.On("disconnection", func() {
			log.Debug("on disconnect (rawStats)")
		})
	})
	server.On("error", func(so socketio.Socket, err error) {
		log.Error("SocketApiServer: ", err)
	})

	http.Handle("/socket.io/", server)
	http.Handle("/", http.FileServer(http.Dir("./asset")))
	log.Debug("socket.io API serving at localhost:5000...")
	log.Error(http.ListenAndServe(":5000", nil))
}

func runRawLogQuery(req string) (rawStats []stat.Stat, err error) {
	const longForm = "2006-01-02 15:04:05-0700"
	startDate, _ := time.Parse(longForm, "2014-09-30 20:50:18-0600")
	endDate, _ := time.Parse(longForm, "2016-09-30 21:50:15-0600")
	rawStats, _ = repo.GetRawStats("stat8", startDate, endDate)

	return rawStats, nil
	/*
		var request rawStatsRequest
		err = json.Unmarshal([]byte(req), &request)
		if err != nil {
			const longForm = "2006-01-02 15:04:05-0700"
			startDate, _ := time.Parse(longForm, "2014-09-30 20:50:18-0600")
			endDate, _ := time.Parse(longForm, "2014-09-30 21:50:15-0600")
			rawStats, _ = repo.GetRawStats("stat8", startDate, endDate)

			return rawStats, nil
		} else {
			return nil, err
		}
	*/
}

func toJson(stats []stat.Stat) string {
	converted := make([]rawStat, 0)

	for _, stat := range stats {
		c := rawStat{Ts: stat.Timestamp.Unix(), Value: stat.Value}
		converted = append(converted, c)
	}

	convertedJson, _ := json.Marshal(converted)

	return string(convertedJson)
}
