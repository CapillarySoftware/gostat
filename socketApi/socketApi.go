package socketApi

import (
	"encoding/json"
	"github.com/CapillarySoftware/gostat/repo"
	"github.com/CapillarySoftware/gostat/stat"
	log "github.com/cihub/seelog"
	"github.com/googollee/go-socket.io"
	"net/http"
	"strings"
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

type lastNRawStatsRequest struct {
	Tracker string `json:"tracker"`
	Name    string `json:"name"`
	Last    int    `json:"last"`
}

func handleRawStatsReq(reqType, msg string, so socketio.Socket) {
	log.Debug(reqType, ": ", msg)
	so.Emit("echo", msg)

	rawStats, err := runRawLogQuery(reqType, msg)
	if err != nil {
		log.Error("error running ", reqType, " query: ", err)
	}

	log.Debug(rawStats)
	resType := strings.TrimSuffix(reqType, "Req") + "Res"
	if so != nil {
		so.Emit(resType, toJson(rawStats))
	}
}

func SocketApiServer() {
	server, err := socketio.NewServer(nil)
	if err != nil {
		log.Error(err)
	}

	server.On("connection", func(so socketio.Socket) {
		log.Debug("on connection (socketApi)")
		so.On("rawStatsReq", func(msg string) {
			handleRawStatsReq("rawStatsReq", msg, so)
		})
		so.On("lastNRawStatsReq", func(msg string) {
			handleRawStatsReq("lastNRawStatsReq", msg, so)
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

func runRawLogQuery(reqType, req string) (rawStats []stat.Stat, err error) {
	switch reqType {
	case "rawStatsReq":
		request, err := unmarshalRawStatsReq(req)
		if err != nil {
			return nil, err
		}
		log.Debugf("parsed rawStatsReq request: %#v (start date: %s, end date: %s)", request, time.Unix(request.StartDate, 0), time.Unix(request.EndDate, 0))
		if rawStats, err = repo.GetRawStats(request.Name, time.Unix(request.StartDate, 0), time.Unix(request.EndDate, 0)); err != nil {
			log.Error("repo error retrieving raw stats for rawStatsReq request (", req, "): ", err)
			return nil, err
		}
	case "lastNRawStatsReq":
		request, err := unmarshalLastNRawStatsReq(req)
		if err != nil {
			return nil, err
		}

		log.Debugf("parsed lastNRawStatsReq request: %#v", request)
		if rawStats, err = repo.GetLastNRawStats(request.Name, request.Last); err != nil {
			log.Error("repo error retrieving last n raw stats for lastNRawStatsReq request (", req, "): ", err)
			return nil, err
		}
	}

	return rawStats, nil
}

func unmarshalRawStatsReq(req string) (request *rawStatsRequest, err error) {
	if err = json.Unmarshal([]byte(req), &request); err != nil {
		log.Error("error parsing raw stats request (", req, "): ", err)
		return nil, err
	}
	return request, nil
}

func unmarshalLastNRawStatsReq(req string) (request *lastNRawStatsRequest, err error) {
	if err = json.Unmarshal([]byte(req), &request); err != nil {
		log.Error("error parsing last n raw stats request (", req, "): ", err)
		return nil, err
	}
	return request, nil
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
