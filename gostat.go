package main

import (
	"flag"
	"fmt"
	"github.com/CapillarySoftware/gostat/aggregator"
	"github.com/CapillarySoftware/gostat/bucketer"
	"github.com/CapillarySoftware/gostat/repo"
	"github.com/CapillarySoftware/gostat/socketApi"
	"github.com/CapillarySoftware/gostat/stat"
	log "github.com/cihub/seelog"
	nano "github.com/op/go-nanomsg"
	"math/rand"
	"os"
	"os/signal"
	"time"
)

func main() {
	simulateData := flag.Bool("sim", false, "randomly generate and insert test data")
	flag.Parse()

	go socketApi.SocketApiServer()

	stats := make(chan *stat.Stat)           // stats received from producers
	rawStats := make(chan *stat.Stat)        // raw stats to be archived
	bucketedStats := make(chan []*stat.Stat) // raw bucketed (non-aggregated) stats are output here
	shutdownBucketer := make(chan bool)      // used to signal the bucketer we are done
	shutdownListener := make(chan bool)      // used to signal the socket listener we are done
	shutdownAggregator := make(chan bool)    // used to signal the aggregator we are done
	shutdownStatRepo := make(chan bool)      // used to signal the stat repo we are done

	installCtrlCHandler(shutdownBucketer, shutdownListener, shutdownAggregator, shutdownStatRepo)

	// create and start a Bucketer
	b := bucketer.NewBucketer(stats, bucketedStats, shutdownBucketer)
	go b.Run(time.Second * 5)

	// create and start a stat repo
	r := repo.NewStatRepo(rawStats, shutdownStatRepo)
	go r.Run()

	// TODO: replace this simulation of an Aggregator with a real one
	go func(bucketed <-chan []*stat.Stat, shutdown <-chan bool) {
		for {
			select {
			case bucket := <-bucketed:
				log.Debugf("aggregator simulation got stats bucket with length %d", len(bucket))
				for _, stat := range bucket {
					log.Debugf("aggregator sim: %#v", stat)
				}
				a := aggregator.Aggregate(bucket)
				log.Debugf("aggregator sim aggregate: %#v", a)
			case <-shutdown:
				log.Info("aggregator simulation shutting down")
				break
			}
		}
	}(bucketedStats, shutdownAggregator)

	// start a socket listener
	go bindSocketListener(stats, shutdownListener)

	// TODO: remove this simulation of stats when the socket listener is really processing data
	for true {
		<-time.After(time.Second * time.Duration(rand.Intn(3))) // sleep 0-3 seconds

		if *simulateData {
			// create a stat randomly named "stat1 ... stat10" with a random value between 1-100
			stat := stat.Stat{Name: fmt.Sprintf("stat%v", (rand.Intn(9) + 1)), Timestamp: time.Now().UTC(), Value: float64(rand.Intn(99) + 1)}
			log.Debug("Generated simulated stat: ", stat)
			stats <- &stat    // send it to the Bucketer
			rawStats <- &stat // for archiving
		}
	}
}

// installCtrlCHandler starts a goroutine that will signal the workers when it's time
// to shut down
func installCtrlCHandler(shutdown ...chan<- bool) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		for sig := range c {
			log.Infof(" captured %v, stopping stats collection and exiting...\n", sig)
			for _, s := range shutdown {
				s <- true
			}
			<-time.After(time.Second * 5) // wait for a clean shutdown, TODO: wait on signal from all routines
			log.Info("Done")
			os.Exit(1)
		}
	}()
}

func bindSocketListener(stats chan<- *stat.Stat, shutdown <-chan bool) {
	var (
		msg  []byte
		err  error
		done = false
	)
	socket, err := nano.NewPullSocket()

	if nil != err {
		log.Error(err)
	}

	socket.SetRecvTimeout(time.Second)

	defer socket.Close()
	_, err = socket.Bind("tcp://*:2025")
	if nil != err {
		log.Error(err)
	}
	log.Info("Ready to receive data")

	for !done {
		msg, err = socket.Recv(0) //blocking
		if nil != err && err.Error() != "resource temporarily unavailable" {
			log.Error(err)
		}

		if nil != msg {
			// TODO: new up a Stat and send it on the stats channel
			log.Debug("Received message: ", msg)
		}

		select {
		case done = <-shutdown:
		default:
			break
		}
	}

	log.Info("Exiting socket listener")
}
