package main

import (
	"github.com/CapillarySoftware/gostat/stat"
	"github.com/CapillarySoftware/gostat/bucketer"
	"fmt"
	"time"
	"os"
	"os/signal"
	"math/rand"
)

func main () {
	stats            := make(chan *stat.Stat)   // stats received from producers
	bucketedStats    := make(chan []*stat.Stat) // raw bucketed (non-aggregated) stats are output here
	shutdownBucketer := make(chan bool)         // used to signal to the bucketer we are done

  installCtrlCHandler(&shutdownBucketer)

  // create and start a Bucketer
	b := bucketer.NewBucketer(stats, bucketedStats, shutdownBucketer)
	go b.Run()

	// TODO: replace this simulation of stats with (something like) a 0mq receiver
	for true {
		<-time.After(time.Second * time.Duration(rand.Intn(3))) // sleep 0-3 seconds

    // create a state named "stat[1-10]" with a random value between 1-100
		stat := stat.Stat{Name : fmt.Sprintf("stat%v", (rand.Intn(9)+1)), Timestamp : time.Now().UTC(), Value : float64(rand.Intn(99)+1)}
		stats <- &stat // send it to the Bucketer
	}
}

// installCtrlCHandler starts a goroutine that will signal the workers when it's time
// to shut down
func installCtrlCHandler(shutdownBucketer *chan bool) {
	c := make(chan os.Signal, 1)                                       
	signal.Notify(c, os.Interrupt)                                     
	
	go func() {                                                        
	  for sig := range c {                                             
	    fmt.Printf(" captured %v, stopping stats collection and exiting...\n", sig)
	    *shutdownBucketer <-true  // stop the bucketer
	    <-time.After(time.Second * 5) // wait for a clean shutdown, TODO: wait on signal from all routines
	    fmt.Println("Done")                                      
	    os.Exit(1)                                                     
	  }                                                                
	}()
}