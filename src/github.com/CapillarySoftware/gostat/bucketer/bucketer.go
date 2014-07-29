package bucketer

import "github.com/CapillarySoftware/gostat/stat"

type Bucketer interface {
	Add(stat *stat.Stat) (err error)
}