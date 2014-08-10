// Package stat represents containers for statistics
package stat

import "time"

type Stat struct {
	// Name uniquely identifies the statistic (e.g. localhost-CPU-utilization)
	Name string

	// Timestamp specifies the moment in time the statistic is applicable to
	Timestamp time.Time

	// Value is the numeric representation of the statistic
	Value float64
}
