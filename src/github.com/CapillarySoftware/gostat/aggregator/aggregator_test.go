package aggregator

import (
	"github.com/CapillarySoftware/gostat/stat"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Aggregator", func() {

	Describe("Aggregate", func() {
		It("should return all 0 values if a nil slice is received", func() {
			a := Aggregate(nil)
			Expect(a).To(Equal(StatsAggregate{Average: 0, Min: 0, Max: 0, Count: 0}))
		})

		It("should return all 0 values if an empty slice is received", func() {
			a := Aggregate([]stat.Stat{})
			Expect(a).To(Equal(StatsAggregate{Average: 0, Min: 0, Max: 0, Count: 0}))
		})

		It("should return all the same values if the slice contains just one Stat", func() {
			const value = 123.456
			stats := []stat.Stat{{"foo", time.Now().UTC(), value}}

			a := Aggregate(stats)
			Expect(a).To(Equal(StatsAggregate{Average: value, Min: value, Max: value, Count: 1}))
		})

		It("should return the expected values for a collection of more than one Stat", func() {
			stats := []stat.Stat{
				{"foo", time.Now().UTC(), 1},
				{"foo", time.Now().UTC(), 2},
				{"foo", time.Now().UTC(), 3},
				{"foo", time.Now().UTC(), 4},
				{"foo", time.Now().UTC(), 5},
				{"foo", time.Now().UTC(), 6}}

			a := Aggregate(stats)
			Expect(a).To(Equal(StatsAggregate{Average: 3.5, Min: 1, Max: 6, Count: 6}))
		})

		It("should ignore stat names", func() {
			stats := []stat.Stat{
				{"each",   time.Now().UTC(), 6},
				{"stat",   time.Now().UTC(), 5},
				{"name",   time.Now().UTC(), 4},
				{"here",   time.Now().UTC(), 3},
				{"is",     time.Now().UTC(), 2},
				{"unique", time.Now().UTC(), 1}}

			a := Aggregate(stats)
			Expect(a).To(Equal(StatsAggregate{Average: 3.5, Min: 1, Max: 6, Count: 6}))
		})

		It("should correctly handle negative values", func() {
			stats := []stat.Stat{
				{"foo", time.Now().UTC(), -1},
				{"foo", time.Now().UTC(), -2},
				{"foo", time.Now().UTC(), -3},
				{"foo", time.Now().UTC(), -4},
				{"foo", time.Now().UTC(), -5},
				{"foo", time.Now().UTC(), -6}}

			a := Aggregate(stats)
			Expect(a).To(Equal(StatsAggregate{Average: -3.5, Min: -6, Max: -1, Count: 6}))
		})
	})


	Describe("AppendStatsAggregate", func() {	
		It("should return all 0 values if both parameters are zeroed-out StatsAggregates", func() {
			appended := AppendStatsAggregate(StatsAggregate{}, StatsAggregate{})
			Expect(appended).To(Equal(StatsAggregate{Average: 0, Min: 0, Max: 0, Count: 0}))
		})	

		It("should return b if a's count is zero", func() {
			a := StatsAggregate{}
			b := StatsAggregate{Average: 3.5, Min: 1, Max: 6, Count: 6}
			appended := AppendStatsAggregate(a, b)
			Expect(appended).To(Equal(b))
		})

		It("should return a if b's count is zero", func() {
			a := StatsAggregate{Average: 3.5, Min: 1, Max: 6, Count: 6}
			b := StatsAggregate{}
			appended := AppendStatsAggregate(a, b)
			Expect(appended).To(Equal(a))
		})

		It("should compute a correct aggregate when both a and b have non-zero counts", func() {
			a := Aggregate([]stat.Stat{
				{"foo", time.Now().UTC(), 1},
				{"foo", time.Now().UTC(), 2},
				{"foo", time.Now().UTC(), 3},
				{"foo", time.Now().UTC(), 4},
				{"foo", time.Now().UTC(), 5}})
			Expect(a).To(Equal(StatsAggregate{Average: 3, Min: 1, Max: 5, Count: 5}))

			b := Aggregate([]stat.Stat{
				{"foo", time.Now().UTC(), 5},
				{"foo", time.Now().UTC(), 7}})
			Expect(b).To(Equal(StatsAggregate{Average: 6, Min: 5, Max: 7, Count: 2}))

			appended := AppendStatsAggregate(a, b)
			Expect(appended).To(Equal(StatsAggregate{Average: 3.857142857142857, Min: 1, Max: 7, Count: 7}))
		})

		It("should compute a correct aggregate for negative values", func() {
			a := Aggregate([]stat.Stat{
				{"foo", time.Now().UTC(), -1},
				{"foo", time.Now().UTC(), -2},
				{"foo", time.Now().UTC(), -3},
				{"foo", time.Now().UTC(), -4},
				{"foo", time.Now().UTC(), -5}})
			Expect(a).To(Equal(StatsAggregate{Average: -3, Min: -5, Max: -1, Count: 5}))

			b := Aggregate([]stat.Stat{{"foo", time.Now().UTC(), -6}})
			Expect(b).To(Equal(StatsAggregate{Average: -6, Min: -6, Max: -6, Count: 1}))

			appended := AppendStatsAggregate(a, b)
			Expect(appended).To(Equal(StatsAggregate{Average: -3.5, Min: -6, Max: -1, Count: 6}))
		})
	})
})
