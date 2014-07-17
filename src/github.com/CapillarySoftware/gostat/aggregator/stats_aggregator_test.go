package aggregator

import (
	"github.com/CapillarySoftware/gostat/stat"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("StatsAggregator", func() {

	var sa StatsAggregator

	JustBeforeEach(func() {
		sa = StatsAggregator{}
	})

	Describe("Aggregate", func() {
		It("Should return all 0 values if a nil slice is received", func() {
			average, min, max := sa.Aggregate(nil)
			Expect(average).To(Equal(0.0))
			Expect(min).To(Equal(0.0))
			Expect(max).To(Equal(0.0))
		})

		It("Should return all 0 values if an empty slice is received", func() {
			average, min, max := sa.Aggregate([]stat.Stat{})
			Expect(average).To(Equal(0.0))
			Expect(min).To(Equal(0.0))
			Expect(max).To(Equal(0.0))
		})

		It("Should return all the same values if the slice contains just one Stat", func() {
			const value = 123.456
			stats := []stat.Stat{{"foo", time.Now().UTC(), value}}

			average, min, max := sa.Aggregate(stats)
			Expect(average).To(Equal(value))
			Expect(min).To(Equal(value))
			Expect(max).To(Equal(value))
		})

		It("Should return the expected values for a collection of more than one Stat", func() {

			stats := []stat.Stat{{"foo", time.Now().UTC(), 1},
				{"foo", time.Now().UTC(), 2},
				{"foo", time.Now().UTC(), 3},
				{"foo", time.Now().UTC(), 4},
				{"foo", time.Now().UTC(), 5},
				{"foo", time.Now().UTC(), 6}}

			average, min, max := sa.Aggregate(stats)
			Expect(average).To(Equal(3.5))
			Expect(min).To(Equal(1.0))
			Expect(max).To(Equal(6.0))
		})
	})

})
