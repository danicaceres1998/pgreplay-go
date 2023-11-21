package aws

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("File Helper functions", func() {
	var (
		filename = "rds_log_391c219c198f5a5ccc5f37a1_1698228000"
		start    = time.Date(2023, time.October, 25, 10, 0, 0, 0, time.UTC)
		finish   = time.Date(2023, time.October, 25, 13, 0, 0, 0, time.UTC)
	)

	Context("#extractDate", func() {
		It("should return the date from the string", func() {
			result, err := extractDate(filename)
			Expect(err).To(BeNil())
			Expect(result).To(Equal(
				time.Date(2023, time.October, 25, 10, 0, 0, 0, time.UTC),
			))
		})

		It("should return an error if the file is not in the correct format", func() {
			result, err := extractDate("random_string")
			Expect(err).NotTo(BeNil())
			Expect(result.IsZero()).To(Equal(true))
		})
	})

	Context("#timeBetween", func() {
		It("should return true if the date is in the interval", func() {
			current := time.Now()
			result := timeBetween(time.Now(), current.Add(-2*time.Hour), current.Add(2*time.Hour))
			Expect(result).To(Equal(true))
		})

		It("should return false if the date is not in the interval", func() {
			current := time.Now()
			result := timeBetween(time.Now().Add(5*time.Hour), current.Add(-2*time.Hour), current.Add(2*time.Hour))
			Expect(result).To(Equal(false))
		})
	})

	Context("#objectBetweenDates", func() {
		It("should send a true if the file date is in the provided interval", func() {
			Expect(objectBetweenDates(filename, start, finish)).To(Equal(true))
		})

		It("should send a false if the file date is not in the provided interval", func() {
			Expect(
				objectBetweenDates(
					fmt.Sprintf("rds_log_391c219c198f5a5ccc5f37a1_%d", start.Add(-3*time.Minute).Unix()),
					start, finish,
				),
			).To(Equal(false))
		})
	})

	Context("#fetchPercentage", func() {
		It("should send the default value if the env var is not present", func() {
			Expect(fetchPercentage()).To(Equal(downloadPercentage))
		})

		It("should send the variable value if the env var is present", func() {
			newValue := 0.25
			err := os.Setenv(percentageEnvVar, fmt.Sprintf("%.2f", newValue))

			Expect(err).To(BeNil())
			Expect(fetchPercentage()).To(Equal(newValue))
			err = os.Unsetenv(percentageEnvVar)

			Expect(err).To(BeNil())
		})

		It("should be less than 50 percent", func() {
			newValue := 0.50
			err := os.Setenv(percentageEnvVar, fmt.Sprintf("%.2f", newValue))

			Expect(err).To(BeNil())
			Expect(fetchPercentage()).To(Equal(downloadPercentage))
			err = os.Unsetenv(percentageEnvVar)

			Expect(err).To(BeNil())
		})
	})

	Context("#popFirstElement", func() {
		It("should return the first element and move all remain elements", func() {
			total := 10
			slice := make([]channelPayload, total)
			for i := range slice {
				slice[i] = channelPayload{index: i}
			}

			var (
				amount int = 0
				cp     channelPayload
			)
			for i := range slice {
				slice, cp = popFirstElement(slice)
				amount++
				Expect(cp.index).To(Equal(i))
				Expect(len(slice)).To(Equal(total - amount))
				Expect(cap(slice)).To(Equal(total))
			}
			Expect(amount).To(Equal(total))
		})

		It("should return an empty value and the same slice if it is empty", func() {
			slice := make([]channelPayload, 0)
			newSlice, element := popFirstElement(slice)

			Expect(len(newSlice)).To(Equal(len(slice)))
			Expect(cap(newSlice)).To(Equal(cap(slice)))
			Expect(element).To(Equal(channelPayload{}))
		})
	})

	Context("#enabledToProcess", func() {
		var (
			// downloadPercentage = 15%
			lessAmount    = 10
			greaterAmount = 30
			totalAmount   = 100
		)
		Context("percentage downloaded", func() {
			It("should return false if the percentage is not ready", func() {
				amount, ok := enabledToProcess([]channelPayload{}, lessAmount, totalAmount, 0)
				Expect(amount).To(Equal(0))
				Expect(ok).To(Equal(false))
			})
		})

		Context("the first matching", func() {
			It("should return false if the first element does not match with the lastProcessed", func() {
				slice := []channelPayload{
					{index: 5},
				}
				amount, ok := enabledToProcess(slice, greaterAmount, totalAmount, 0)
				Expect(amount).To(Equal(0))
				Expect(ok).To(Equal(false))
			})
		})

		Context("should return the amount to pop", func() {
			It("should return true and the amount to pop (always in couples)", func() {
				lastProcessed := 0
				slice := []channelPayload{
					{index: 0},
					{index: 1},
					{index: 5},
				}
				amount, ok := enabledToProcess(slice, greaterAmount, totalAmount, lastProcessed)
				Expect(amount).To(Equal(1))
				Expect(ok).To(Equal(true))
			})

			It("should return ture and 0 if the buffer only has 1 element", func() {
				slice := []channelPayload{{index: 0}}
				amount, ok := enabledToProcess(slice, greaterAmount, totalAmount, 0)
				Expect(amount).To(Equal(0))
				Expect(ok).To(Equal(true))
			})
		})

		It("it should provide an interface to pop elements", func() {
			total := 25
			slice := make([]channelPayload, total)
			for i := range slice {
				slice[i] = channelPayload{index: i}
			}
			shuffle(slice)

			// Integration Test
			out := make(chan int)
			go func() {
				counter, lastProcessed, buff := 0, 0, make([]channelPayload, 0, total)
				for _, cp := range slice {
					buff = append(buff, cp)
					sort.Slice(buff, func(i, j int) bool {
						return buff[i].index < buff[j].index
					})
					counter++

					if amount, ok := enabledToProcess(buff, counter, total, lastProcessed); ok {
						var (
							cp channelPayload
							c  int = 0
						)
						for {
							buff, cp = popFirstElement(buff)
							out <- cp.index
							if c == amount {
								lastProcessed = cp.index + 1
								break
							}
							c++
						}
					}
				}
			}()

			lastElement := 0
			for el := range out {
				Expect(el).To(Equal(lastElement))
				lastElement++
				if lastElement == total {
					close(out)
				}
			}
		})
	})
})

func shuffle(slice []channelPayload) {
	n := len(slice)
	for i := n - 1; i > 0; i-- {
		// Generate a ramdon index between 0 and i
		j := rand.Intn(i + 1)

		// Switch positions for the elements
		slice[i].index, slice[j].index = slice[j].index, slice[i].index
	}
}
