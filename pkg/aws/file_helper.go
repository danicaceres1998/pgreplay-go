package aws

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	downloadPercentage = 0.25
)

func objectBetweenDates(fileName string, start, finish time.Time) bool {
	date, err := extractDate(fileName)
	if err != nil {
		return false
	}

	return timeBetween(date, start, finish)
}

func extractDate(fileName string) (time.Time, error) {
	dataName := strings.Split(fileName, "_")
	if len(dataName) != 4 {
		return time.Time{}, fmt.Errorf("unable to parse the file date: %s", fileName)
	}
	unixTime, err := strconv.Atoi(dataName[3])
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(int64(unixTime), 0), nil
}

func timeBetween(t, min, max time.Time) bool {
	if min.After(max) {
		min, max = max, min
	}

	return (t.Equal(min) || t.After(min)) && (t.Equal(max) || t.Before(max))
}

func enabledToProcess(slice []channelPayload, counter, total, lastProcessed int) (int, bool) {
	// Enabled to start processing
	if (float64(counter) / float64(total)) < downloadPercentage {
		return 0, false
	}

	// Checking the first element
	if slice[0].index != lastProcessed {
		return 0, false
	}

	// The slice must be pre sorted
	c := 0
	for i := 0; i < len(slice); i++ {
		if i+1 == len(slice) {
			break
		}

		if slice[i].index != (slice[i+1].index - 1) {
			break
		}
		c++
	}

	return c, c > 0
}
