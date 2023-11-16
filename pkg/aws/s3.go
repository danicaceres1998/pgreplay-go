package aws

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	kingpin "github.com/alecthomas/kingpin/v2"
	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gocardless/pgreplay-go/pkg/pgreplay"
)

type ParserHelper struct {
	Parser pgreplay.ParserFunc
	Start  time.Time
	Finish time.Time
}

type channelPayload struct {
	index  int
	target *os.File
}

func CreateS3Client(cfg awsSDK.Config) s3.Client {
	return *s3.NewFromConfig(cfg)
}

func StreamItemsFromS3(ctx context.Context, logger kitlog.Logger, bucketName string, ph ParserHelper) chan pgreplay.Item {
	folder, ok := os.LookupEnv("PGREPLAY_PID")
	if !ok {
		kingpin.Fatalf("fatal to get the PGREPLAY_PID from the env vars")
	}

	cfg, err := VerifyAWSConfig(ctx)
	if err != nil {
		kingpin.Fatalf("fatal to connect to aws: %s", err)
	}

	s3Client := CreateS3Client(cfg)
	fileObjects, err := getAllLogFiles(ctx, s3Client, bucketName, folder, ph.Start, ph.Finish)
	if err != nil {
		kingpin.Fatalf("fatal to get log files from s3 bucket: %s", err)
	}

	// Processing all files
	out := make(chan pgreplay.Item, pgreplay.InitialScannerBufferSize)
	go func() {
		for f := range streamFiles(ctx, s3Client, logger, fileObjects, bucketName) {
			file, err := os.Open(f.Name())
			if err != nil {
				logger.Log("event", "file.error", "error", err)
				continue
			}

			items, logerrs, done := ph.Parser(file)
			go func() {
				logger.Log("event", "parse.finished", "file", file.Name(), "error", <-done)
			}()

			go func() {
				for err := range logerrs {
					level.Debug(logger).Log("event", "parse.error", "error", err)
				}
			}()

			// Sending the items
			for i := range items {
				out <- i
			}

			// Clean tmp File
			os.Remove(file.Name())
			file.Close()
			level.Debug(logger).Log("event", "file.cleaner", "msg", "file cleaned", "path", file.Name())
		}

		close(out)
	}()

	return out
}

// Private Functions //

func getAllLogFiles(ctx context.Context, s3Client s3.Client, bucketName, folder string, start, finish time.Time) ([]types.Object, error) {
	var currentToken *string = nil
	objects := make([]types.Object, 0, 1000)

	for {
		result, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &bucketName,
			Prefix:            &folder,
			ContinuationToken: currentToken,
		})
		if err != nil {
			return nil, err
		}

		for _, o := range result.Contents {
			if objectBetweenDates(*o.Key, start, finish) {
				objects = append(objects, o)
			}
		}

		if !result.IsTruncated {
			break
		}

		*currentToken = *result.ContinuationToken
	}

	return objects, nil
}

func streamFiles(ctx context.Context, s3Client s3.Client, logger kitlog.Logger, objects []types.Object, bucketName string) chan *os.File {
	parallel, total := 5, len(objects)
	out := make(chan *os.File, parallel)

	go func() {
		localBuffer, maxParallel := make(chan channelPayload, parallel*3), make(chan struct{}, parallel)
		for i := 0; i < parallel; i++ {
			maxParallel <- struct{}{}
		}

		// Local Buffer Configuration, this waits to the local buffer to accomplish 10 downloaded files
		go func() {
			counter, lastProcessed, buff := 0, 0, make([]channelPayload, 0, parallel*3)
			for f := range localBuffer {
				buff = append(buff, f)
				sort.Slice(buff, func(i, j int) bool {
					return buff[i].index < buff[j].index
				})
				counter++

				if amount, ok := enabledToProcess(buff, counter, total, lastProcessed); ok {
					var (
						cp channelPayload
						c  int = 0
					)
					// Sending the files
					for {
						buff, cp = popFirstElement(buff)
						out <- cp.target
						if c == amount {
							lastProcessed = cp.index + 1
							break
						}
						c++
					}
				}
			}

			// Closing channel
			close(out)
		}()

		var wg sync.WaitGroup
		for idx, obj := range objects {
			<-maxParallel
			wg.Add(1)
			go func(o types.Object, i int) {
				defer func() {
					maxParallel <- struct{}{}
					wg.Done()
				}()

				f, err := downloadFile(ctx, s3Client, bucketName, *o.Key, *o.Key)
				if err != nil {
					logger.Log("unable to download the file", "error", err)
				}

				localBuffer <- channelPayload{index: i, target: f}
			}(obj, idx)
		}

		// Waiting to all process to finish
		wg.Wait()
		// Closing all channels
		close(maxParallel)
		close(localBuffer)
	}()

	return out
}

func downloadFile(ctx context.Context, s3Client s3.Client, bucketName, objectKey, fileName string) (*os.File, error) {
	result, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucketName,
		Key:    &objectKey,
	})
	if err != nil {
		return nil, err
	}
	defer result.Body.Close()

	file, err := os.CreateTemp("/var/tmp/", fmt.Sprintf("%s_", strings.SplitN(fileName, "/", 2)[1]))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	body, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, err
	}

	_, err = file.Write(body)
	return file, err
}
