package aws

import (
	"context"
	"io"
	"os"
	"sync"

	kingpin "github.com/alecthomas/kingpin/v2"
	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gocardless/pgreplay-go/pkg/pgreplay"
)

func CreateS3Client(cfg awsSDK.Config) s3.Client {
	return *s3.NewFromConfig(cfg)
}

func StreamItemsFromS3(ctx context.Context, logger kitlog.Logger, parser pgreplay.ParserFunc, bucketName string) chan pgreplay.Item {
	folder, ok := os.LookupEnv("PGREPLAY_PID")
	if !ok {
		kingpin.Fatalf("fatal to get the PGREPLAY_PID from the env vars")
	}

	cfg, err := VerifyAWSConfig(ctx)
	if err != nil {
		kingpin.Fatalf("fatal to connect to aws: %s", err)
	}

	s3Client := CreateS3Client(cfg)
	fileObjects, err := getAllLogFiles(ctx, s3Client, bucketName, folder)
	if err != nil {
		kingpin.Fatalf("fatal to get log files from s3 bucket: %s", err)
	}

	// Processing all files
	out := make(chan pgreplay.Item, pgreplay.InitialScannerBufferSize)
	go func() {
		for file := range streamFiles(ctx, s3Client, logger, fileObjects, bucketName) {
			items, logerrs, done := parser(file)
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
			file.Close()
		}

		close(out)
	}()

	return out
}

func getAllLogFiles(ctx context.Context, s3Client s3.Client, bucketName, folder string) ([]types.Object, error) {
	var currentToken *string = nil
	objects := make([]types.Object, 1000)

	for {
		result, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &bucketName,
			Prefix:            &folder,
			ContinuationToken: currentToken,
		})
		if err != nil {
			return nil, err
		}

		objects = append(objects, result.Contents...)
		if !result.IsTruncated {
			break
		}

		*currentToken = *result.ContinuationToken
	}

	return objects, nil
}

func streamFiles(ctx context.Context, s3Client s3.Client, logger kitlog.Logger, objects []types.Object, bucketName string) chan *os.File {
	parallel := 5
	out := make(chan *os.File, 5)

	go func() {
		localBuffer, maxParallel := make(chan *os.File, parallel*3), make(chan struct{}, parallel)
		for i := 0; i < parallel; i++ {
			maxParallel <- struct{}{}
		}

		// Local Buffer Configuration, this waits to the local buffer to accomplish 10 downloaded files
		go func() {
			popFirstElement := func(slice []*os.File) ([]*os.File, *os.File) {
				newSlice := make([]*os.File, 0, cap(slice))
				return append(newSlice, slice[1:]...), slice[0]
			}

			counter, buff := 0, make([]*os.File, 0, parallel*3)
			for f := range localBuffer {
				buff = append(buff, f)
				if counter < 10 {
					counter++
					continue
				}

				var element *os.File
				for {
					buff, element = popFirstElement(buff)
					out <- element
					if len(buff) == 0 {
						break
					}
				}
			}
		}()

		var wg sync.WaitGroup
		for _, obj := range objects {
			<-maxParallel
			wg.Add(1)
			go func(o types.Object) {
				defer func() {
					maxParallel <- struct{}{}
					wg.Done()
				}()

				f, err := downloadFile(ctx, s3Client, bucketName, *o.Key, *o.Key)
				if err != nil {
					logger.Log("unable to download the file", "error", err)
				}

				localBuffer <- f
			}(obj)
		}

		// Waiting to all process to finish
		wg.Wait()
		// Closing all channels
		close(maxParallel)
		close(localBuffer)
		close(out)
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

	file, err := os.CreateTemp("/var/tmp/", fileName)
	if err != nil {
		return nil, err
	}

	body, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, err
	}

	_, err = file.Write(body)
	return file, err
}
