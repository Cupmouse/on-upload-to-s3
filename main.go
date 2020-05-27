package main

import (
	"bufio"
	"compress/gzip"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	sc "github.com/exchangedataset/streamcommons"
)

func getStartAndEnd(objectKey string) (start int64, end int64, isStart bool, err error) {
	var reader io.ReadCloser
	reader, err = sc.GetS3Object(objectKey)
	if err != nil {
		return
	}
	defer func() {
		serr := reader.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("%v, original error was: %v", serr, err)
			} else {
				err = serr
			}
		}
	}()

	var greader *gzip.Reader
	greader, err = gzip.NewReader(reader)
	if err != nil {
		return
	}
	defer func() {
		serr := greader.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("%v, original error was: %v", serr, err)
			} else {
				err = serr
			}
		}
	}()
	breader := bufio.NewReader(greader)
	// start reading line by line
	for {
		var lineType string
		lineType, err = breader.ReadString('\t')
		if err != nil {
			if err == io.EOF {
				// ignore EOF error at the beginning of line
				err = nil
			}
			return
		}
		if start == 0 {
			// this is the first line
			if lineType == "start\t" {
				// this is the first line in continuous dataset
				isStart = true
			}
		}
		// read timestamp from line
		var timestampStr string
		timestampStr, err = breader.ReadString('\t')
		if err != nil {
			return
		}
		// convert timestamp from string to int64
		var timestamp int64
		timestamp, err = strconv.ParseInt(timestampStr[:len(timestampStr)-1], 10, 64)
		if err != nil {
			return
		}
		if start == 0 {
			// this is the first line, store its timestamp
			start = timestamp
		}
		// update the last timestamp
		end = timestamp

		// skip the rest of the line
		_, err = breader.ReadBytes('\n')
		if err != nil {
			return
		}
	}
}

func handleRequest(event events.S3Event) (err error) {
	logger := log.New(os.Stdout, "on-upload-to-s3", log.LstdFlags)
	logger.Println("connecting to main db")
	var db *sql.DB
	db, err = sc.ConnectDatabase()
	if err != nil {
		for _, record := range event.Records {
			logger.Println("could not process for:", record.S3.Object.Key)
		}
		return
	}
	defer func() {
		serr := db.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("%v, original error was: %v", serr, err)
			} else {
				err = serr
			}
		}
	}()
	for i, record := range event.Records {
		objectKey := record.S3.Object.Key
		exchange := objectKey[:strings.IndexRune(objectKey, '_')]
		var startNanosec, endNanosec int64
		var isStart bool
		startNanosec, endNanosec, isStart, err = getStartAndEnd(objectKey)
		if err != nil {
			logger.Println("offset:", i)
			for _, record := range event.Records {
				logger.Println("could not process for:", record.S3.Object.Key)
			}
			return
		}
		logger.Printf("object %s is uploaded to s3\n", objectKey)

		_, err = db.Exec("INSERT INTO dataset_info.datasets VALUES(?, ?, ?, ?, ?)", objectKey, exchange, startNanosec, endNanosec, isStart)
		if err != nil {
			logger.Println("offset:", i)
			for _, record := range event.Records {
				logger.Println("could not process for:", record.S3.Object.Key)
			}
			return
		}
	}
	return
}

func main() {
	lambda.Start(handleRequest)
}
