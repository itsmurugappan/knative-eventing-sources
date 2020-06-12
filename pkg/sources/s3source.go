package sources

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	duckv1 "knative.dev/pkg/apis/duck/v1"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

var (
	chunks        int64
	chunkSize     int
	sinkDumpCount int
	ceOverrides   *duckv1.CloudEventOverrides
	id            int
)

//S3Source houses all the state related to the event
type S3Source struct {
	buffer            string
	sinkData          []string
	cloudEventsClient cloudevents.Client
	sentCount         int
	processErrs       int
	s3Client          *s3.S3
	ctx               context.Context

	Sink       SinkConfig
	Connection ObjectStoreConfig
}

//SinkConfig houses the state of binding
type SinkConfig struct {
	URL           string `envconfig:"K_SINK"`
	CEOverrides   string `envconfig:"K_CE_OVERRIDES"`
	DumpCount     string `envconfig:"SINK_DUMP_COUNT" default:"100"`
	Retries       string `envconfig:"SINK_RETRY_COUNT" default:"3"`
	RetryInterval string `envconfig:"SINK_RETRY_INTERVAL" default:"1"`
}

//ObjectStoreConfig houses object store connection state
type ObjectStoreConfig struct {
	Bucket    string `envconfig:"S3_BUCKET"`
	Key       string `envconfig:"S3_FILE_NAME"`
	Region    string `envconfig:"S3_REGION"`
	Access    string `envconfig:"S3_ACCESS_KEY"`
	Secret    string `envconfig:"S3_SECRET_KEY"`
	Endpoint  string `envconfig:"S3_URL"`
	ChunkSize string `envconfig:"DOWNLOAD_CHUNK_SIZE" default:"500000000"`
}

//S3SourceData specifies the shape of the data transmitted
type S3SourceData struct {
	Data string
}

//Result specifies type returned by S3Source
type Result struct {
	ErrorCount int
	SentCount  int
}

func (source *S3Source) checkForOverrides() {
	ceo := source.Sink.CEOverrides
	if len(ceo) > 0 {
		overrides := duckv1.CloudEventOverrides{}
		err := json.Unmarshal([]byte(ceo), &overrides)
		if err != nil {
			log.Fatalf("[ERROR] Unparseable CloudEvents overrides %s: %v", ceo, err)
		}
		ceOverrides = &overrides
	}
}

func getCloudEvents() cloudevents.Event {
	id++
	event := cloudevents.NewEvent("1.0")
	event.SetType("s3-flat-file-source")
	event.SetSource("https://github.com/itsmurugappan/s3-flat-file-source")
	event.SetID(fmt.Sprintf("%d", id))

	if ceOverrides != nil {
		for n, v := range ceOverrides.Extensions {
			event.SetExtension(n, v)
		}
	}

	return event
}

//ConstructCloudEventsClient establishes HTTP client
func (source *S3Source) ConstructCloudEventsClient() {
	p, err := cloudevents.NewHTTP()
	if err != nil {
		log.Fatalf("failed to create http protocol: %s", err.Error())
	}

	c, err := cloudevents.NewClient(p, cloudevents.WithUUIDs(), cloudevents.WithTimeNow())
	if err != nil {
		log.Fatalf("failed to create client: %s", err.Error())
	}

	source.cloudEventsClient = c
}

//SetCtx set the context for the data transfer
func (source *S3Source) SetCtx() {
	ctx := cloudevents.ContextWithTarget(context.Background(), source.Sink.URL)
	interval, _ := time.ParseDuration(source.Sink.RetryInterval)
	retry, _ := strconv.Atoi(source.Sink.Retries)
	source.ctx = cloudevents.ContextWithRetriesExponentialBackoff(ctx, interval*time.Second, retry)
}

func (source *S3Source) createS3Session() {
	newSession := session.New(&aws.Config{
		Region:           &source.Connection.Region,
		Endpoint:         &source.Connection.Endpoint,
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials(source.Connection.Access, source.Connection.Secret, "")})

	source.s3Client = s3.New(newSession)
}

func (source *S3Source) createChunks() {
	head, _ := source.s3Client.HeadObject(&s3.HeadObjectInput{
		Bucket: &source.Connection.Bucket,
		Key:    &source.Connection.Key,
	})

	contentSize := *head.ContentLength
	chunkSize, _ = strconv.Atoi(source.Connection.ChunkSize)
	sinkDumpCount, _ = strconv.Atoi(source.Sink.DumpCount)
	chunks = contentSize/int64(chunkSize) + 1
	log.Printf("chunks to process: %d", chunks)
}

//GenerateEvents does the heavy lifting in the whole process
func (source *S3Source) GenerateEvents() interface{} {
	id = 0
	source.checkForOverrides()
	source.createS3Session()
	source.createChunks()

	startByte, endByte := int64(0), int64(chunkSize)

	for i := int64(0); i < chunks; i++ {
		log.Printf("Processing chunk %d, starting byte %d, ending byte %d ", i, startByte, endByte)
		results, err := source.s3Client.GetObject(&s3.GetObjectInput{
			Bucket: &source.Connection.Bucket,
			Key:    &source.Connection.Key,
			Range:  aws.String(fmt.Sprintf("bytes=%d-%d", startByte, endByte)),
		})
		if err != nil {
			source.processErrs = source.processErrs + 1
			log.Printf("Error Getting Object from S3 : %s", err.Error())
			break
		}

		rdr := results.Body
		defer rdr.Close()
		reader := bufio.NewReader(rdr)

		source.constructData(reader)

		startByte = endByte + 1
		endByte = endByte + int64(chunkSize)
	}

	source.flush()
	return Result{source.processErrs, source.sentCount - 1}
}

func (source *S3Source) constructData(reader *bufio.Reader) {
	for {
		stringRead, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				source.buffer = fmt.Sprintf("%s%s", source.buffer, stringRead)
				break
			} else {
				source.processErrs = source.processErrs + 1
				log.Printf("failed to read data: %s", err.Error())
			}
		}
		if source.buffer != "" {
			fmt.Println(source.buffer)
			source.sinkData = append(source.sinkData, fmt.Sprintf("%s%s", source.buffer, stringRead))
			source.buffer = ""
		} else {
			source.sinkData = append(source.sinkData, stringRead)
		}
		if len(source.sinkData) >= sinkDumpCount {
			source.pushToSink()
		}
	}
}

func (source *S3Source) pushToSink() {
	event := getCloudEvents()
	if err := event.SetData("application/json", S3SourceData{strings.Join(source.sinkData, "")}); err != nil {
		panic(err)
	}
	if res := source.cloudEventsClient.Send(source.ctx, event); !cloudevents.IsACK(res) {
		source.processErrs = source.processErrs + 1
		log.Printf("failed to send cloudevent: %v", res)
		return
	}
	//reset
	source.sentCount = source.sentCount + len(source.sinkData)
	source.sinkData = source.sinkData[:0]
}

func (source *S3Source) flush() {
	// flush buffer
	if source.buffer != "" {
		source.sinkData = append(source.sinkData, source.buffer)
	}

	source.sinkData = append(source.sinkData, "EOF")
	source.pushToSink()
}
