package sources

import (
	"context"
	"gotest.tools/assert"
	"net/http/httptest"
	"strings"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	cloudeventstest "github.com/cloudevents/sdk-go/v2/client/test"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	fakes3 "github.com/johannesboyne/gofakes3"
	s3mem "github.com/johannesboyne/gofakes3/backend/s3mem"
)

func TestS3EventSource(t *testing.T) {
	backend := s3mem.New()
	faker := fakes3.New(backend)
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	for _, tc := range []struct {
		name      string
		file      []string
		key       string
		event     []cloudevents.Event
		dumpCount string
		result    Result
	}{{
		name:      "1 line file",
		key:       "t1.txt",
		dumpCount: "5",
		file:      []string{"this is the first line", "\n"},
		event:     []cloudevents.Event{getEvent([]string{"this is the first line", "\n", "EOF"}, "1")},
		result:    Result{0, 1},
	}, {
		name:      "big file - test chunks",
		key:       "t2.txt",
		dumpCount: "5",
		result:    Result{0, 11},
		file: []string{
			"this is the first line", "\n",
			"this is the second line", "\n",
			"this is the third line", "\n",
			"this is the fourth line", "\n",
			"this is the fifth line", "\n",
			"this is the sixth line", "\n",
			"this is the seventh line", "\n",
			"this is the eighth line", "\n",
			"this is the nineth line", "\n",
			"this is the tenth line", "\n",
			"this is the eleventh line", "\n",
		},
		event: []cloudevents.Event{
			getEvent([]string{
				"this is the first line", "\n",
				"this is the second line", "\n",
				"this is the third line", "\n",
				"this is the fourth line", "\n",
				"this is the fifth line", "\n",
			}, "1"),
			getEvent([]string{
				"this is the sixth line", "\n",
				"this is the seventh line", "\n",
				"this is the eighth line", "\n",
				"this is the nineth line", "\n",
				"this is the tenth line", "\n",
			}, "2"),
			getEvent([]string{
				"this is the eleventh line", "\n", "EOF",
			}, "3"),
		},
	}, {
		name:      "test different dump count",
		key:       "t3.txt",
		dumpCount: "7",
		result:    Result{0, 7},
		file: []string{
			"this is the first line", "\n",
			"this is the second line", "\n",
			"this is the third line", "\n",
			"this is the fourth line", "\n",
			"this is the fifth line", "\n",
			"this is the sixth line", "\n",
			"this is the seventh line", "\n",
		},
		event: []cloudevents.Event{
			getEvent([]string{
				"this is the first line", "\n",
				"this is the second line", "\n",
				"this is the third line", "\n",
				"this is the fourth line", "\n",
				"this is the fifth line", "\n",
				"this is the sixth line", "\n",
				"this is the seventh line", "\n",
			}, "1"),
			getEvent([]string{
				"EOF",
			}, "2"),
		},
	}, {
		name:      "no line break",
		key:       "t3.txt",
		dumpCount: "7",
		result:    Result{0, 1},
		file: []string{
			"this is the first line",
			"this is the second line",
			"this is the third line",
			"this is the fourth line",
			"this is the fifth line",
			"this is the sixth line",
			"this is the seventh line",
		},
		event: []cloudevents.Event{
			getEvent([]string{
				"this is the first line",
				"this is the second line",
				"this is the third line",
				"this is the fourth line",
				"this is the fifth line",
				"this is the sixth line",
				"this is the seventh line", "EOF",
			}, "1"),
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			c, eventCh := cloudeventstest.NewMockSenderClient(t, len(tc.event))
			s3src := getS3Source(ts.URL, c)
			s3src.Connection.Key = tc.key
			s3src.Sink.DumpCount = tc.dumpCount
			MakeS3TestServer(ts, getFile(tc.file), tc.key)
			s3src.GenerateEvents()
			for _, e := range tc.event {
				assert.DeepEqual(t, e, <-eventCh)
			}
		})
	}
}

func getS3Source(u string, c cloudevents.Client) *S3Source {
	osCon := ObjectStoreConfig{
		Bucket:    "newbucket",
		Region:    "eu-central-1",
		Access:    "YOUR-ACCESSKEYID",
		Secret:    "YOUR-SECRETACCESSKEY",
		Endpoint:  u,
		ChunkSize: "100",
	}

	s3src := &S3Source{
		Connection:        osCon,
		cloudEventsClient: c,
		ctx:               context.TODO(),
	}
	return s3src
}

func getEvent(data []string, id string) cloudevents.Event {
	event := cloudevents.NewEvent()
	event.SetType("s3-flat-file-source")
	event.SetSource("https://github.com/itsmurugappan/s3-flat-file-source")
	event.SetID(id)
	event.SetData("text/json", strings.Join(data, ""))
	return event
}

func getFile(file []string) strings.Builder {
	fb := strings.Builder{}
	for _, line := range file {
		fb.WriteString(line)
	}
	return fb
}

func MakeS3TestServer(ts *httptest.Server, fb strings.Builder, fileName string) {
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials("YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", ""),
		Endpoint:         aws.String(ts.URL),
		Region:           aws.String("eu-central-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}
	newSession := session.New(s3Config)

	s3Client := s3.New(newSession)
	cparams := &s3.CreateBucketInput{
		Bucket: aws.String("newbucket"),
	}

	s3Client.CreateBucket(cparams)

	s3Client.PutObject(&s3.PutObjectInput{
		Body:   strings.NewReader(fb.String()),
		Bucket: aws.String("newbucket"),
		Key:    aws.String(fileName),
	})
}
