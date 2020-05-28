package sources

import ()

type S3SourceFake struct {
}

func (source *S3SourceFake) SetCtx() {
}

func (source *S3SourceFake) ConstructCloudEventsClient() {
}

func (source *S3SourceFake) GenerateEvents() interface{} {
	//return some thing
	return "Events Generated"
}
