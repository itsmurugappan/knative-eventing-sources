package sources

//S3SourceFake for testing
type S3SourceFake struct {
}

//SetCtx for testing
func (source *S3SourceFake) SetCtx() {
}

//ConstructCloudEventsClient for testing
func (source *S3SourceFake) ConstructCloudEventsClient() {
}

//GenerateEvents for testing
func (source *S3SourceFake) GenerateEvents() interface{} {
	//return some thing
	return "Events Generated"
}
