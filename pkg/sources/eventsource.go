package sources

//EventSource is a Interface for
//knative sink binding sources
type EventSource interface {
	SetCtx()
	ConstructCloudEventsClient()
	GenerateEvents() interface{}
}

//SourceEvents is the entry point for intializing client
//and generating events
func SourceEvents(source EventSource) interface{} {
	source.SetCtx()
	source.ConstructCloudEventsClient()
	return source.GenerateEvents()
}
