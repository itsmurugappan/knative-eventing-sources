package sources

type EventSource interface {
	SetCtx()
	ConstructCloudEventsClient()
	GenerateEvents() interface{}
}

func SourceEvents(source EventSource) interface{} {
	source.SetCtx()
	source.ConstructCloudEventsClient()
	return source.GenerateEvents()
}
