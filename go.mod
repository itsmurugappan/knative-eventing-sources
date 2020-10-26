module github.com/itsmurugappan/knative-eventing-sources

go 1.15

require (
	github.com/aws/aws-sdk-go v1.31.12
	github.com/cloudevents/sdk-go/v2 v2.0.0
	github.com/google/uuid v1.1.1
	github.com/johannesboyne/gofakes3 v0.0.0-20200510090907-02d71f533bec
	github.com/stretchr/testify v1.5.1
	gotest.tools v2.2.0+incompatible
	knative.dev/pkg v0.0.0-20201026165541-2bc79285d20c
)

replace k8s.io/client-go => k8s.io/client-go v0.18.8
