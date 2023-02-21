package knative

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"

	"fmt"

	"github.com/cloudevents/sdk-go/v2/client"
)

type Knative struct {
	logger         logger.Logger
	client         client.Client
	brokerName     string
	namespace      string
}

// NewKnative returns a new Knative output binding instance
func NewKnative(logger logger.Logger) bindings.OutputBinding {

	client, err := cloudevents.NewClientHTTP()
	if err != nil {
		return nil
	}

	return &Knative{
		logger:         logger,
		client:         client,
	}
}

// Init performs metadata parsing.
func (k *Knative) Init(_ context.Context, metadata bindings.Metadata) (err error) {

	if err = k.extractValueForKey("broker", k.brokerName, metadata); err != nil {
		return err
	}
	if err = k.extractValueForKey("namespace", k.namespace, metadata); err != nil {
		return err
	}

	return nil
}

// Operations returns supported operations for this binding.
func (k *Knative) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

// Invoke performs an HTTP request to the configured HTTP endpoint.
func (k *Knative) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	return k.sendCloudEvent(ctx, req)
}

// helper to extract metadata from the binding manifest
func (k *Knative) extractValueForKey(key string, field string, metadata bindings.Metadata) error {
	if value, ok := metadata.Properties[key]; ok && value != "" {
		field = value
		return nil
	}
	return fmt.Errorf("the %s parameter is required", key)
}

// helper to send a CloudEvent to the Knative broker
func (k *Knative) sendCloudEvent(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	event := cloudevents.NewEvent()
	event.SetSource("/dapr/system")
	event.SetType("dev.knative.dapr.binding")
	event.SetData(cloudevents.ApplicationCloudEventsJSON, map[string]string{"hello": "Dapr"})

	// Set a target.
	ctx = cloudevents.ContextWithTarget(ctx, buildUriString(k.namespace, k.brokerName))

	// Send that Event.
	if result := k.client.Send(ctx, event); cloudevents.IsUndelivered(result) {
		k.logger.Fatalf("failed to send, %v", result)
	}

	return nil, nil
}

// helper to build the URI for the Knative broker
func buildUriString(namespace, brokername string) string {
	// TODO: fix the ingress service host name
	return fmt.Sprintf("http://kafka-broker-ingress.knative-eventing.svc.cluster.local/%s/%s", namespace, brokername)
}
