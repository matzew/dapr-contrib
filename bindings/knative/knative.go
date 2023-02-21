package knative

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/cloudevents/sdk-go/v2/client"
	eventingclientset "knative.dev/eventing/pkg/client/clientset/versioned"
	"knative.dev/pkg/injection"
)

type Knative struct {
	logger         logger.Logger
	client         client.Client
	eventingClient *eventingclientset.Clientset
	brokerName     string
	namespace      string
}

func NewKnative(logger logger.Logger) bindings.OutputBinding {

	//	ctx := signals.NewContext()
	cfg := injection.ParseAndGetRESTConfigOrDie()

	client, err := cloudevents.NewClientHTTP()
	if err != nil {
		return nil
	}

	return &Knative{
		logger:         logger,
		client:         client,
		eventingClient: eventingclientset.NewForConfigOrDie(cfg),
	}
}

func (k *Knative) Init(_ context.Context, metadata bindings.Metadata) (err error) {

	if err = k.extractValueForKey("broker", k.brokerName, metadata); err != nil {
		return err
	}
	if err = k.extractValueForKey("namespace", k.namespace, metadata); err != nil {
		return err
	}

	return nil
}

func (k *Knative) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (k *Knative) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	return k.sendCloudEvent(ctx, req)
}

func (k *Knative) extractValueForKey(key string, field string, metadata bindings.Metadata) error {
	if value, ok := metadata.Properties[key]; ok && value != "" {
		field = value
		return nil
	}
	return fmt.Errorf("the %s parameter is required", key)
}
func (k *Knative) sendCloudEvent(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	event := cloudevents.NewEvent()
	event.SetSource("/dapr/system")
	event.SetType("dev.knative.dapr.binding")
	event.SetData(cloudevents.ApplicationCloudEventsJSON, map[string]string{"hello": "Dapr"})

	broker, err := k.eventingClient.EventingV1().Brokers(k.namespace).Get(ctx, k.brokerName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		//k.logger.Fatalf("Broker %s not found in namespace %s", k.brokerName, k.namespace)
		return nil, err
	}
	// Set a target.
	ctx = cloudevents.ContextWithTarget(ctx, broker.Status.Address.URL.String())

	// Send that Event.
	if result := k.client.Send(ctx, event); cloudevents.IsUndelivered(result) {
		k.logger.Fatalf("failed to send, %v", result)
	}

	return nil, nil
}
