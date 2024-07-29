//go:generate ../../../tools/readme_config_includer/generator
package eventhub_consumer_v2

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/checkpoints"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"
)

//go:embed sample.conf
var sampleConfig string

var once sync.Once

type EventHub struct {
	ConnectionString string `toml:"eventhub_connection_string"`
	ConsumerGroup    string `toml:"eventhub_consumer_group"`

	StorageConnectionString string `toml:"checkpoint_connection_string"`
	StorageContainerName    string `toml:"checkpoint_container_name"`

	PrefetchSize  int32         `toml:"prefetch_size"`
	BatchInterval time.Duration `toml:"batch_interval"`
	BatchSize     int           `toml:"batch_size"`

	// Metadata
	EnqueuedTimeAsTs              bool     `toml:"enqueued_time_as_ts"`
	IotHubEnqueuedTimeAsTs        bool     `toml:"iot_hub_enqueued_time_as_ts"`
	PropertyFields                []string `toml:"property_fields"`
	PropertyTags                  []string `toml:"property_tags"`
	SequenceNumberField           string   `toml:"sequence_number_field"`
	OffsetField                   string   `toml:"offset_field"`
	PartitionIDTag                string   `toml:"partition_id_tag"`
	PartitionKeyTag               string   `toml:"partition_key_tag"`
	EnqueuedTimeField             string   `toml:"enqueued_time_field"`
	IoTHubEnqueuedTimeField       string   `toml:"iot_hub_enqueued_time_field"`
	IoTHubConnectionDeviceIDTag   string   `toml:"iot_hub_connection_device_id_tag"`
	IoTHubAuthGenerationIDTag     string   `toml:"iot_hub_auth_generation_id_tag"`
	IoTHubConnectionAuthMethodTag string   `toml:"iot_hub_connection_auth_method_tag"`
	IoTHubConnectionModuleIDTag   string   `toml:"iot_hub_connection_module_id_tag"`

	Log telegraf.Logger `toml:"-"`

	checkpointStore *checkpoints.BlobStore
	consumerClient  *azeventhubs.ConsumerClient
	processorCancel context.CancelFunc
	partitionGroup  sync.WaitGroup

	parser telegraf.Parser
}

func (*EventHub) SampleConfig() string {
	return sampleConfig
}

func (e *EventHub) SetParser(parser telegraf.Parser) {
	e.parser = parser
}

func (*EventHub) Gather(telegraf.Accumulator) error {
	return nil
}

func (e *EventHub) Init() (err error) {

	e.Log.Info("Initializing Event Hub consumer")

	// Set default configuration values
	if e.BatchInterval == 0 {
		e.BatchInterval = time.Second
	}
	if e.BatchSize == 0 {
		e.BatchSize = 1024
	}
	if e.ConsumerGroup == "" {
		e.ConsumerGroup = azeventhubs.DefaultConsumerGroup
	}

	// NOTE: the storageContainerName must exist before the checkpoint store can be used.
	checkpointClient, err := container.NewClientFromConnectionString(e.StorageConnectionString, e.StorageContainerName, nil)
	if err != nil {
		return err
	}

	e.checkpointStore, err = checkpoints.NewBlobStore(checkpointClient, nil)
	if err != nil {
		return err
	}

	e.consumerClient, err = azeventhubs.NewConsumerClientFromConnectionString(e.ConnectionString, "", e.ConsumerGroup, nil)
	if err != nil {
		return err
	}

	return nil
}

func (e *EventHub) Start(accumulator telegraf.Accumulator) error {

	e.Log.Info("Starting Event Hub consumer")

	// The Processor handles load balancing with other Processor instances, running in separate
	// processes or even on separate machines. Each one will use the checkpointStore to coordinate
	// state and ownership, dynamically.
	processor, err := azeventhubs.NewProcessor(e.consumerClient, e.checkpointStore, &azeventhubs.ProcessorOptions{
		Prefetch: e.PrefetchSize,
	})
	if err != nil {
		return err
	}

	// Run in the background, launching goroutines to process each partition
	go e.dispatchPartitionClients(processor)

	// Run the load balancer. The dispatchPartitionClients goroutine (launched above)
	// will receive and dispatch ProcessorPartitionClients as partitions are claimed.
	go func() {
		var processorCtx context.Context
		processorCtx, e.processorCancel = context.WithCancel(context.Background())
		if err := processor.Run(processorCtx); err != nil {
			e.Log.Errorf("Error running Event Hub processor: %v", err)
		}
	}()

	return nil
}

func (e *EventHub) dispatchPartitionClients(processor *azeventhubs.Processor) {
	for {
		processorPartitionClient := processor.NextPartitionClient(context.Background())
		if processorPartitionClient == nil {
			// Processor has stopped
			break
		}
		go e.processEventsForPartition(processorPartitionClient)
	}
}

func (e *EventHub) processEventsForPartition(partitionClient *azeventhubs.ProcessorPartitionClient) {

	defer func() {
		err := partitionClient.Close(context.Background())
		if err != nil {
			e.Log.Errorf("Error closing partition client: %v", err)
		}
	}()

	e.Log.Infof("Starting to receive for partition %s", partitionClient.PartitionID())
	for {
		// Wait up to e.BatchInterval for e.BatchSize events, otherwise returns whatever we collected during that time.
		receiveCtx, cancelReceive := context.WithTimeout(context.Background(), e.BatchInterval)
		events, err := partitionClient.ReceiveEvents(receiveCtx, e.BatchSize, nil)
		cancelReceive()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			var eventHubError *azeventhubs.Error
			if errors.As(err, &eventHubError) && eventHubError.Code == azeventhubs.ErrorCodeOwnershipLost {
				return
			}
			e.Log.Errorf("Error receiving events on partition %s: %v", partitionClient.PartitionID(), err)
			return
		}

		if len(events) == 0 {
			continue
		}

		metrics := make([]telegraf.Metric, 0, len(events))
		for _, event := range events {
			if m, err := e.createMetrics(event); err == nil {
				metrics = append(metrics, m...)
			}
		}

		// Updates the checkpoint with the latest event received. If processing needs to restart
		// it will restart from this point, automatically.
		updateCtx, cancelUpdate := context.WithTimeout(context.Background(), 30*time.Second)
		err = partitionClient.UpdateCheckpoint(updateCtx, events[len(events)-1], nil)
		cancelUpdate()
		if err != nil {
			e.Log.Errorf("Error updating partition %s checkpoint: %v", partitionClient.PartitionID(), err)
		}
	}
}

// createMetrics returns the Metrics from an Event.
func (e *EventHub) createMetrics(event *azeventhubs.ReceivedEventData) ([]telegraf.Metric, error) {
	metrics, err := e.parser.Parse(event.Body)
	if err != nil {
		return nil, err
	}

	if len(metrics) == 0 {
		once.Do(func() {
			e.Log.Debug(internal.NoMetricsCreatedMsg)
		})
	}

	for i := range metrics {
		for _, field := range e.PropertyFields {
			if val, ok := event.Properties[field]; ok {
				metrics[i].AddField(field, val)
			}
		}

		for _, tag := range e.PropertyTags {
			if val, ok := event.Properties[tag]; ok {
				metrics[i].AddTag(tag, fmt.Sprintf("%v", val))
			}
		}

		if e.EnqueuedTimeAsTs {
			metrics[i].SetTime(*event.EnqueuedTime)
		}
		if e.EnqueuedTimeField != "" {
			metrics[i].AddField(e.EnqueuedTimeField, (*event.EnqueuedTime).UnixMilli())
		}

		if t, ok := event.SystemProperties["iothub-enqueuedtime"].(time.Time); ok {
			if e.IotHubEnqueuedTimeAsTs {
				metrics[i].SetTime(t)
			}
			if e.IoTHubEnqueuedTimeField != "" {
				metrics[i].AddField(e.IoTHubEnqueuedTimeField, t.UnixMilli())
			}
		}

		if e.SequenceNumberField != "" {
			metrics[i].AddField(e.SequenceNumberField, event.SequenceNumber)
		}
		if e.OffsetField != "" {
			metrics[i].AddField(e.OffsetField, event.Offset)
		}

		if str, ok := event.SystemProperties["iothub-connection-device-id"].(string); ok && e.IoTHubConnectionDeviceIDTag != "" {
			metrics[i].AddTag(e.IoTHubConnectionDeviceIDTag, str)
		}
		if str, ok := event.SystemProperties["iothub-connection-auth-generation-id"].(string); ok && e.IoTHubAuthGenerationIDTag != "" {
			metrics[i].AddTag(e.IoTHubAuthGenerationIDTag, str)
		}
		if str, ok := event.SystemProperties["iothub-connection-auth-method"].(string); ok && e.IoTHubConnectionAuthMethodTag != "" {
			metrics[i].AddTag(e.IoTHubConnectionAuthMethodTag, str)
		}
		if str, ok := event.SystemProperties["iothub-connection-module-id"].(string); ok && e.IoTHubConnectionModuleIDTag != "" {
			metrics[i].AddTag(e.IoTHubConnectionModuleIDTag, str)
		}
	}

	return metrics, nil
}

func (e *EventHub) Stop() {
	e.Log.Info("Stopping Event Hub consumer")
	e.processorCancel()
	err := e.consumerClient.Close(context.Background())
	if err != nil {
		e.Log.Errorf("Error closing Event Hub connection: %v", err)
	}
}

func init() {
	inputs.Add("eventhub_consumer_v2", func() telegraf.Input {
		return &EventHub{}
	})
}
