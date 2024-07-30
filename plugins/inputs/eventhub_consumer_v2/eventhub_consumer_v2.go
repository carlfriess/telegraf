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

type empty struct{}
type semaphore chan empty

type EventHub struct {
	ConnectionString string `toml:"eventhub_connection_string"`
	ConsumerGroup    string `toml:"eventhub_consumer_group"`

	StorageConnectionString string `toml:"checkpoint_connection_string"`
	StorageContainerName    string `toml:"checkpoint_container_name"`

	PrefetchSize          int32         `toml:"prefetch_size"`
	BatchInterval         time.Duration `toml:"batch_interval"`
	BatchSize             int           `toml:"batch_size"`
	MaxUndeliveredBatches int           `toml:"max_undelivered_batches"`

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

	Log    telegraf.Logger `toml:"-"`
	parser telegraf.Parser

	checkpointStore *checkpoints.BlobStore
	consumerClient  *azeventhubs.ConsumerClient
	processorCancel context.CancelFunc
	partitionGroup  sync.WaitGroup
}

type partitionState struct {
	sync.Mutex
	ac         telegraf.TrackingAccumulator
	ctx        context.Context
	cancel     context.CancelFunc
	sem        semaphore
	nextAdd    uint64
	nextRemove uint64
	idMap      map[telegraf.TrackingID]uint64
	batchMap   map[uint64]batchInfo
}

type batchInfo struct {
	metrics    []telegraf.Metric
	checkpoint *azeventhubs.ReceivedEventData
	delivered  bool
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
		e.BatchSize = 100
	}
	if e.MaxUndeliveredBatches == 0 {
		e.MaxUndeliveredBatches = 1000
	}
	if e.ConsumerGroup == "" {
		e.ConsumerGroup = azeventhubs.DefaultConsumerGroup
	}

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
	// The Processor handles load balancing with other Processor instances,
	// running in separate processes or even on separate machines. Each one will
	// use the checkpointStore to coordinate state and ownership, dynamically.
	processor, err := azeventhubs.NewProcessor(e.consumerClient, e.checkpointStore, &azeventhubs.ProcessorOptions{
		Prefetch: e.PrefetchSize,
	})
	if err != nil {
		return err
	}

	// Create a context for the processor to run in, so we can cancel it later.
	var processorCtx context.Context
	processorCtx, e.processorCancel = context.WithCancel(context.Background())

	// Run in the background, launching goroutines to process each partition.
	go e.dispatchPartitionClients(processor, accumulator)

	// Run the load balancer. The dispatchPartitionClients goroutine will
	// receive and dispatch ProcessorPartitionClients as partitions are claimed.
	go func() {
		if err := processor.Run(processorCtx); err != nil {
			e.Log.Errorf("Error running Event Hub processor: %v", err)
		}
	}()

	return nil
}

func (e *EventHub) dispatchPartitionClients(processor *azeventhubs.Processor, ac telegraf.Accumulator) {
	for {
		// Wait for the next partition to be claimed
		processorPartitionClient := processor.NextPartitionClient(context.Background())
		if processorPartitionClient == nil {
			// Processor has stopped
			break
		}

		// Launch goroutines to receive events and update checkpoints for this
		// partition once the events have been processed and delivered.
		ps := e.newPartitionState(ac)
		e.partitionGroup.Add(2)
		go e.processEventsForPartition(processorPartitionClient, ps)
		go e.updateCheckpointForPartition(processorPartitionClient, ps)
	}
}

func (e *EventHub) processEventsForPartition(client *azeventhubs.ProcessorPartitionClient, ps *partitionState) {

	defer func() {
		err := client.Close(context.Background())
		if err != nil {
			e.Log.Errorf("Error closing partition client: %v", err)
		}
		ps.cancel()
		e.partitionGroup.Done()
	}()

	e.Log.Infof("Starting to receive for partition %s", client.PartitionID())
	for {
		// Wait up to e.BatchInterval for e.BatchSize events, otherwise returns whatever we collected during that time.
		receiveCtx, cancelReceive := context.WithTimeout(context.Background(), e.BatchInterval)
		events, err := client.ReceiveEvents(receiveCtx, e.BatchSize, nil)
		cancelReceive()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			var eventHubError *azeventhubs.Error
			if errors.As(err, &eventHubError) && eventHubError.Code == azeventhubs.ErrorCodeOwnershipLost {
				e.Log.Infof("Ownership lost for partition %s", client.PartitionID())
				return
			}
			e.Log.Warnf("Error receiving events on partition %s: %v", client.PartitionID(), err)
			return
		}

		if len(events) == 0 {
			continue
		}

		// Parse and create metrics from the events
		metrics := make([]telegraf.Metric, 0, len(events))
		for _, event := range events {
			if m, err := e.createMetrics(event); err == nil {
				metrics = append(metrics, m...)
			}
		}

		// Add the metrics to the accumulator
		e.addMetricGroup(ps, events[len(events)-1], metrics)
	}
}

// newPartitionState creates a new partition state object.
func (e *EventHub) newPartitionState(ac telegraf.Accumulator) *partitionState {
	ctx, cancel := context.WithCancel(context.Background())
	return &partitionState{
		ac:       ac.WithTracking(e.MaxUndeliveredBatches),
		ctx:      ctx,
		cancel:   cancel,
		sem:      make(semaphore, e.MaxUndeliveredBatches),
		idMap:    make(map[telegraf.TrackingID]uint64),
		batchMap: make(map[uint64]batchInfo),
	}
}

// addMetricGroup adds a group of metrics to the accumulator, tracking them
// within the partition state.
func (e *EventHub) addMetricGroup(ps *partitionState, cp *azeventhubs.ReceivedEventData, metrics []telegraf.Metric) telegraf.TrackingID {
	ps.sem <- empty{}

	ps.Lock()
	defer ps.Unlock()

	id := ps.ac.AddTrackingMetricGroup(metrics)
	ps.idMap[id] = ps.nextAdd
	ps.batchMap[ps.nextAdd] = batchInfo{
		metrics:    metrics,
		checkpoint: cp,
		delivered:  false,
	}
	ps.nextAdd++
	return id
}

// onMetricGroupDelivered updates the tracking state for a delivered metric
// group and returns a checkpoint if possible.
func (e *EventHub) onMetricGroupDelivered(ps *partitionState, info telegraf.DeliveryInfo) (*azeventhubs.ReceivedEventData, bool) {
	ps.Lock()
	defer ps.Unlock()

	// Look up batch information for this tracking ID
	id := info.ID()
	idx := ps.idMap[id]
	delete(ps.idMap, id)
	if batch, ok := ps.batchMap[idx]; ok {

		// Mark as delivered or retry if undelivered
		if info.Delivered() {
			batch.delivered = true
			ps.batchMap[idx] = batch
		} else {
			newId := ps.ac.AddTrackingMetricGroup(batch.metrics)
			ps.idMap[newId] = idx
			return nil, false
		}

	} else {
		// This should never happen
		e.Log.Errorf("Failed to find batch info for tracking ID %v / %d", info.ID(), idx)
	}

	// Check if we can update the checkpoint
	var checkpoint *azeventhubs.ReceivedEventData
	update := false
	for {
		if batch, ok := ps.batchMap[ps.nextRemove]; ok && batch.delivered {
			checkpoint = batch.checkpoint
			update = true
			delete(ps.batchMap, ps.nextRemove)
			<-ps.sem
			ps.nextRemove++
		} else {
			break
		}
	}
	return checkpoint, update
}

// updateCheckpointForPartition continuously updates the checkpoint for a partition based on tracking results.
func (e *EventHub) updateCheckpointForPartition(client *azeventhubs.ProcessorPartitionClient, ps *partitionState) {
	defer e.partitionGroup.Done()

	for {
		select {
		case <-ps.ctx.Done():
			return
		case info := <-ps.ac.Delivered():
			if checkpoint, update := e.onMetricGroupDelivered(ps, info); update {
				updateCtx, cancelUpdate := context.WithTimeout(context.Background(), 30*time.Second)
				err := client.UpdateCheckpoint(updateCtx, checkpoint, nil)
				cancelUpdate()
				if err != nil {
					e.Log.Errorf("Error updating partition %s checkpoint: %v", client.PartitionID(), err)
				}
			}
		}
	}

}

// createMetrics returns the Metrics from an Event.
func (e *EventHub) createMetrics(event *azeventhubs.ReceivedEventData) ([]telegraf.Metric, error) {
	// Parse the event body using the configured parser
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
		// Add application-specific fields and tags from the event properties
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

		// Handle the enqueued timestamp according to the configuration
		if e.EnqueuedTimeAsTs {
			metrics[i].SetTime(*event.EnqueuedTime)
		}
		if e.EnqueuedTimeField != "" {
			metrics[i].AddField(e.EnqueuedTimeField, (*event.EnqueuedTime).UnixMilli())
		}

		// Handle the IoT Hub enqueued timestamp according to the configuration
		if t, ok := event.SystemProperties["iothub-enqueuedtime"].(time.Time); ok {
			if e.IotHubEnqueuedTimeAsTs {
				metrics[i].SetTime(t)
			}
			if e.IoTHubEnqueuedTimeField != "" {
				metrics[i].AddField(e.IoTHubEnqueuedTimeField, t.UnixMilli())
			}
		}

		// Add sequence number and offset fields
		if e.SequenceNumberField != "" {
			metrics[i].AddField(e.SequenceNumberField, event.SequenceNumber)
		}
		if e.OffsetField != "" {
			metrics[i].AddField(e.OffsetField, event.Offset)
		}

		// Add IoT Hub connection properties as tags
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
	// Cancel the processor context to stop claiming new partitions and close
	// currently owned partitions.
	e.processorCancel()

	// Wait for all partition processing goroutines to finish
	e.partitionGroup.Wait()

	// Close the consumer client
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
