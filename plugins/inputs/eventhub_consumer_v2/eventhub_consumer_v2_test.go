package eventhub_consumer_v2

import (
	"fmt"
	"github.com/influxdata/telegraf"
	influx "github.com/influxdata/telegraf/plugins/parsers/influx/influx_upstream"
	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/stretchr/testify/require"
)

type mockDeliveryInfo struct {
	id        telegraf.TrackingID
	delivered bool
}

func (d mockDeliveryInfo) ID() telegraf.TrackingID {
	return d.id
}

func (d mockDeliveryInfo) Delivered() bool {
	return d.delivered
}

func TestInit(t *testing.T) {
	// Initialize EventHub object
	e := &EventHub{
		ConnectionString:        "Endpoint=sb://test-event-hub.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=testkey;EntityPath=testpath",
		StorageConnectionString: "BlobEndpoint=https://test.blob.core.windows.net;SharedAccessSignature=sp=racwdli&st=2020-01-01T10:00:00Z&se=2021-01-01T10:00:00Z&spr=https&sv=2022-11-02&sr=c&sig=testsig",
		StorageContainerName:    "testcontainer",

		Log: testutil.Logger{
			Name:  "eventhub_consumer_v2",
			Quiet: true,
		},
	}
	err := e.Init()
	require.NoError(t, err)
	require.NotNil(t, e.consumerClient)
	require.NotNil(t, e.checkpointStore)

	// Check default values
	require.Equal(t, time.Second, e.BatchInterval)
	require.Equal(t, 100, e.BatchSize)
	require.Equal(t, 1000, e.MaxUndeliveredBatches)
	require.Equal(t, azeventhubs.DefaultConsumerGroup, e.ConsumerGroup)
}

func TestCreateMetrics(t *testing.T) {
	// Mock time stamps
	time1 := time.Now()
	time2 := time1.Add(-time.Hour)
	time3 := time1.Add(time.Hour)

	// Initialize EventHub object
	e := &EventHub{}
	parser := &influx.Parser{}
	require.NoError(t, parser.Init())
	e.SetParser(parser)

	// Mock event data
	event := &azeventhubs.ReceivedEventData{
		EventData: azeventhubs.EventData{
			Body: []byte(fmt.Sprintf(`measurementName,testTag=tag123 testField="field123" %d`, time3.UnixNano())),
			Properties: map[string]interface{}{
				"property1": "value1",
				"property2": 123,
				"property3": "value3",
			},
		},
		SystemProperties: map[string]interface{}{
			"iothub-enqueuedtime":                  time1,
			"iothub-connection-device-id":          "device123",
			"iothub-connection-auth-generation-id": "gen123",
			"iothub-connection-auth-method":        "authMethod",
			"iothub-connection-module-id":          "module123",
		},
		EnqueuedTime:   &time2,
		SequenceNumber: 345,
		Offset:         678,
	}

	// Check baseline
	metrics, err := e.createMetrics(event)
	metric := metrics[0]
	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Equal(t, "measurementName", metric.Name())
	require.Len(t, metric.Tags(), 1)
	require.Equal(t, "tag123", metric.Tags()["testTag"])
	require.Len(t, metrics[0].Fields(), 1)
	require.Equal(t, "field123", metric.Fields()["testField"])
	require.Equal(t, time3.Unix(), metrics[0].Time().Unix())

	// Check with additional fields and tags
	e.PropertyFields = []string{"property1", "property2"}
	e.PropertyTags = []string{"property1", "property2"}
	e.EnqueuedTimeAsTs = false
	e.EnqueuedTimeField = "enqueued_time"
	e.IotHubEnqueuedTimeAsTs = false
	e.IoTHubEnqueuedTimeField = "iothub_enqueued_time"
	e.SequenceNumberField = "sequence_number"
	e.OffsetField = "offset"
	e.IoTHubConnectionDeviceIDTag = "device_id"
	e.IoTHubConnectionAuthGenerationIDTag = "auth_gen_id"
	e.IoTHubConnectionAuthMethodTag = "auth_method"
	e.IoTHubConnectionModuleIDTag = "module_id"
	metrics, err = e.createMetrics(event)
	metric = metrics[0]
	require.Equal(t, "value1", metric.Fields()["property1"])
	require.Equal(t, int64(123), metric.Fields()["property2"])
	require.NotContains(t, metric.Fields(), "property3")
	require.Equal(t, "value1", metric.Tags()["property1"])
	require.Equal(t, "123", metric.Tags()["property2"])
	require.NotContains(t, metric.Tags(), "property3")
	require.Equal(t, time2.UnixMilli(), metric.Fields()["enqueued_time"])
	require.Equal(t, time1.UnixMilli(), metric.Fields()["iothub_enqueued_time"])
	require.Equal(t, int64(345), metric.Fields()["sequence_number"])
	require.Equal(t, int64(678), metric.Fields()["offset"])
	require.Equal(t, "device123", metric.Tags()["device_id"])
	require.Equal(t, "gen123", metric.Tags()["auth_gen_id"])
	require.Equal(t, "authMethod", metric.Tags()["auth_method"])
	require.Equal(t, "module123", metric.Tags()["module_id"])
	require.Equal(t, time3.Unix(), metric.Time().Unix())

	// Check with EnqueuedTimeAsTs and IotHubEnqueuedTimeAsTs
	e.EnqueuedTimeAsTs = true
	metrics, err = e.createMetrics(event)
	metric = metrics[0]
	require.Equal(t, time2.Unix(), metric.Time().Unix())
	e.EnqueuedTimeAsTs = false
	e.IotHubEnqueuedTimeAsTs = true
	metrics, err = e.createMetrics(event)
	metric = metrics[0]
	require.Equal(t, time1.Unix(), metric.Time().Unix())
}

func TestTrackingRetry(t *testing.T) {

	// Initialize EventHub object
	e := &EventHub{
		MaxUndeliveredBatches: 10,
	}

	// Create partition
	ac := &testutil.Accumulator{}
	ps := e.newPartitionState(ac)

	// Add metric
	id := e.addMetricGroup(ps, &azeventhubs.ReceivedEventData{
		SequenceNumber: 1,
		Offset:         1,
	}, []telegraf.Metric{
		testutil.TestMetric(0, "metric1"),
	})
	ac.AssertContainsFields(t, "metric1", map[string]interface{}{"value": int64(0)})
	ac.ClearMetrics()

	// Report undelivered and check for retry
	e.onMetricGroupDelivered(ps, mockDeliveryInfo{
		id:        id,
		delivered: false,
	})
	ac.AssertContainsFields(t, "metric1", map[string]interface{}{"value": int64(0)})
}

func TestTrackingCheckpoint(t *testing.T) {

	// Initialize EventHub object
	e := &EventHub{
		MaxUndeliveredBatches: 10,
	}

	// Create partition
	ac := &testutil.Accumulator{}
	ps := e.newPartitionState(ac)

	// Add some metrics
	id1 := e.addMetricGroup(ps, &azeventhubs.ReceivedEventData{
		SequenceNumber: 1,
		Offset:         1,
	}, []telegraf.Metric{
		testutil.TestMetric(0, "metric1"),
	})
	id2 := e.addMetricGroup(ps, &azeventhubs.ReceivedEventData{
		SequenceNumber: 2,
		Offset:         2,
	}, []telegraf.Metric{
		testutil.TestMetric(0, "metric2"),
	})
	id3 := e.addMetricGroup(ps, &azeventhubs.ReceivedEventData{
		SequenceNumber: 3,
		Offset:         3,
	}, []telegraf.Metric{
		testutil.TestMetric(0, "metric3"),
	})
	id4 := e.addMetricGroup(ps, &azeventhubs.ReceivedEventData{
		SequenceNumber: 4,
		Offset:         4,
	}, []telegraf.Metric{
		testutil.TestMetric(0, "metric4"),
	})

	// Check metrics are present
	ac.AssertContainsFields(t, "metric1", map[string]interface{}{"value": int64(0)})
	ac.AssertContainsFields(t, "metric2", map[string]interface{}{"value": int64(0)})
	ac.AssertContainsFields(t, "metric3", map[string]interface{}{"value": int64(0)})
	ac.AssertContainsFields(t, "metric4", map[string]interface{}{"value": int64(0)})

	// Report delivered
	cp, ok := e.onMetricGroupDelivered(ps, mockDeliveryInfo{
		id:        id1,
		delivered: true,
	})
	assert.True(t, ok)
	assert.Equal(t, int64(1), cp.SequenceNumber)

	// Report delivered out of order
	cp, ok = e.onMetricGroupDelivered(ps, mockDeliveryInfo{
		id:        id3,
		delivered: true,
	})
	assert.False(t, ok)
	cp, ok = e.onMetricGroupDelivered(ps, mockDeliveryInfo{
		id:        id2,
		delivered: true,
	})
	assert.True(t, ok)
	assert.Equal(t, int64(3), cp.SequenceNumber)

	// Report undelivered
	cp, ok = e.onMetricGroupDelivered(ps, mockDeliveryInfo{
		id:        id4,
		delivered: false,
	})
	assert.False(t, ok)

	// Make sure resources are cleaned up
	assert.Len(t, ps.idMap, 1)
	assert.Len(t, ps.batchMap, 1)
}
