# Event Hub Consumer Input Plugin v2

This plugin provides a consumer for use with Azure Event Hubs and Azure IoT Hub.
It uses an Azure Blob Storage Container to store checkpoints, as well as
coordinate ownership of partitions and load balancing between multiple instances
of this plugin connected to the same Event Hub and consumer group.

## Azure IoT Hub Setup

This plugin is also compatible with the Event Hub compatible endpoint of an
Azure IoT Hub. In addition, the following configuration options can be used to
annotate metrics with IoT Hub specific metadata:

- `iot_hub_enqueued_time_as_ts`
- `iot_hub_enqueued_time_field`
- `iot_hub_connection_device_id_tag`
- `iot_hub_connection_auth_generation_id_tag`
- `iot_hub_connection_auth_method_tag`
- `iot_hub_connection_module_id_tag`

## Service Input <!-- @/docs/includes/service_input.md -->

This plugin is a service input. Normal plugins gather metrics determined by the
interval setting. Service plugins start a service to listens and waits for
metrics or events to occur. Service plugins have two key differences from
normal plugins:

1. The global or plugin specific `interval` setting may not apply
2. The CLI options of `--test`, `--test-wait`, and `--once` may not produce
   output for this plugin

## Global configuration options <!-- @/docs/includes/plugin_config.md -->

In addition to the plugin-specific configuration settings, plugins support
additional global and plugin configuration settings. These settings are used to
modify metrics, tags, and field or create aliases and configure ordering, etc.
See the [CONFIGURATION.md][CONFIGURATION.md] for more details.

[CONFIGURATION.md]: ../../../docs/CONFIGURATION.md#plugins

## Configuration

```toml @sample.conf
# Azure Event Hubs service input plugin v2
[[inputs.eventhub_consumer_v2]]

  ## The connection string used to connect to the Event Hub.
  ## It should should contain the Event Hub name.
  eventhub_connection_string = ""

  ## The consumer group of the Event Hub that should be used.
  # eventhub_consumer_group = "$Default"

  ## The connection string of the Azure Storage Account used to store the
  ## Event Hub checkpoints.
  checkpoint_connection_string = ""

  ## The name of the container in the Azure Storage Account used to store the
  ## Event Hub checkpoints. The container should already exist.
  checkpoint_container_name = ""

  ## The number of events to prefetch from the Event Hub asynchronously.
  ## This reduces the likelihood of having to wait for more events to arrive
  ## from the network.
  # prefetch_size = 300

  ## The amount of time that is waited during each read from the Event Hub
  ## before processing the batch.
  # batch_interval = "1s"

  ## The maximum amount of messages that are read and processed from the
  ## Event Hub in each batch. In most situations, this should be equal or
  ## smaller than prefetch_size.
  # batch_size = 100

  ## Max undelivered batches
  ## This plugin uses tracking metrics, which ensure messages are read to
  ## outputs before updating the partition checkpoints to ensure data is not
  ## lost. This option sets the maximum number of batches to read from the
  ## broker that have not been written by an output.
  ##
  ## This value needs to be picked with awareness of the agent's
  ## metric_batch_size value as well. Setting max undelivered batches too high
  ## can result in a constant stream of data batches to the output. While
  ## setting it too low may never flush the metric batches.
  # max_undelivered_batches = 1000

  ## Set either option below to true to use a system property as timestamp.
  ## You have the choice between EnqueuedTime and IoTHubEnqueuedTime.
  ## It's recommended to use this setting when the data itself has no timestamp.
  # enqueued_time_as_ts = true
  # iot_hub_enqueued_time_as_ts = true

  ## Tags or fields to create from keys present in the application property bag.
  ## These could for example be set by message enrichments in Azure IoT Hub.
  # property_tags = []
  # property_fields = []

  ## Tag or field name to use for metadata.
  ## By default all metadata is disabled.
  # sequence_number_field = "SequenceNumber"
  # offset_field = "Offset"
  # enqueued_time_field = "EnqueuedTime"
  # iot_hub_enqueued_time_field = "IoTHubEnqueuedTime"
  # iot_hub_connection_device_id_tag = "IoTHubConnectionDeviceID"
  # iot_hub_connection_auth_generation_id_tag = "IoTHubConnectionAuthGenerationID"
  # iot_hub_connection_auth_method_tag = "IoTHubConnectionAuthMethod"
  # iot_hub_connection_module_id_tag = "IoTHubConnectionModuleID"

  ## Data format to consume.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"

```
