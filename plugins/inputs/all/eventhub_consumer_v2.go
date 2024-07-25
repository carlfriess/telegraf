//go:build !custom || inputs || inputs.eventhub_consumer_v2

package all

import _ "github.com/influxdata/telegraf/plugins/inputs/eventhub_consumer_v2" // register plugin
