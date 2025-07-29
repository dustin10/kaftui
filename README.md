# kaftui

A simple TUI application which can be used to view records published to a [Kafka](https://kafka.apache.org/) topic.

## Quick Start

First, install the `kafktui` application using `cargo`.

```sh
> cargo install --git https://github.com/dustin10/kaftui.git
```

> Note that currently the `rdkafka` shared library is configured to be dynamically linked, so it should be installed
on the user's system before installation of the `kaftui` application.

Next, execute the application passing the `--bootstrap-servers` and `--topic` arguments and start viewing records from
the topic.

```sh
> kaftui --bootstrap-servers localhost:9092 --topic orders
```

To use a custom group id for the consumer simply specify it using the `--group-id` argument.

```sh
> kaftui --bootstrap-servers localhost:9092 --topic orders --group-id tui-consumer
```

## CLI Arguments

* `--bootstrap-servers, -b` - Host value used to set the bootstrap servers configuration for the Kafka consumer.
* `--topic, -t` - Name of the Kafka topic to consume records from.
* `--group-id, -g` - Id of the group that the application will use when consuming messages from the Kafka topic. By
default a group id will be generated from the hostname of the machine that is executing the application.
* `--filter, -f` - JSONPath filter that is applied to a record. Can be used to filter out any records from the Kafka
topic that the end user may not be interested in. A message will only be presented to the user if it matches the filter.
By default no filter is applied. See the [Filtering](#Filtering) section below for further details.
* `--seek-to` - CSV of colon separated pairs of partition and offset values that the Kafka consumer will seek to before
starting to consume records. For example, `0:42,1:10` would cause the consumer to seek to offset `42` on partition `0`
and offset `10` on partition `1`.
* `--profile, -p` - Specifies the name of pre-configured set of values that will be used as defaults for the execution
of the application. Profiles are stored in the `$HOME/.kaftui.json` file. Any other arguments specified when executing
the application will take precedence over the ones loaded from the profile. See the [Profiles](#Profiles) section below
for further details.
* `--consumer-properties-file` - Path to a `.properties` file containing additional configuration for the Kafka consumer
other than the bootstrap servers and group id. Typically used for configuration authentication, etc.
* `--max-records` - Maximum nunber of records that should be held in memory at any given time after being consumed from
the Kafka topic. Defaults to `256`.
* `--help, -h` - Prints the help text for the application to the terminal.

## Key Bindings

There are some key bindings for the `kaftui` application which are global while others are dependent upon the currently
active widget. The currently active key bindings will always be displayed in the UI on the right side of the footer.

### Global

The following key bindings apply no matter which widget is currently selected.

* `esc` - Exits the `kaftui` application.
* `tab` - Changes focus from the current widget to the next available one.
* `p` - Pause consumption of records from the Kafka topic.
* `r` - Resume consumption of records from the Kafka topic.
* `e` - Exports the currently selected record to a JSON file.

### Record List

When the record list is the widget with focus then the following key bindings will be active.

* `gg` - Select the first record in the list.
* `j` - Select the next record in the list.
* `k` - Select the previous record in the list.
* `G` - Select the last record in the list.

### Record Value

When the record value widget has focus then the following key bindings will be active.

* `j` - Scrolls the text in the panel down.
* `k` - Scrolls the text in the panel up.

## Filtering

## Profiles

## Theme

## Configuration Reference

The JSON below contains is a full example of the set of values which can be used to configure the application using the
`.kaftui.json` file.

```json
{
  "scrollFactor": 3,
  "exportDirectory": ".",
  "profiles": [{
    "name": "local",
    "bootstrapServers": "localhost:9092"
  }, {
    "name": "local-filtered",
    "bootstrapServers": "localhost:9092",
    "filter": "$.headers[?(@.tenantId=='42')]"
  }, {
    "name": "cloud",
    "bootstrapServers": "kafka-brokers.acme.com:9092",
    "consumerProperties": {
      "security.protocol": "SASL_SSL",
      "sasl.mechanisms": "PLAIN",
      "sasl.username": "<username>",
      "sasl.password": "<password>"
    }
  }],
  "theme": {
    "panelBorderColor": "FFFFFF",
    "selectedPanelBorderColor": "00FFFF",
    "statusTextColorPaused": "FF0000",
    "statusTextColorProcessing": "00FF00",
    "keyBindingsTextColor": "FFFFFF",
    "labelColor": "FFFFFF",
    "recordListColor": "FFFFFF"
  }
}
```

Most of the available configuration above was discussed in previous sections. The list below outlines the rest.

* `scrollFactor` - Determines the number of lines to scroll the record value panel with each keypress.
* `exportDirectory` - Specifies the directory on the file system where exported records should be saved.

