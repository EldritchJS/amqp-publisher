[![Build Status](https://travis-ci.org/radanalyticsio/equoid-data-publisher.svg?branch=master)](https://travis-ci.org/radanalyticsio/equoid-data-publisher)

# equoid-data-publisher

## Build

`mvn package`

## Run

locally

```bash
AMQP_HOST=<AMQP Broker Address> \
AMQP_PORT=<AMQP Port> \
AMQP_USERNAME=<AMQP Username> \
AMQP_PASSWORD=<AMQP Password> \
QUEUE_NAME=<AMQP Queue> \
DATA_URL=<Data file URL> \
java -jar ./target/equoid-data-publisher-*.jar
```

or in POD:
```bash
oc new-app -l app=publisher redhat-openjdk18-openshift:1.2~https://github.com/radanalyticsio/equoid-data-publisher
```

in case you want to tweak some default parameters or debug the app, here is a tweaked example:

```bash
oc new-app redhat-openjdk18-openshift:1.2~https://github.com/radanalyticsio/equoid-data-publisher \
  -l app=publisher \
  -e AMQP_HOST=broker-amq-amqp \
  -e AMQP_PORT=5672 \
  -e AMQP_USERNAME=daikon \
  -e AMQP_PASSWORD=daikon \
  -e QUEUE_NAME=salesq \
  -e DATA_URL=https://raw.githubusercontent.com/radanalyticsio/equoid-data-publisher/master/data/LiquorNames.txt \
  -e JAVA_DEBUG=true \
  -e JAVA_DEBUG_PORT=9009
```
