# Hang Connector

Demo to show how Kafka Connect tasks blocked on network operations can cause the worker to end up in an invalid state.

Instructions for running the connector (with stand-alone or distributed workers) are given below.
 
## Common Instructions

Clone and build the connector

```bash
git clone https://github.com/smarter-travel-media/hang-connector.git
cd hang-connector
mvn clean install
```

Get Kafka

```bash
git clone https://github.com/apache/kafka.git

# Follow the instructions for Kafka to set it up and build it
```

## Stand-Alone Connect


