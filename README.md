# Real-time visualization of delays in the norwegian public transport

> This repository is part of a project in the course Real-Time Big Data Processing at unibz.

## Requirements

This project requires

- `docker` and `docker-compose`
- `python 3.7`
- `yarn` and `node 16`

## Setup

> All setup instructions assume a \*nix system, preferably some Linux variant.

First, start all all the docker containers for kafka and flink: `docker-compose up -d`.

**Python setup**

The producer and processor can run using the same python interpreter and virtual environment.

```bash
# Create virtual environment
$ python3.7 -m venv venv
$ source venv/bin/activate
# Install dependencies
$ pip install -r requirements.txt
```

> NOTE: It's important to run the producer before the processor, as it sets up the kafka topic used
> for producing.

Run the producer:

```bash
$ python producer/producer.py
```

Run the processor (in another shell):

```bash
$ pytbon processor/processor.py
```

For the processor, you also need the jar for the kafka sql connector for flink. The latest version
can be found [here](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/kafka/).

As of writing, version [`1.15.0`](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.15.0/flink-sql-connector-kafka-1.15.0.jar) is used

**Web app**:

> Open another shell to run the front-end beside the stream processor

```bash
$ cd webapp
```

First, run the `kafka-proxy`. This application will create a WebSocket proxy to kafka that we can
use to access the kafka topic directly from the browser.

```bash
$ node kafka-proxy.js
```

> Then, start the webapp

```bash
# Install dependencies
$ yarn
# Run the web application
$ yarn dev
# The application should now be running on http://localhost:3000
```
