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

**Processor**:

```bash
$ cd processor
# Create a virtual environment, e.g:
$ python3.7 -m venv venv
$ source venv/bin/activate
# Install dependencies
$ pip install -r requirements.txt
# Run the processor
$ python processor.py
```

**Web app**:

> Open another shell to run the front-end beside the stream processor

```bash
$ cd webapp
# Install dependencies
$ yarn
# Run the web application
$ yarn run
# The application should now be running on http://localhost:3000
```
