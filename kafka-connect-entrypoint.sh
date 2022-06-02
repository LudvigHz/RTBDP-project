#!/bin/bash

function deploy_plugins {
  # NOTE: place here the commands to install Kafka Connect plugins, e.g.:
  # confluent-hub install --no-prompt castorm/kafka-connect-http:0.8.6  # do not use with AVRO + Schema Registry and do not try parsing timestamps
  # confluent-hub install --no-prompt jcustenborder/kafka-connect-spooldir:2.0.62
  # confluent-hub install --no-prompt jcustenborder/kafka-connect-twitter:0.3.34
  # confluent-hub install --no-prompt C0urante/kafka-connect-reddit:latest
  # confluent-hub install --no-prompt kaliy/kafka-connect-rss:0.1.0
  # confluent-hub install --no-prompt cjmatta/kafka-connect-sse:1.0
  :
}

function deploy_connectors {
  # NOTE: place here the commands to deploy connectors using the installed plugins, e.g.:
  # deploy 'myfileconnector' '{
  # 	"connector.class" : "com.github.jcustenborder.kafka.connect.spooldir.SpoolDirLineDelimitedSourceConnector",
  # 	"topic" : "mytopic",
  # 	"input.file.pattern" : ".*",
  # 	"input.path": "/data",
  # 	"error.path": "/data",
  # 	"finished.path": "/data",
  # 	"cleanup.policy": "DELETE",
  # 	"empty.poll.wait.ms": 10000
  # }'
  # deploy 'mytwitterconnector' '{
  # 	"connector.class": "com.github.jcustenborder.kafka.connect.twitter.TwitterSourceConnector",
  # 	"process.deletes": "false",
  # 	"filter.keywords": "<COMMA-SEPARATED KEYWORDS, @MENTIONS, #HASHTAGS>",
  # 	"kafka.status.topic": "mytwittertopic",
  # 	"twitter.oauth.consumerKey": "<API KEY>",
  # 	"twitter.oauth.consumerSecret": "<API KEY SECRET>",
  # 	"twitter.oauth.accessToken": "<ACCESS TOKEN>",
  # 	"twitter.oauth.accessTokenSecret": "<ACCESS TOKEN SECRET>"
  # }'
  # deploy 'myredditconnector' '{
  # 	"connector.class": "com.github.c0urante.kafka.connect.reddit.RedditSourceConnector",
  # 	"posts.subreddits": "italy",
  # 	"comments.subreddits": "italy",
  # 	"reddit.log.http.requests": "true"
  # }'
  # deploy 'myrssconnector' '{
  # 	"connector.class": "org.kaliy.kafka.connect.rss.RssSourceConnector",
  # 	"rss.urls": "https://www.ansa.it/sito/ansait_rss.xml https://www.ansa.it/trentino/notizie/trentino_rss.xml ",
  # 	"topic": "myrsstopic"
  # }'
  # deploy 'mysseconnector' '{
  # 	"connector.class": "com.github.cjmatta.kafka.connect.sse.ServerSentEventsSourceConnector",
  # 	"sse.uri": "https://stream.wikimedia.org/v2/stream/recentchange",
  # 	"topic": "edits"
  # }'
  :
}

function deploy {
  curl -s -X PUT -H "Content-Type:application/json" "http://localhost:8083/connectors/$1/config" -d "$2"
}

if [ ! -f /usr/share/confluent-hub-components/initialized ]; then
  touch /usr/share/confluent-hub-components/initialized

  echo "Kafka Connect initialization started"
  deploy_plugins

  function wait_started_and_deploy_connectors() {
    while :; do
      status=$(curl -s -o /dev/null -w %{http_code} http://localhost:8083/connectors)
      [ "$status" -eq 200 ] && break
      sleep 1
    done
    echo "Kafka Connect started"
    deploy_connectors
    echo "Kafka Connect initialization completed"
  }

  wait_started_and_deploy_connectors & # run in parallel to exec
fi

exec /etc/confluent/docker/run # original command to start Kafka Connect (CMD directive)
