# dataflow-twitter-stream-java

This repository contains the `pubsub-to-bq` app that shows how to use a [Google Cloud Dataflow](https://cloud.google.com/dataflow/) pipeline to stream Tweets from a [Google Cloud PubSub](https://cloud.google.com/pubsub/docs) topic to a [BigQuery](https://cloud.google.com/bigquery/) table. This app is an alternative/updated version of the GoogleCloudPlatform `pubsub` app in [kubernetes-bigquery-python](https://github.com/GoogleCloudPlatform/kubernetes-bigquery-python/tree/master/pubsub). 

This new version attempts to improve on the original in the following ways (to be updated):

**Leverage the Dataflow platform**

**Python Client Library implementation for streaming Tweets to Pub/Sub**

See [pubsub-twitter-stream-python](https://github.com/svanstiphout/pubsub-twitter-stream-python)