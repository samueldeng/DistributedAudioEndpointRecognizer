#DistributedAudioEndpointRecognizer

A Audio Endpoint Recognizer based Storm Project

## Running topologies with Maven

you can run this topology in a local mode by using this command:

    $ mvn compile exec:java

## Submitting to a Storm Cluster

you can submit this topology by two steps.
first, generate the jar and send it to a host where there are storm installed.

	$ mvn package

second, on a storm host, just execute this command to submit the topology.

	$ storm jar *.jar  cn.xjtu.DistributedAudioEndpointRecognizer

## More function are comming.