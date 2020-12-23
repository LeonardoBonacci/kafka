Where Franz meets Wolfgang Amadeus...
=================
When one reads the [docs](https://kafka.apache.org/documentation/#record) on the Kafka protocol it soon becomes clear that there *is* such a thing as a 'free lunch'.
One byte for each record: unused.

What's the plan?
Make changes to the source code of the Kafka clients, both producer and consumer, to send an input stream byte by byte onto the broker, and then allow the consumer to read these bytes and reconstruct the original input stream.

Why? Now we can pass Mozart into a KafkaProducer to allow a KafkaConsumer to play it when processing the messages. Why not? Allegro!

Code changes are made on [this branch](https://github.com/LeonardoBonacci/kafka/tree/2.7-piggyback).
