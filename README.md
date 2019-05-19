# Howlett.Kafka.Extensions

Some lightweight abstractions over [Confluent.Kafka](https://github.com/confluentinc/confluent-kafka-dotnet) to make common use cases easy.

Early days! Let's get the ball rolling with:

## Stateless Stream Processor

- Specify the input topic and/or output topic and a transform function.
- Can be used for:
  - stateless stream processing.
  - data source.
  - data sink.

[example](examples/Stateless)
