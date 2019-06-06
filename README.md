# Howlett.Kafka.Extensions

Some lightweight abstractions over [Confluent.Kafka](https://github.com/confluentinc/confluent-kafka-dotnet) to make common use cases easy.

[What I'm Buildng And Why](https://www.matthowlett.com/2019-06-02-stream-processing-pt1-what-and-why.html)

Early days! Let's get the ball rolling with:

## Stateless Stream Processor

- Specify the input topic and/or output topic and a transform function.
- Can be used for:
  - stateless stream processing.
  - data source.
  - data sink.

[example](examples/Stateless)

## Async Stream Processor

- Apply async functions to input messages.
    - Manage many simultaneously executing tasks.
    - Specify output order as input order or task completion order.
    - Use as a processor, filter or sink (not yet source).

[example](examples/AsyncProcessor)
