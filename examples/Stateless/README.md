## Stateless Stream Processor Example

A multi-purpose console app demonstrating use of the `Processor` class. Usage (kafka broker @ 127.0.0.1:9092):

1. Use the `AdminClient` to (re)create topics `simulated-weblog` and `filtered-weblog`:

```
dotnet run recreate 127.0.0.1:9092
```

2. Use the `Processor` class and mock `WebLogLine` class to emit fake log messages to the `simulated-weblog` kafka topic.

```
dotnet run fakegen 127.0.0.1:9092 1
```

3. Use the `Processor` class and the mock `GeoLookup` class to process messages in the `simulated-weblog` class and write the results to the `filtered-weblog` Kafka topic, repartitioning by country.

```
dotnet run process 127.0.0.1:9092 1
```

4. Use the `Processor` class to read messages in the `filtered-weblog` Kafka topic and write them to the console.

```
dotnet run log 127.0.0.1:9092 1
```

Notes: 

1. Copy/paste the code & tweak to your own needs. More out-of-the-box flexibility planned. Open an issue with what you need that this can't do.
1. High-throughput (100k's messages / s).
1. At-least once semantics.
1. Scale by running multiple instances of each step.
