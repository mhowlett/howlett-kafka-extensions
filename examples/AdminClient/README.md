## AdminClient Example

The standard admin API provided by Kafka clients prioritizes flexibility, which is great if you need that, but awkward if you don't (and much of the time you won't).

This example demonstrates a simplified Admin API for working with Kafka topics. Available methods:

- `CreateTopicAsync` - Create a single topic.
- `CreateTopicMaybeAsync` - Create a topic if it doesn't exist.
- `DeleteTopicAsync` - Delete a topic (throws if topic doesn't exist)
- `DeleteTopicMaybeAsync` - Delete a topic, if it exists.
- `DescribeTopicAsync` - Get configs for a single topic.

