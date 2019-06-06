## Async Stream Processor Example

A console app demonstrating use of the `AsyncProcessor` class.

In this example, multiple `AsyncProcessor` instances are run in the same process on different threads.

Usage:

```
dotnet run 127.0.0.1:9092
```

Notes:

1. Copy/paste the code & tweak to your own needs. Open an issue with what you need that this can't do.
1. The AsyncProcessor class enables you to:
    1. Limit the maximum number of outstanding tasks.
    1. Write results as per input order or task completion order.

