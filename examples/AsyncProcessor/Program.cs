using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Howlett.Kafka.Extensions.Mock;
using Howlett.Kafka.Extensions.Streaming;
using Bogus;
using Bogus.DataSets;


namespace Howlett.Kafka.Extensions.Examples
{
    class Program
    {
        async static Task RecreateTopicsAsync(string brokerAddress, TopicSpecification[] topicSpecs)
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = brokerAddress }).Build())
            {
                try
                {
                    await adminClient.DeleteTopicsAsync(topicSpecs.Select(ts => ts.Name));
                }
                catch (DeleteTopicsException ex)
                {
                    foreach (var r in ex.Results)
                    {
                        if (r.Error.Code != ErrorCode.UnknownTopicOrPart)
                        {
                            throw;
                        }
                    }
                }

                await Task.Delay(TimeSpan.FromSeconds(2));
                await adminClient.CreateTopicsAsync(topicSpecs);
            }
        }

        async static Task Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("usage: .. <bootstrap servers>");
                Environment.Exit(1);
            }

            var brokerAddress = args[0];

            var simulatedWeblogTopic = "simulated-weblog";
            var filteredWeblogTopic = "filtered-weblog";

            var topicSpecs = new Dictionary<string, TopicSpecification>
            {
                { simulatedWeblogTopic, new TopicSpecification { Name = simulatedWeblogTopic, NumPartitions = 24, ReplicationFactor = 1 } },
                { filteredWeblogTopic, new TopicSpecification { Name = filteredWeblogTopic, NumPartitions = 24, ReplicationFactor = 1 } }
            };

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            await RecreateTopicsAsync(brokerAddress, topicSpecs.Values.ToArray());


            // 1. A processor that generates some fake weblog data.
            Random r = new Random();
            var fakeDataSourceProcessor = new Processor<Null, Null, Null, string>
            {
                Name = "fakegen",
                BootstrapServers = brokerAddress,
                OutputTopic = simulatedWeblogTopic,
                Function = (_) =>
                {
                    Thread.Sleep(1000);
                    return new Message<Null, string> { Value = WebLogLine.GenerateFake() };
                }
            };


            // 2. An async processor that does a (mock) geoip lookup, removes pii information
            //    (IP address), and repartitions by country.
            var transformProcessor = new AsyncProcessor<Null, string, string, string>
            {
                Name = "geo-lookup-processor",
                BootstrapServers = brokerAddress,
                InputTopic = simulatedWeblogTopic,
                OutputTopic = filteredWeblogTopic,
                OutputOrderPolicy = OutputOrder.InputOrder,
                ConsumeErrorTolerance = ErrorTolerance.All,
                Function = async (m) =>
                {
                    try
                    {
                        var logline = m.Value;
                        var firstSpaceIndex = logline.IndexOf(' ');
                        if (firstSpaceIndex < 0)
                        {
                            throw new FormatException("unexpected logline format");
                        }
                        var ip = logline.Substring(0, firstSpaceIndex);
                        var country = await MockGeoLookup.GetCountryFromIPAsync(ip);
                        var loglineWithoutIP = logline.Substring(firstSpaceIndex+1);
                        var dateStart = loglineWithoutIP.IndexOf('[');
                        var dateEnd = loglineWithoutIP.IndexOf(']');
                        if (dateStart < 0 || dateEnd < 0 || dateEnd < dateStart)
                        {
                            throw new FormatException("unexpected logline format");
                        }
                        var requestInfo = loglineWithoutIP.Substring(dateEnd + 2);
                        return new Message<string, string> { Key = country, Value = requestInfo };
                    }
                    catch (Exception)
                    {
                        // Unhandled exceptions in your processing function will cause the 
                        // processor to terminate.

                        return null; // null -> filter (don't write output message corresponding to input message).
                    }
                }
            };

            // 3. A processor that just writes messages to stdout.
            //    Note: Using a Processor would be better here, this
            //    just demonstrates AsyncProcessor can be used as a sink.
            var consoleWriterProcessor = new AsyncProcessor<string, string, Null, Null>
            {
                Name = "console-writer",
                BootstrapServers = brokerAddress,
                InputTopic = filteredWeblogTopic,
                OutputOrderPolicy = OutputOrder.InputOrder,
                Function = (m) =>
                {
                    Console.WriteLine($"{m.Key} ~~~ {m.Value}");
                    Message<Null, Null> msg = null;
                    return Task.FromResult(msg); // don't write anything to output topic.
                }
            };

            // start all the processors in their own threads. Note: typically
            // each task would be in it's own process, and these would typically
            // be spread across machines.
            var processorTasks = new List<Task>();
            processorTasks.Add(Task.Run(() => fakeDataSourceProcessor.Start("1", cts.Token)));
            processorTasks.Add(Task.Run(() => transformProcessor.Start("1", cts.Token)));
            processorTasks.Add(Task.Run(() => consoleWriterProcessor.Start("1", cts.Token)));

            var result = await Task.WhenAny(processorTasks);

            if (result.IsFaulted)
            {
                throw result.Exception;
            }
        }
    }
}
