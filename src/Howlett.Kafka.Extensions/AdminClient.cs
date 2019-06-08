using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;


namespace Howlett.Kafka.Extensions
{
    public class AdminClient : IDisposable
    {
        private Confluent.Kafka.IAdminClient client;

        public AdminClient(AdminClientConfig config)
        {
            client = new AdminClientBuilder(config).Build();
        }

        public async Task DeleteTopicAsync(string topic)
        {
            await client.DeleteTopicsAsync(new List<string> { topic }).ConfigureAwait(false);
        }

        public async Task DeleteTopicMaybeAsync(string topic)
        {
            try
            {
                await client.DeleteTopicsAsync(new List<string> { topic }).ConfigureAwait(false);
            }
            catch (DeleteTopicsException e)
            {
                if (e.Results[0].Error.Code != ErrorCode.UnknownTopicOrPart)
                {
                    throw e;
                }
            }
        }

        public async Task<Dictionary<string, ConfigEntryResult>> DescribeTopicAsync(string topic)
        {
            var r = await client.DescribeConfigsAsync(new List<ConfigResource> { new ConfigResource {
                Name = topic,
                Type = ResourceType.Topic
            } }).ConfigureAwait(false);

            return r[0].Entries;
        }

        public async Task CreateTopicAsync(string topic, int numPartitions, short replicationFactor, Dictionary<string, string> configs)
        {
            await client.CreateTopicsAsync(new List<TopicSpecification> { new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = numPartitions,
                    ReplicationFactor = replicationFactor,
                    Configs = configs
                } }).ConfigureAwait(false);
        }

        public async Task CreateTopicMaybeAsync(string topic, int numPartitions, short replicationFactor, Dictionary<string, string> configs)
        {
            try
            {
                // throws if topic exists. won't create topic if it doesn't.
                await DescribeTopicAsync(topic).ConfigureAwait(false);
            }
            catch (DescribeConfigsException e)
            {
                // if the error is that the topic already exists, ignore, else it's a real error.
                if (e.Results[0].Error.Code != ErrorCode.UnknownTopicOrPart)
                {
                    throw e;
                }

                // create topic.
                await CreateTopicAsync(topic, numPartitions, replicationFactor, configs).ConfigureAwait(false);
            }
        }

        public void Dispose()
        {
            client.Dispose();
        }
    }
}