using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;


namespace Howlett.Kafka.Extensions.Experiment
{
    public class SubTable : IDisposable
    {
        private TimeSpan defaultTimeoutMs = TimeSpan.FromSeconds(10);

        private TableSpecification tableSpecification;
        private int partition;
        private int replica;
        private string columnName;

        private IProducer<string, string> producer;

        private IConsumer<string, string> commandConsumer;
        private IConsumer<string, string> changeLogConsumer;


        private void CreateTopicsIfRequired(string bootstrapServers)
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers
            };

            using (var ac = new Howlett.Kafka.Extensions.AdminClient(config))
            {
                // perhaps only need full replication on one compacted log per table.
                ac.CreateTopicMaybeAsync(
                    tableSpecification.ChangeLogTopicName(columnName), 1, 1,
                    new Dictionary<string, string>
                    {
                        { "cleanup.policy", "compact" }
                    }
                ).GetAwaiter().GetResult();
                
                ac.CreateTopicMaybeAsync(
                    tableSpecification.CommandTopicName(columnName), 1, 1, null
                ).GetAwaiter().GetResult();
            }
        }

        private void InitClients(string bootstrapServers)
        {
            var pConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                LingerMs = 5
            };

            producer = new ProducerBuilder<string, string>(pConfig).Build();

            var cConfig1 = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                GroupId = tableSpecification.ChangeLogTopicName(this.columnName) + "_cg",
                EnablePartitionEof = true
            };
            changeLogConsumer = new ConsumerBuilder<string, string>(cConfig1).Build();

            var cConfig2 = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                GroupId = tableSpecification.CommandTopicName(this.columnName) + "_cg"
            };
            commandConsumer = new ConsumerBuilder<string, string>(cConfig2)
                .SetPartitionsAssignedHandler((c, ps) => {
                    return new [] { new TopicPartitionOffset(this.tableSpecification.CommandTopicName(this.columnName), this.partition, Offset.Unset) };
                })
                .Build();
        }

        public SubTable(TableSpecification tableSpecification, string bootstrapServers, string columnName, int partition, CancellationToken ct)
        {
            this.tableSpecification = tableSpecification;
            this.partition = partition;
            this.columnName = columnName;

            InitClients(bootstrapServers);
            CreateTopicsIfRequired(bootstrapServers);

            StartCommandConsumer(ct);
        }

        
        public void WaitReady()
        {
            // wait for change log eof.
        }

        private void StartCommandConsumer(CancellationToken ct)
        {
            Task.Run(() =>
            {
                commandConsumer.Subscribe(this.tableSpecification.CommandTopicName(this.columnName));

                while (!ct.IsCancellationRequested)
                {
                    var cr = commandConsumer.Consume(ct);
                    var o = JsonConvert.DeserializeObject(cr.Value);

                    Thread.Sleep(100);
                }
            });
        }

        private void StartChangeLogConsumer(CancellationToken ct)
        {
            Task.Run(() =>
            {
                changeLogConsumer.Subscribe(this.tableSpecification.ChangeLogTopicName(this.columnName));

                while (!ct.IsCancellationRequested)
                {
                    var cr = changeLogConsumer.Consume(ct);
                    if (cr.IsPartitionEOF)
                    {
                        // done.
                    }

                    // materialize.
                }
            });
        }

        public async Task AddOrUpdate(ChangeType changeType, object keyValue, Dictionary<string, object> row)
        {
            var keyName = this.columnName;
            var p = Table.Partitioner(keyValue, tableSpecification.NumPartitions);
            if (p != partition)
            {
                throw new Exception("incorrect partition");
            }

            if (row.Keys.Contains("keyName"))
            {
                throw new Exception("row update dict shouldn't include value for key being updated");
            }

            Command_Apply c = new Command_Apply
            {
                Correlation = Guid.NewGuid().ToString(),
                ChangeType = changeType,
                ColumnName = this.columnName,
                ColumnValue = keyValue,
                Data = row
            };
            
            var s = JsonConvert.SerializeObject(c, Formatting.Indented);
        
            List<Task<DeliveryResult<string, string>>> rs = new List<Task<DeliveryResult<string, string>>>();
            foreach (var ct in tableSpecification.AllCommandTopics())
            {
                Console.WriteLine($"producing to {ct}");
                rs.Add(producer.ProduceAsync(
                    ct,
                    new Message<string, string>
                    {
                        Key = JsonConvert.SerializeObject(keyValue),
                        Value = s
                    }));
            }
            await Task.WhenAll(rs);
            Console.WriteLine(rs[0].Result.TopicPartitionOffset);
            // await consumer getting all verifys for correlationId.
        }

        public void Dispose()
        {
            producer.Dispose();

            commandConsumer.Close();
            commandConsumer.Dispose();

            changeLogConsumer.Close();
            changeLogConsumer.Dispose();
        }        
    }
}