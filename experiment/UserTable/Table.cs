using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;


namespace Howlett.Kafka.Extensions.Experiment
{
    public delegate int PartitionerDelegate(string val, int partitionCount);

    /// <summary>
    ///     A helper class to facilitate running multiple table partitions.
    /// 
    ///     TODO: Some abstractions to help use in a distributed setting.
    /// </summary>
    public class Table : IDisposable
    {
        int numPartitions;
        CancellationTokenSource cts;
        List<TablePartition> tablePartitions;

        public Table(string desc, string bootstrapServers, int numPartitions, bool recreate, bool logCommands)
        {
            this.numPartitions = numPartitions;
            CreateTopicsIfRequired(bootstrapServers, desc, numPartitions, recreate);
            cts = new CancellationTokenSource();

            tablePartitions = new List<TablePartition>();
            for (int i=0; i<numPartitions; ++i)
            {
                tablePartitions.Add(new TablePartition(bootstrapServers, desc, i, numPartitions, recreate, logCommands, cts.Token));
            };

            Console.WriteLine("waiting for all table partitions to be ready");
            tablePartitions.ForEach(ut => ut.WaitReady());
            Console.WriteLine("...ready\n");
        }

        public Task<bool> Add(string keyName, string keyValue, Dictionary<string, string> row)
            => tablePartitions[Table.Partitioner(keyValue, numPartitions)].Add(keyName, keyValue, row);

        public Task<bool> Update(string keyName, string keyValue, Dictionary<string, string> row)
            => tablePartitions[Table.Partitioner(keyValue, numPartitions)].Update(keyName, keyValue, row);

        public Task<bool> AddOrUpdate(string keyName, string keyValue, Dictionary<string, string> row)
            => tablePartitions[Table.Partitioner(keyValue, numPartitions)].AddOrUpdate(keyName, keyValue, row);

        public Task<bool> Delete(string keyName, string keyValue)
            => tablePartitions[Table.Partitioner(keyValue, numPartitions)].Delete(keyName, keyValue);

        public Dictionary<string, string> Get(string keyName, string keyValue)
            => tablePartitions[Table.Partitioner(keyValue, numPartitions)].Get(keyName, keyValue);

        public List<Dictionary<string, string>> GetMetrics()
            => tablePartitions.Select(tp => tp.GetMetrics()).SelectMany(a => a).ToList();

        public static PartitionerDelegate Partitioner
            => (val, partitionCount) =>
                {
                    var bs = Encoding.UTF8.GetBytes(val);
                    var h = Crc32Provider.ComputeHash(bs, 0, bs.Length);
                    return (int)(BitConverter.ToUInt32(h) % partitionCount);
                };

        public static void CreateTopicsIfRequired(string bootstrapServers, string tableSpecification, int numPartitions, bool recreate)
        {
            var tableSpec = new TableSpecification(tableSpecification);

            var config = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers
            };

            using (var ac = new Howlett.Kafka.Extensions.AdminClient(config))
            {
                foreach (var cs in tableSpec.ColumnSpecifications.Where(a => a.Unique))
                {
                    if (recreate)
                    {
                        Console.WriteLine($"deleting topics for column {cs.Name}");
                        ac.DeleteTopicMaybeAsync(tableSpec.ChangeLogTopicName(cs.Name)).GetAwaiter().GetResult();
                        ac.DeleteTopicMaybeAsync(tableSpec.CommandTopicName(cs.Name)).GetAwaiter().GetResult();
                        Thread.Sleep(1000);
                    }

                    ac.CreateTopicMaybeAsync(
                        tableSpec.ChangeLogTopicName(cs.Name), numPartitions, 1,
                        new Dictionary<string, string>
                        {
                            { "cleanup.policy", "compact" }
                        }
                    ).GetAwaiter().GetResult();
                    
                    ac.CreateTopicMaybeAsync(
                        tableSpec.CommandTopicName(cs.Name), numPartitions, 1, null
                    ).GetAwaiter().GetResult();
                }
            }
        }

        public void Dispose()
        {
            cts.Cancel();
            tablePartitions.ForEach(ut => ut.Dispose());
        }
    }
}