using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;


namespace Howlett.Kafka.Extensions.Experiment
{
    public class TablePartition : IDisposable
    {
        private Dictionary<string, ColumnPartition> columns;

        private int partition;

        private TableSpecification tableSpecification;

        public TablePartition(
            string bootstrapServers,
            string tableSpec,
            int partition,
            int numPartitions,
            bool recreate,
            bool logCommands,
            CancellationToken ct)
        {
            this.partition = partition;
            this.tableSpecification = new TableSpecification(tableSpec);
            
            columns = new Dictionary<string, ColumnPartition>();
            foreach (var c in tableSpecification.ColumnSpecifications)
            {
                if (c.Unique)
                {
                    var column = new ColumnPartition(tableSpecification, bootstrapServers, c.Name, partition, numPartitions, recreate, logCommands, ct);
                    columns.Add(c.Name, column);
                }
            }
        }

        public Task<bool> Add(string keyName, string keyValue, Dictionary<string, string> row)
            => columns[keyName].Change(Experiment.ChangeType.Add, keyValue, row);

        public Task<bool> Update(string keyName, string keyValue, Dictionary<string, string> row)
            => columns[keyName].Change(Experiment.ChangeType.Update, keyValue, row);

        public Task<bool> AddOrUpdate(string keyName, string keyValue, Dictionary<string, string> row)
            => columns[keyName].Change(Experiment.ChangeType.AddOrUpdate, keyValue, row);
            
        public Task<bool> Delete(string keyName, string keyValue)
            => columns[keyName].Change(Experiment.ChangeType.Delete, keyValue, null);

        public Dictionary<string, string> Get(string keyName, string keyValue)
        {
            if (!this.columns.ContainsKey(keyName))
            {
                throw new Exception($"unknown key column {keyName}");
            }

            return this.columns[keyName].Get(keyValue);
        }

        public List<Dictionary<string, string>> GetMetrics()
            => columns.Values.ToList().Select(a => a.GetMetrics()).ToList();

        public void WaitReady()
        {
            columns.Values.ToList().ForEach(c => c.WaitReady());
        }

        public void Dispose()
        {
            columns.Values.ToList().ForEach(c => c.Dispose());
        }
    }
}