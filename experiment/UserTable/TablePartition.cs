using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;


namespace Howlett.Kafka.Extensions.Experiment
{
    public class TablePartition : IDisposable
    {
        private Dictionary<string, ColumnPartition> columns;

        private int partition;

        private TableSpecification tableSpecification;

        public TablePartition(string bootstrapServers, string tableSpec, int partition, int numPartitions, bool recreate, CancellationToken ct)
        {
            this.partition = partition;
            this.tableSpecification = new TableSpecification(tableSpec);
            
            columns = new Dictionary<string, ColumnPartition>();
            foreach (var c in tableSpecification.ColumnSpecifications)
            {
                if (c.Unique)
                {
                    var column = new ColumnPartition(tableSpecification, bootstrapServers, c.Name, partition, numPartitions, recreate, ct);
                    columns.Add(c.Name, column);
                }
            }
        }


        public async Task<bool> Add(string keyName, string keyValue, Dictionary<string, string> row)
            => await columns[keyName].AddOrUpdate(AddOrUpdate.Add, keyValue, row);


        public async Task<bool> Update(string keyName, string keyValue, Dictionary<string, string> row)
            => await columns[keyName].AddOrUpdate(AddOrUpdate.Update, keyValue, row);


        public void WaitReady()
        {
            foreach (var st in columns.Values)
            {
                st.WaitReady();
            }
        }


        public Dictionary<string, string> Get(string keyName, string keyValue)
        {
            if (!this.columns.ContainsKey(keyName))
            {
                throw new Exception($"unknown key column {keyName}");
            }

            return this.columns[keyName].Get(keyValue);
        }

        public void Dispose()
        {
            foreach (var st in columns.Values)
            {
                st.Dispose();
            }
        }
    }
}