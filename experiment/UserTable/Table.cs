using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;


namespace Howlett.Kafka.Extensions.Experiment
{
    public delegate int PartitionerDelegate(object o, int partitionCount);

    public class Table : IDisposable
    {
        public static PartitionerDelegate Partitioner
            { get => (o, partitionCount) => o.ToString().GetHashCode() % partitionCount; }

        private Dictionary<string, SubTable> subTables;

        private int partition;

        private TableSpecification tableSpecification;

        public Table(string bootstrapServers, string desc, int partition, CancellationToken ct)
        {
            this.partition = partition;
            this.tableSpecification = new TableSpecification(desc);

            subTables = new Dictionary<string, SubTable>();
            foreach (var c in tableSpecification.ColumnSpecifications)
            {
                if (c.Unique)
                {
                    var subTable = new SubTable(tableSpecification, bootstrapServers, c.Name, partition, ct);
                    subTables.Add(c.Name, subTable);
                }
            }
        }


        public async Task AddOrUpdate(
            ChangeType changeType,
            string keyName, object keyValue,
            Dictionary<string, object> row)
        {
            var subTable = subTables[keyName];
            await subTable.AddOrUpdate(changeType, keyValue, row);
        }

        public void WaitReady()
        {
            foreach (var st in subTables.Values)
            {
                st.WaitReady();
            }
        }

        public async Task<Dictionary<string, object>> Get(string keyName, object keyValue)
        {
            return null;
        }

        public void Dispose()
        {
            foreach (var st in subTables.Values)
            {
                st.Dispose();
            }
        }
    }
}