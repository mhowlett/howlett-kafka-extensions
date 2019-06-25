using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Howlett.Kafka.Extensions.Experiment
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // all tables make topics if not exist.            
            var desc = System.IO.File.ReadAllText("/git/howlett-kafka-extensions/experiment/UserTable/tablespec.json");
            var bootstrapServers = "127.0.0.1:9092";

            CancellationTokenSource cts = new CancellationTokenSource();

            var uTables = new []
            {
                new Table(bootstrapServers, desc, 0, cts.Token),
                new Table(bootstrapServers, desc, 1, cts.Token)
            }.ToList();

            uTables.ForEach(ut => ut.WaitReady());

            await uTables[Table.Partitioner("mhowlett", 2)].AddOrUpdate(
                ChangeType.Update,
                "username", "mhowlett",
                new Dictionary<string, object>
                {
                    { "email", "matt@somedomain.com" },
                    { "quota", 100 }
                });

            var user = await uTables[Table.Partitioner("mhowlett", 2)].Get("username", "mhowlett");

            Thread.Sleep(10000);
            cts.Cancel();
            uTables.ForEach(ut => ut.Dispose());
        }
    }
}
