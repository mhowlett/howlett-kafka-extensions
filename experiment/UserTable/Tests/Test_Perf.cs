using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Newtonsoft.Json;


namespace Howlett.Kafka.Extensions.Experiment
{
    public class Test_Perf
    {
        public static void Assert(bool cond)
        {
            Assert(cond, "[null]");
        }

        public static void Assert(bool cond, string error)
        {
            if (!cond)
            {
                Console.WriteLine(error);
            }
        }

        public static async Task Run()
        {
            var desc = System.IO.File.ReadAllText("/git/howlett-kafka-extensions/experiment/UserTable/tablespec_user.json");
            var bootstrapServers = "127.0.0.1:9092";
            var numPartitions = 3;
            var recreate = true;

            using (var table = new Table(desc, bootstrapServers, numPartitions, recreate, false))
            {
                var tasks = new List<Task<bool>>();
                for (int i=0; i<1000; ++i)
                {
                    // 1. add an entry, where all values are on different partitions.
                    tasks.Add(table.Add("id", i.ToString(),
                        new Dictionary<string, string>
                        {
                            { "username", "jsmith" + i.ToString() },
                            { "email", "john@smith.org" + i.ToString() },
                            { "quota", "100" },
                            { "firstname", "John" },
                            { "lastname", "Smith"}
                        }));
                }

                await Task.WhenAll(tasks.ToArray());
            }
        }
    }
}