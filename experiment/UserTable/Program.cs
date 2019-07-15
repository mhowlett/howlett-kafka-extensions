using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Howlett.Kafka.Extensions.Experiment
{
    class Program
    {
        static async Task Main(string[] args)
        {         
            var desc = System.IO.File.ReadAllText("/git/howlett-kafka-extensions/experiment/UserTable/tablespec.json");
            var bootstrapServers = "127.0.0.1:9092";
            var numPartitions = 3;
            var recreate = true;

            using (var table = new Table(desc, bootstrapServers, numPartitions, recreate))
            {
                var r = await table.Add("id", "42",
                    new Dictionary<string, string>
                    {
                        { "username", "jsmith" },
                        { "email", "john@smith.org" },
                        { "quota", "100" },
                        { "firstname", "John" },
                        { "lastname", "Smith"}
                    });
        
                // r = await table.Add("id", "43",
                //     new Dictionary<string, string>
                //     {
                //         { "username", "auser" },
                //         { "email", "auser@gmail.com" },
                //         { "quota", "200" },
                //         { "firstname", "Anthony" },
                //         { "lastname", "User"}
                //     });

                var user = table.Get("username", "jsmith");
                Console.WriteLine("\nget jsmith:\n" + JsonConvert.SerializeObject(user, Formatting.Indented));

                // user = table.Get("username", "auser");
                // Console.WriteLine("\nget auser:\n" + JsonConvert.SerializeObject(user, Formatting.Indented));
            }
        }
    }
}
