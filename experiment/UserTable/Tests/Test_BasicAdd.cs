using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;


namespace Howlett.Kafka.Extensions.Experiment
{
    public class Test_BasicAdd
    {
        public static async Task Run()
        {
            var desc = System.IO.File.ReadAllText("/git/howlett-kafka-extensions/experiment/UserTable/tablespec_user.json");
            var bootstrapServers = "127.0.0.1:9092";
            var numPartitions = 3;
            var recreate = true;

            using (var table = new Table(desc, bootstrapServers, numPartitions, recreate, true))
            {
                // 1. add an entry, where all values are on different partitions.

                var r = await table.Add("id", "42",
                    new Dictionary<string, string>
                    {
                        { "username", "jsmith" },
                        { "email", "john@smith.org" },
                        { "quota", "100" },
                        { "firstname", "John" },
                        { "lastname", "Smith"}
                    });

                Assert.True(table.GetMetrics().Select(a => a["locked.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.state.active.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.state.secondary.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.tasks.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["materialized.count"]).Where(a => a == "1").Count() == 3);
                Assert.True(table.GetMetrics().Select(a => a["materialized.count"]).Where(a => a == "0").Count() == 6);
                Assert.True(table.GetMetrics().Select(a => a["blocked.for.commit.count"]).Where(a => a != "0").Count() == 0);

                bool hadException = false;
                Dictionary<string, string> user = null;
                // user = table.Get("id", "42");
                // Console.WriteLine("\nget jsmith:\n" + JsonConvert.SerializeObject(user, Formatting.Indented));

                user = table.Get("id", "42");
                Assert.True(user != null);
                Assert.True(!user.ContainsKey("id"));
                Assert.True(user.ContainsKey("email"));
                Assert.True(user["email"] == "john@smith.org");
                Assert.True(user.ContainsKey("username"));
                Assert.True(user["username"] == "jsmith");
                Assert.True(user.ContainsKey("quota"));
                Assert.True(user["quota"] == "100");
                Assert.True(user.ContainsKey("firstname"));
                Assert.True(user["firstname"] == "John");
                Assert.True(user.ContainsKey("lastname"));
                Assert.True(user["lastname"] == "Smith");

                user = table.Get("username", "jsmith");
                Assert.True(user != null);
                Assert.True(!user.ContainsKey("username"));
                Assert.True(user.ContainsKey("id"));
                Assert.True(user["id"] == "42");
                Assert.True(user.ContainsKey("email"));
                Assert.True(user["email"] == "john@smith.org");
                Assert.True(user.ContainsKey("quota"));
                Assert.True(user["quota"] == "100");
                Assert.True(user.ContainsKey("firstname"));
                Assert.True(user["firstname"] == "John");
                Assert.True(user.ContainsKey("lastname"));
                Assert.True(user["lastname"] == "Smith");

                user = table.Get("email", "john@smith.org");
                Assert.True(user != null);
                Assert.True(!user.ContainsKey("email"));
                Assert.True(user.ContainsKey("id"));
                Assert.True(user["id"] == "42");
                Assert.True(user.ContainsKey("username"));
                Assert.True(user["username"] == "jsmith");
                Assert.True(user.ContainsKey("quota"));
                Assert.True(user["quota"] == "100");
                Assert.True(user.ContainsKey("firstname"));
                Assert.True(user["firstname"] == "John");
                Assert.True(user.ContainsKey("lastname"));
                Assert.True(user["lastname"] == "Smith");

                // 2. add another entry, where all values are on different partitions.
                r = await table.Add("id", "43",
                    new Dictionary<string, string>
                    {
                        { "username", "jsmith2" },
                        { "email", "john@smith.net" },
                        { "quota", "101" },
                        { "firstname", "John2" },
                        { "lastname", "Smith2"}
                    });

                Assert.True(table.GetMetrics().Select(a => a["locked.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.state.active.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.state.secondary.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.tasks.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["materialized.count"]).Where(a => a != "0").Select(a => int.Parse(a)).Sum() == 6);
                Assert.True(table.GetMetrics().Select(a => a["blocked.for.commit.count"]).Where(a => a != "0").Count() == 0);

                user = table.Get("id", "43");
                Assert.True(user != null);
                Assert.True(!user.ContainsKey("id"));
                Assert.True(user.ContainsKey("email"));
                Assert.True(user["email"] == "john@smith.net");
                Assert.True(user.ContainsKey("username"));
                Assert.True(user["username"] == "jsmith2");
                Assert.True(user.ContainsKey("quota"));
                Assert.True(user["quota"] == "101");
                Assert.True(user.ContainsKey("firstname"));
                Assert.True(user["firstname"] == "John2");
                Assert.True(user.ContainsKey("lastname"));
                Assert.True(user["lastname"] == "Smith2");

                user = table.Get("username", "jsmith2");
                Assert.True(user != null);
                Assert.True(!user.ContainsKey("username"));
                Assert.True(user.ContainsKey("id"));
                Assert.True(user["id"] == "43");
                Assert.True(user.ContainsKey("email"));
                Assert.True(user["email"] == "john@smith.net");
                Assert.True(user.ContainsKey("quota"));
                Assert.True(user["quota"] == "101");
                Assert.True(user.ContainsKey("firstname"));
                Assert.True(user["firstname"] == "John2");
                Assert.True(user.ContainsKey("lastname"));
                Assert.True(user["lastname"] == "Smith2");

                user = table.Get("email", "john@smith.net");
                Assert.True(user != null);
                Assert.True(!user.ContainsKey("email"));
                Assert.True(user.ContainsKey("id"));
                Assert.True(user["id"] == "43");
                Assert.True(user.ContainsKey("username"));
                Assert.True(user["username"] == "jsmith2");
                Assert.True(user.ContainsKey("quota"));
                Assert.True(user["quota"] == "101");
                Assert.True(user.ContainsKey("firstname"));
                Assert.True(user["firstname"] == "John2");
                Assert.True(user.ContainsKey("lastname"));
                Assert.True(user["lastname"] == "Smith2");

                // 3. attempt to add where active key unique constraint violated.
                hadException = false;
                try
                {
                    r = await table.Add("id", "43",
                        new Dictionary<string, string>
                        {
                            { "username", "test" },
                            { "email", "test@user.net" },
                        });
                }
                catch (Exception)
                {
                    hadException = true;
                }
                Assert.True(hadException);                
                Assert.True(table.GetMetrics().Select(a => a["locked.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.state.active.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.state.secondary.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.tasks.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["materialized.count"]).Where(a => a != "0").Select(a => int.Parse(a)).Sum() == 6);
                Assert.True(table.GetMetrics().Select(a => a["blocked.for.commit.count"]).Where(a => a != "0").Count() == 0);


                // 4. attempt to add where non-active key unique constraint violated.
                hadException = false;
                try
                {
                    r = await table.Add("id", "47",
                        new Dictionary<string, string>
                        {
                            { "username", "jsmith2" },
                            { "email", "test2@user.net" },
                        });
                }
                catch (Exception)
                {
                    hadException = true;
                }
                Assert.True(hadException);
                Assert.True(table.GetMetrics().Select(a => a["locked.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.state.active.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.state.secondary.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.tasks.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["materialized.count"]).Where(a => a != "0").Select(a => int.Parse(a)).Sum() == 6);
                Assert.True(table.GetMetrics().Select(a => a["blocked.for.commit.count"]).Where(a => a != "0").Count() == 0);


                // 5. attempt to add where non-active key unique constraint violated (2)
                hadException = false;
                try
                {
                    r = await table.Add("id", "47",
                        new Dictionary<string, string>
                        {
                            { "username", "testagain" },
                            { "email", "john@smith.net" },
                        });
                }
                catch (Exception)
                {
                    hadException = true;
                }
                Assert.True(hadException);
                Assert.True(table.GetMetrics().Select(a => a["locked.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.state.active.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.state.secondary.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.tasks.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["materialized.count"]).Where(a => a != "0").Select(a => int.Parse(a)).Sum() == 6);
                Assert.True(table.GetMetrics().Select(a => a["blocked.for.commit.count"]).Where(a => a != "0").Count() == 0);


                // 6. attempt to add where non-active key unique constraint violated (3)
                hadException = false;
                try
                {
                    r = await table.Add("username", "jsmith2",
                        new Dictionary<string, string>
                        {
                            { "id", "12" },
                            { "email", "test@smith.net" },
                        });
                }
                catch (Exception)
                {
                    hadException = true;
                }
                Assert.True(hadException);
                Assert.True(table.GetMetrics().Select(a => a["locked.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.state.active.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.state.secondary.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.tasks.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["materialized.count"]).Where(a => a != "0").Select(a => int.Parse(a)).Sum() == 6);
                Assert.True(table.GetMetrics().Select(a => a["blocked.for.commit.count"]).Where(a => a != "0").Count() == 0);


                // 7. attempt to add where non-active key unique constraint violated (4)
                hadException = false;
                try
                {
                    r = await table.Add("email", "john@smith.net",
                        new Dictionary<string, string>
                        {
                            { "id", "12" },
                            { "username", "anothername" },
                        });
                }
                catch (Exception)
                {
                    hadException = true;
                }
                Assert.True(hadException);
                Assert.True(table.GetMetrics().Select(a => a["locked.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.state.active.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.state.secondary.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["in.progress.tasks.count"]).Where(a => a != "0").Count() == 0);
                Assert.True(table.GetMetrics().Select(a => a["materialized.count"]).Where(a => a != "0").Select(a => int.Parse(a)).Sum() == 6);
                Assert.True(table.GetMetrics().Select(a => a["blocked.for.commit.count"]).Where(a => a != "0").Count() == 0);
            }
        }
    }
}