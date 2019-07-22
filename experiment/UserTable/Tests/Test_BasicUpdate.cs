using System;
using System.Collections.Generic;
using System.Threading.Tasks;


namespace Howlett.Kafka.Extensions.Experiment
{
    public class Test_BasicUpdate
    {
        public static async Task Run()
        {
            var desc = System.IO.File.ReadAllText("/git/howlett-kafka-extensions/experiment/UserTable/tablespec_user.json");
            var bootstrapServers = "127.0.0.1:9092";
            var numPartitions = 3;
            var recreate = true;

            using (var table = new Table(desc, bootstrapServers, numPartitions, recreate, true))
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

                Dictionary<string, string> user = null;

                // test updating a non-unique column works.
                r = await table.Update("id", "42",
                    new Dictionary<string, string>
                    {
                        { "lastname", "Smith2"}
                    });

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
                Assert.True(user["lastname"] == "Smith2");

                // test updating more than one non-unique column works.
                r = await table.Update("id", "42",
                    new Dictionary<string, string>
                    {
                        { "firstname", "John2" },
                        { "quota", "102" }
                    });

                user = table.Get("id", "42");
                Assert.True(user != null);
                Assert.True(!user.ContainsKey("id"));
                Assert.True(user.ContainsKey("email"));
                Assert.True(user["email"] == "john@smith.org");
                Assert.True(user.ContainsKey("username"));
                Assert.True(user["username"] == "jsmith");
                Assert.True(user.ContainsKey("quota"));
                Assert.True(user["quota"] == "102");
                Assert.True(user.ContainsKey("firstname"));
                Assert.True(user["firstname"] == "John2");
                Assert.True(user.ContainsKey("lastname"));
                Assert.True(user["lastname"] == "Smith2");

                // test updating a unique column works.
                r = await table.Update("id", "42",
                    new Dictionary<string, string>
                    {
                        { "username", "jsmith2"}
                    });

                user = table.Get("id", "42");
                Assert.True(user != null);
                Assert.True(!user.ContainsKey("id"));
                Assert.True(user.ContainsKey("email"));
                Assert.True(user["email"] == "john@smith.org");
                Assert.True(user.ContainsKey("username"));
                Assert.True(user["username"] == "jsmith2");
                Assert.True(user.ContainsKey("quota"));
                Assert.True(user["quota"] == "102");
                Assert.True(user.ContainsKey("firstname"));
                Assert.True(user["firstname"] == "John2");
                Assert.True(user.ContainsKey("lastname"));
                Assert.True(user["lastname"] == "Smith2");

                user = table.Get("username", "jsmith2");
                Assert.True(user != null);
                Assert.True(!user.ContainsKey("username"));
                Assert.True(user.ContainsKey("email"));
                Assert.True(user["email"] == "john@smith.org");
                Assert.True(user.ContainsKey("id"));
                Assert.True(user["id"] == "42");
                Assert.True(user.ContainsKey("quota"));
                Assert.True(user["quota"] == "102");
                Assert.True(user.ContainsKey("firstname"));
                Assert.True(user["firstname"] == "John2");
                Assert.True(user.ContainsKey("lastname"));
                Assert.True(user["lastname"] == "Smith2");

                user = table.Get("email", "john@smith.org");
                Assert.True(user != null);
                Assert.True(!user.ContainsKey("email"));
                Assert.True(user.ContainsKey("username"));
                Assert.True(user["username"] == "jsmith2");
                Assert.True(user.ContainsKey("id"));
                Assert.True(user["id"] == "42");
                Assert.True(user.ContainsKey("quota"));
                Assert.True(user["quota"] == "102");
                Assert.True(user.ContainsKey("firstname"));
                Assert.True(user["firstname"] == "John2");
                Assert.True(user.ContainsKey("lastname"));
                Assert.True(user["lastname"] == "Smith2");

                user = table.Get("username", "jsmith");
                Assert.True(user == null); // old key shouldn't exist.

                // test updating more than one unique column works.
                r = await table.Update("username", "jsmith2",
                    new Dictionary<string, string>
                    {
                        { "id", "50" },
                        { "email", "j@smith.com" },
                        { "quota", "150" }
                    });

                user = table.Get("email", "j@smith.com");
                Assert.True(user != null);
                Assert.True(!user.ContainsKey("email"));
                Assert.True(user.ContainsKey("username"));
                Assert.True(user["username"] == "jsmith2");
                Assert.True(user.ContainsKey("id"));
                Assert.True(user["id"] == "50");
                Assert.True(user.ContainsKey("quota"));
                Assert.True(user["quota"] == "150");
                Assert.True(user.ContainsKey("firstname"));
                Assert.True(user["firstname"] == "John2");
                Assert.True(user.ContainsKey("lastname"));
                Assert.True(user["lastname"] == "Smith2");

                user = table.Get("id", "42");
                Assert.True(user == null);
                user = table.Get("email", "john@smith.org");
                Assert.True(user == null);
            }
        }
    }
}