//    Copyright 2019 Howlett.Kafka.Extensions Contributors
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Howlett.Kafka.Extensions;


namespace AdminClientExample
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = "127.0.0.1:9092"
            };
            
            string topic = "test_topic";

            using (var adminClient = new AdminClient(config))
            {
                // won't throw if topic doesn't exist.
                await adminClient.DeleteTopicMaybeAsync("topic_that_doesnt_exist");

                // only create topic if it doesn't exist already.
                await adminClient.CreateTopicMaybeAsync(topic, 1, 1, null);

                // topic may take some time to be created.
                await Task.Delay(1000);

                // a simplified describe topics method.
                var configs = await adminClient.DescribeTopicAsync(topic);
                foreach (var c in configs)
                {
                    Console.WriteLine($"{c.Key}: {c.Value.Value}");
                }

                // delete the topic (if it doesn't exist, considered an error).
                await adminClient.DeleteTopicAsync(topic);
            }
        }
    }
}
