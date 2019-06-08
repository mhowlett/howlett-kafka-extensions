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
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;


namespace Howlett.Kafka.Extensions.Streaming
{
    public class Processor<TInKey, TInValue, TOutKey, TOutValue>
    {
        public string Name { get; set; }

        public string BootstrapServers { get; set; }

        public string InputTopic { get; set; }

        public string OutputTopic { get; set; }

        public IDeserializer<TInKey> InKeyDeserializer { get; set; }

        public IDeserializer<TInValue> InValueDeserializer { get; set; }

        public ISerializer<TOutKey> OutKeySerializer { get; set; }

        public ISerializer<TOutValue> OutValueSerializer { get; set; }

        public Action<LogMessage> Logger { get; set; }

        public Func<Message<TInKey, TInValue>, Message<TOutKey, TOutValue>> Function { get; set; }

        public ErrorTolerance ConsumeErrorTolerance { get; set; } = ErrorTolerance.None;

        public string DebugContext { get; set; } = null;


        private bool aMessageHasBeenProcessed = false;

        IConsumer<TInKey, TInValue> constructConsumer(string instanceId, CancellationTokenSource errorCts)
        {
            var cConfig = new ConsumerConfig
            {
                ClientId = $"{Name}-consumer-{instanceId}",
                GroupId = $"{Name}-group",
                BootstrapServers = BootstrapServers,
                EnableAutoOffsetStore = false,
                AutoOffsetReset = AutoOffsetReset.Latest,
                Debug = DebugContext
            };

            var cBuilder = new ConsumerBuilder<TInKey, TInValue>(cConfig)
                .SetKeyDeserializer(InKeyDeserializer)
                .SetValueDeserializer(InValueDeserializer)
                .SetLogHandler((_, m) => Logger(m))
                .SetErrorHandler((c, e) =>
                {
                    if (e.Code == ErrorCode.Local_AllBrokersDown ||
                        e.Code == ErrorCode.Local_Authentication)
                    {
                        if (!aMessageHasBeenProcessed)
                        {
                            errorCts.Cancel();
                            return;
                        }
                    }

                    if (Logger != null)
                    {
                        Logger(new LogMessage(c.Name, SyslogLevel.Error, "unknown", e.Reason));
                    }
                });

            return cBuilder.Build();
        }

        IProducer<TOutKey, TOutValue> constructProducer(string instanceId, CancellationTokenSource errorCts)
        {
            var pConfig = new ProducerConfig
            {
                ClientId = $"{Name}-producer-{instanceId}",
                BootstrapServers = BootstrapServers,
                EnableIdempotence = true,
                LingerMs = 5,
                DeliveryReportFields = "none",
                Debug = DebugContext
            };

            var pBuilder = new ProducerBuilder<TOutKey, TOutValue>(pConfig)
                .SetKeySerializer(OutKeySerializer)
                .SetValueSerializer(OutValueSerializer)
                .SetLogHandler((_, m) =>
                {
                    Logger(m);
                })
                .SetErrorHandler((p, e) =>
                {
                    if (e.IsFatal)
                    {
                        errorCts.Cancel();
                    }

                    if (e.Code == ErrorCode.Local_AllBrokersDown ||
                        e.Code == ErrorCode.Local_Authentication)
                    {
                        if (!aMessageHasBeenProcessed)
                        {
                            errorCts.Cancel();
                        }
                    }

                    if (Logger != null)
                    {
                        Logger(new LogMessage(p.Name, SyslogLevel.Error, "unknown", e.Reason));
                    }
                });

            return pBuilder.Build();
        }

        public void Start(string instanceId, CancellationToken cancellationToken)
        {
            CancellationTokenSource errorCts = new CancellationTokenSource();
            CancellationTokenSource compositeCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, errorCts.Token);

            IConsumer<TInKey, TInValue> consumer = null;
            IProducer<TOutKey, TOutValue> producer = null;

            try
            {
                if (InputTopic != null)
                {
                    consumer = constructConsumer(instanceId, errorCts);
                    consumer.Subscribe(InputTopic);
                }

                if (OutputTopic != null)
                {
                    producer = constructProducer(instanceId, errorCts);
                }

                while (!compositeCts.IsCancellationRequested)
                {
                    ConsumeResult<TInKey, TInValue> cr = null;

                    if (InputTopic != null)
                    {
                        try
                        {
                            cr = consumer.Consume(compositeCts.Token);
                        }
                        catch (ConsumeException ex)
                        {
                            if (ex.Error.Code == ErrorCode.Local_ValueDeserialization ||
                                ex.Error.Code == ErrorCode.Local_KeyDeserialization)
                            {
                                if (ConsumeErrorTolerance == ErrorTolerance.All)
                                {
                                    continue;
                                }

                                break; 
                            }

                            break;
                        }
                    }

                    var result = Function(cr == null ? null : cr.Message);

                    if (result != null)
                    {
                        while (true)
                        {
                            try
                            {
                                producer.Produce(OutputTopic, result,
                                    d =>
                                    {
                                        if (d.Error.Code != ErrorCode.NoError)
                                        {
                                            errorCts.Cancel();
                                        }
                                        if (cr != null)
                                        {
                                            consumer.StoreOffset(cr);
                                        }
                                    });
                                break;
                            }
                            catch (KafkaException e)
                            {
                                if (e.Error.Code == ErrorCode.Local_QueueFull)
                                {
                                    producer.Poll(TimeSpan.FromSeconds(1));
                                }
                            }
                        }
                    }
                    else
                    {
                        consumer.StoreOffset(cr);
                    }

                    aMessageHasBeenProcessed = true;
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
                if (producer != null)
                {
                    producer.Dispose();
                }
                if (consumer != null)
                {
                    consumer.Dispose();
                }
            }

            if (errorCts.IsCancellationRequested)
            {
                throw new Exception("error occured.");
            }
        }
    }
}
