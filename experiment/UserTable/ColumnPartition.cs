using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;


namespace Howlett.Kafka.Extensions.Experiment
{
    public class ColumnPartition : IDisposable
    {
        private TimeSpan defaultTimeoutMs = TimeSpan.FromSeconds(10);

        private TableSpecification tableSpecification;
        private List<ColumnSpecification> otherUniqueColumns;
        private int partition;
        private int numPartitions;
        private string columnName;

        private IProducer<Null, string> cmdProducer;
        private IProducer<string, string> clProducer;

        private IConsumer<Null, string> commandConsumer;
        private IConsumer<string, string> changeLogConsumer;


        private void InitClients(string bootstrapServers)
        {
            var pConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                EnableIdempotence = true,
                MessageTimeoutMs = 900000, // 15 minutes. TODO: does this need to be infinite to guarantee gapless?
                LingerMs = 5,
            };

            cmdProducer = new ProducerBuilder<Null, string>(pConfig).Build();
            clProducer = new DependentProducerBuilder<string, string>(cmdProducer.Handle).Build();

            var cConfig1 = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                // EnableAutoCommit = true,
                // EnableAutoOffsetStore = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                GroupId = tableSpecification.ChangeLogTopicName(this.columnName) + "_cg",
                EnablePartitionEof = true
            };
            changeLogConsumer = new ConsumerBuilder<string, string>(cConfig1).Build();

            var cConfig2 = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                // EnableAutoCommit = true,
                // EnableAutoOffsetStore = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                GroupId = tableSpecification.CommandTopicName(this.columnName) + "_cg"
            };
            if (columnName == "username" && partition == 2)
            {
             //   cConfig2.Debug = "all";
            }
            commandConsumer = new ConsumerBuilder<Null, string>(cConfig2)
                .SetPartitionsAssignedHandler((c, ps) => {
                    return new [] { new TopicPartitionOffset(this.tableSpecification.CommandTopicName(this.columnName), this.partition, Offset.Unset) };
                })
                .Build();
        }

        public ColumnPartition(TableSpecification tableSpecification, string bootstrapServers, string columnName, int partition, int numPartitions, bool recreate, CancellationToken ct)
        {
            this.tableSpecification = tableSpecification;
            this.otherUniqueColumns = tableSpecification.ColumnSpecifications.Where(s => s.Unique && s.Name != columnName).ToList();
            this.partition = partition;
            this.numPartitions = numPartitions;
            this.columnName = columnName;

            InitClients(bootstrapServers);

            StartCommandConsumer(ct);
            StartChangeLogConsumer(ct);
        }

        
        private bool ready = false;
        private object readyMonitor = new object();

        public void WaitReady()
        {
            lock (readyMonitor)
            {
                while (!ready)
                {
                    Monitor.Wait(readyMonitor);
                }
            }
        }

        // materialized state for this column. columnValue: rowData.
        private Dictionary<string, Dictionary<string, string>> materialized = new Dictionary<string, Dictionary<string, string>>();

        // correlation: WaitingForVerify info.
        private Dictionary<string, WaitingForVerify> waitingVerify = new Dictionary<string, WaitingForVerify>();

        // correlation: WaitingForMaterialized info.
        private Dictionary<string, WaitingForAck> waitingMaterialized = new Dictionary<string, WaitingForAck>();

        // columnValue: correlation. correlation allows for de-duping.
        private Dictionary<string, string> locked = new Dictionary<string, string>();

        // Waiting requests that are being awaited.
        private Dictionary<string, WaitingForResult> waitingResult = new Dictionary<string, WaitingForResult>();


        private void HandleChange(JObject o)
        {
            var changeType = (AddOrUpdate)o.GetValue("ChangeType").Value<int>();
            var columnValue = o.GetValue("ColumnValue").Value<string>();
            var correlation = o.GetValue("Correlation").Value<string>();
            var data = (JObject)o.GetValue("Data");

            var colId = String.Format("{0,15}", $"[{this.columnName}|{this.partition}]");
            var begin = $"{colId} CHANGE  {columnValue}";
            var line = String.Format("{0,-60} .." + correlation.Substring(correlation.Length-8), begin);
            Console.WriteLine(line);

            if (waitingVerify.ContainsKey(correlation))
            {
                Console.WriteLine($"de-dupe change command {correlation}");
                return;
            }
            
            Dictionary<string, string> dataAsDict = new Dictionary<string, string>();
            foreach (var v in data.Descendants())
            {
                if (v.GetType() != typeof(JProperty)) continue;
                dataAsDict.Add(((JProperty)v).Name, ((JProperty)v).Value.ToString());
            }

            Dictionary<string, string> oldData = null;

            // keep track of the columns we're waiting for verify from.
            waitingVerify.Add(correlation, new WaitingForVerify
                { 
                    ColumnValue = columnValue,
                    OtherColumns = otherUniqueColumns.Select(a => a.Name).ToList(),
                    NewData = dataAsDict,
                    OldData = oldData,
                });

            // implicit lock command on this columns value.
            locked.Add(columnValue, correlation);

            foreach (var cs in otherUniqueColumns)
            {
                var v = data.GetValue(cs.Name).Value<string>();

                var lockCommand = new Command_Lock
                {
                    Correlation = correlation,
                    ChangeType = changeType,
                    ColumnValue = v,
                    SourceColumnName = this.columnName,
                    SourceColumnPartition = this.partition
                };

                var tp = new TopicPartition(
                    tableSpecification.CommandTopicName(cs.Name),
                    Table.Partitioner(v, numPartitions)
                );
                // producer settings ensure in order, gapless produce.
                cmdProducer.ProduceAsync(
                    tp,
                    new Message<Null, string>
                    {
                        Value = JsonConvert.SerializeObject(lockCommand, Formatting.Indented)
                    }
                ).ContinueWith(r =>
                    {
                        // if there's a problem, require a process restart.
                        if (r.IsFaulted)
                        {
                            Console.WriteLine("Fatal error");
                            System.Environment.Exit(1);
                        }
                    });
            }
        }

        private void HandleLock(JObject o)
        {
            var changeType = (AddOrUpdate)o.GetValue("ChangeType").Value<int>();
            var columnValue = o.GetValue("ColumnValue").Value<string>();
            var correlation = o.GetValue("Correlation").Value<string>();
            var sourceColumnName = o.GetValue("SourceColumnName").Value<string>();
            var SourceColumnPartition = o.GetValue("SourceColumnPartition").Value<int>();

            var colId = String.Format("{0,15}", $"[{this.columnName}|{this.partition}]");
            var begin = $"{colId} LOCK    {columnValue}";
            var line = String.Format("{0,-60} .." + correlation.Substring(correlation.Length-8), begin);
            Console.WriteLine(line);

            // TODO: check correlation.

            bool canChange = true;
            if (locked.ContainsKey(columnValue))
            {
                if (locked[columnValue] != correlation) // dedupe.
                {
                    // if key is already locked, cannot acquire lock again.
                    canChange = false;
                }
            }
            else if (materialized.ContainsKey(columnValue) && changeType == Experiment.AddOrUpdate.Add)
            {
                // if this is not an update, then can't overwrite existing value.
                canChange = false;
            }

            var verifyCommand = new Command_Verify
            {
                Correlation = correlation,
                Verified = canChange,
                SourceColumnName = this.columnName
            };

            var tp = new TopicPartition(tableSpecification.CommandTopicName(sourceColumnName), SourceColumnPartition);
            cmdProducer.ProduceAsync(
                tp,
                new Message<Null, string>
                {
                    Value = JsonConvert.SerializeObject(verifyCommand, Formatting.Indented)
                }
            ).ContinueWith(r => 
                {
                    if (r.IsFaulted)
                    {
                        Console.WriteLine("produce failed");
                        System.Environment.Exit(1);
                    }
                });

            locked.Add(columnValue, correlation);
        }

        private void HandleVerify(JObject o)
        {
            var correlation = o.GetValue("Correlation").Value<string>();
            var sourceColumnName = o.GetValue("SourceColumnName").Value<string>();
            var verified = o.GetValue("Verified").Value<bool>();

            var colId = String.Format("{0,15}", $"[{this.columnName}|{this.partition}]");
            var begin = $"{colId} VERIFY  {sourceColumnName}";
            var line = String.Format("{0,-60} .." + correlation.Substring(correlation.Length-8), begin);
            Console.WriteLine(line);

            if (!waitingVerify.ContainsKey(correlation))
            {
                // This could occur in the case of duplicate writes and can be safely ignored.
                Console.WriteLine("un-expected correlation");
                return;
            }

            if (verified == false)
            {
                waitingVerify[correlation].Verified = false;
            }
            
            var wl = waitingVerify[correlation].OtherColumns.Where(a => a != sourceColumnName).ToList();

            // Verification result received for all columns.
            if (wl.Count == 0)
            {
                var columnValue = waitingVerify[correlation].ColumnValue;

                //   1. commit new changes values to change logs.

                var dataRow = waitingVerify[correlation].NewData;

                // TODO: It's not ideal that these stay forever in the changelog topic.
                dataRow.Add("_correlation", correlation);
                dataRow.Add("_sourceColumn", this.columnName);
                dataRow.Add("_sourceValue", columnValue);

                var tasks = new List<Task<DeliveryResult<string, string>>>();

                var tp = new TopicPartition(
                        this.tableSpecification.ChangeLogTopicName(this.columnName),
                        Table.Partitioner(columnValue, this.numPartitions));
                tasks.Add(clProducer.ProduceAsync(
                    tp,
                    new Message<string, string>
                    {
                        Key = columnValue,
                        Value = JsonConvert.SerializeObject(dataRow, Formatting.Indented)
                    }
                ));

                foreach (var col in otherUniqueColumns)
                {
                    tp = new TopicPartition(
                        this.tableSpecification.ChangeLogTopicName(col.Name),
                        Table.Partitioner(dataRow[col.Name], this.numPartitions));
                    var dr = dataRow.Select(a => a).ToDictionary(a => a.Key, a => a.Value);   // copy
                    dr.Add(this.columnName, columnValue);
                    var cVal = dr[col.Name];
                    dr.Remove(col.Name);

                    tasks.Add(clProducer.ProduceAsync(
                        tp,
                        new Message<string, string>
                        {
                            Key = cVal,
                            Value = JsonConvert.SerializeObject(dr, Formatting.Indented)
                        }
                    ));
                }

                //  2. if this is an update, remove old values from change log.
                //  TODO:


                //   3. write unlock commands as required. Note: change has already been sent to changelog topic.

                Task.WhenAll(tasks).ContinueWith(r => 
                {
                    if (r.IsFaulted)
                    {
                        Console.WriteLine("fatal");
                        System.Environment.Exit(1);
                    }

                    foreach (var col in otherUniqueColumns)
                    {
                        var unlockCommand = new Command_Unlock
                        {
                            Correlation = correlation,
                            ColumnValue = dataRow[col.Name]
                        };

                        tp = new TopicPartition(
                            tableSpecification.CommandTopicName(col.Name),
                            Table.Partitioner(unlockCommand.ColumnValue, this.numPartitions));

                        cmdProducer.ProduceAsync(
                            tp,
                            new Message<Null, string>
                            {
                                Value = JsonConvert.SerializeObject(unlockCommand, Formatting.Indented)
                            }
                        ).ContinueWith(r => 
                            {
                                if (r.IsFaulted)
                                {
                                    Console.WriteLine("produce failed");
                                    System.Environment.Exit(1);
                                }
                            });
                    }

                    lock (waitingResult)
                    {
                        waitingResult[correlation].Verified = waitingVerify[correlation].Verified;
                    }

                    waitingVerify.Remove(correlation);
                });
            }
            else
            {
                waitingVerify[correlation].OtherColumns = wl;
            }
        }

        private void HandleUnlock(JObject o)
        {
            var correlation = o.GetValue("Correlation").Value<string>();
            var columnValue = o.GetValue("ColumnValue").Value<string>();

            var colId = String.Format("{0,15}", $"[{this.columnName}|{this.partition}]");
            var begin = $"{colId} UNLOCK  {columnValue}";
            var line = String.Format("{0,-60} .." + correlation.Substring(correlation.Length-8), begin);
            Console.WriteLine(line);

            if (locked.ContainsKey(columnValue))
            {
                if (locked[columnValue] != correlation)
                {
                    Console.WriteLine("correlation doesn't match, unlocking.");
                    System.Environment.Exit(1);
                }
            }

            locked.Remove(columnValue);
        }

        private void HandleAck(JObject o)
        {
            var correlation = o.GetValue("Correlation").Value<string>();
            var sourceColumnName = o.GetValue("SourceColumnName").Value<string>();

            var colId = String.Format("{0,15}", $"[{this.columnName}|{this.partition}]");
            var begin = $"{colId} ACK     {sourceColumnName}";
            var line = String.Format("{0,-60} .." + correlation.Substring(correlation.Length-8), begin);
            Console.WriteLine(line);

            Task.Run(() => 
                {
                    lock (waitingResult)
                    {
                        var wr = waitingResult[correlation];
                        waitingResult.Remove(correlation);
                        wr.TaskCompletionSource.SetResult(wr.Verified);
                    }
                });
        }

        private Thread _noCollect; 
        private void StartCommandConsumer(CancellationToken ct)
        {
            _noCollect = new Thread(() =>
            {
                try
                {
                    // var tp = new TopicPartition(this.tableSpecification.CommandTopicName(this.columnName), this.partition);
                    // commandConsumer.Assign(tp);
                    commandConsumer.Subscribe(this.tableSpecification.CommandTopicName(this.columnName));

                    while (!ct.IsCancellationRequested)
                    {
                        var cr = commandConsumer.Consume(TimeSpan.FromSeconds(1));
                        if (cr == null)
                        {
                            continue;
                        }
                        commandConsumer.Commit(cr);

                        var o = (JObject)JsonConvert.DeserializeObject(cr.Value);
                        var commandType = (CommandType)o.GetValue("CommandType").Value<int>();
                        switch(commandType)
                        {
                            case CommandType.Change:
                                HandleChange(o);
                                break;
                            case CommandType.Lock:
                                HandleLock(o);
                                break;
                            case CommandType.Verify:
                                HandleVerify(o);
                                break;
                            case CommandType.Unlock:
                                HandleUnlock(o);
                                break;
                            case CommandType.Ack:
                                HandleAck(o);
                                break;
                            default:
                                Console.WriteLine($"Unknown command type: {commandType}");
                                break;
                        }
                    }
                }
                catch (Exception e)
                {
                        Console.WriteLine("the end " + e);
                }
            });

            _noCollect.Start();
        }

        private Thread _noCollect2;
        private void StartChangeLogConsumer(CancellationToken ct)
        {
            _noCollect2 = new Thread(() =>
            {
                changeLogConsumer.Assign(new TopicPartition(this.tableSpecification.ChangeLogTopicName(this.columnName), this.partition));

                while (!ct.IsCancellationRequested)
                {
                    var cr = changeLogConsumer.Consume(TimeSpan.FromSeconds(1));
                    if (cr == null)
                    {
                        continue;
                    }

                    if (cr.IsPartitionEOF)
                    {
                        lock (readyMonitor)
                        {
                            if (!ready)
                            {
                                ready = true;
                                Monitor.PulseAll(readyMonitor);
                            }
                        }
                        continue;
                    }

                    changeLogConsumer.Commit(cr);

                    // materialize.
                    var columnValue = cr.Key;
                    var columnName = this.columnName;
                    var o = (JObject)JsonConvert.DeserializeObject(cr.Value);
                    Dictionary<string, string> dataAsDict = new Dictionary<string, string>();
                    foreach (var v in o.Descendants())
                    {
                        if (v.GetType() != typeof(JProperty)) continue;
                        dataAsDict.Add(((JProperty)v).Name, ((JProperty)v).Value.ToString());
                    }

                    var correlation = dataAsDict["_correlation"];
                    var sourceColumn = dataAsDict["_sourceColumn"];
                    var sourceValue = dataAsDict["_sourceValue"];

                    dataAsDict.Remove("_correlation");
                    dataAsDict.Remove("_sourceColumn");
                    dataAsDict.Remove("_sourceValue");

                    materialized.Add(columnValue, dataAsDict);

                    if (this.columnName != sourceColumn)
                    {
                        var ackCommand = new Command_Ack
                        {
                            Correlation = correlation,
                            SourceColumnName = columnName
                        };

                        var tp = new TopicPartition(
                            tableSpecification.CommandTopicName(sourceColumn),
                            Table.Partitioner(sourceValue, numPartitions)
                        );

                        cmdProducer.ProduceAsync(
                            tp,
                            new Message<Null, string>
                            {
                                Value = JsonConvert.SerializeObject(ackCommand, Formatting.Indented)
                            }
                        ).ContinueWith(r => 
                            {
                                if (r.IsFaulted)
                                {
                                    Console.WriteLine("produce failed");
                                    System.Environment.Exit(1);
                                }
                            });
                    }
                }
            });

            _noCollect2.Start();
        }

        public async Task<bool> WaitForResult(string correlation)
        {
            TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>();
            lock (waitingResult)
            {
                waitingResult.Add(correlation, new WaitingForResult { TaskCompletionSource = tcs });
            }
            return await tcs.Task.ConfigureAwait(false);
        }

        public async Task<bool> AddOrUpdate(AddOrUpdate changeType, string keyValue, Dictionary<string, string> row)
        {
            var keyName = this.columnName;
            var p = Table.Partitioner(keyValue, this.numPartitions);
            if (p != partition)
            {
                throw new Exception("Applying change to incorrect partition.");
            }

            if (row.Keys.Contains("keyName"))
            {
                throw new Exception("Row update dict shouldn't include value for key being updated.");
            }

            Command_Change changeCommand = new Command_Change
            {
                Correlation = Guid.NewGuid().ToString(),
                ChangeType = changeType,
                ColumnValue = keyValue,
                Data = row
            };

            var r = await cmdProducer.ProduceAsync(
                new TopicPartition(tableSpecification.CommandTopicName(this.columnName), this.partition),
                new Message<Null, string>
                {
                    Value = JsonConvert.SerializeObject(changeCommand, Formatting.Indented)
                }
            );

            return await WaitForResult(changeCommand.Correlation).ConfigureAwait(false);
        }

        public Dictionary<string, string> Get(string keyValue)
        {
            if (materialized.ContainsKey(keyValue))
            {
                return materialized[keyValue];
            }
            return null;
        }
        
        public void Dispose()
        {
            cmdProducer.Dispose();

            commandConsumer.Close();
            commandConsumer.Dispose();

            changeLogConsumer.Close();
            changeLogConsumer.Dispose();
        }        
    }
}