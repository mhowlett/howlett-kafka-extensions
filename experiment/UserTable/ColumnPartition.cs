using System.Reactive;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;


namespace Howlett.Kafka.Extensions.Experiment
{
    public static class AAA 
    {
        public static void FailIfFaulted<K,V>(this Task<DeliveryResult<K,V>> task, string facility, string message)
        {
            task.ContinueWith(
            r => 
            {
                if (r.IsFaulted)
                {
                    Logger.Log(facility, message);
                    System.Environment.Exit(1);
                }
            });
        }
    }

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
        private Dictionary<string, InProgressState> inProgressState = new Dictionary<string, InProgressState>();

        // columnValue: correlation. correlation allows for de-duping.
        private Dictionary<string, string> locked = new Dictionary<string, string>();

        // Waiting requests that are being awaited.
        private Dictionary<string, InProgressTask> inProgressTasks = new Dictionary<string, InProgressTask>();

        private SortedList<long, string> blockedForCommit = new SortedList<long, string>();

        /// <summary>
        ///     Complete the method call.
        /// </summary>
        private void CompleteChangeRequest(string correlation, Exception exception)
        {
            Task.Run(() => 
            {
                lock (inProgressTasks)
                {
                    var wr = inProgressTasks[correlation];
                    if (wr != null)
                    {
                        inProgressTasks.Remove(correlation);
                        if (exception == null)
                        {
                            wr.TaskCompletionSource.SetResult(true);
                        }
                        else
                        {
                            wr.TaskCompletionSource.SetException(exception);
                        }
                    }
                    else
                    {
                        Logger.Log("COMPLETE", $"No task to complete for command [{correlation}]");
                    }
                }
            });
        }


        /// <summary>
        ///     Handle a brand new add or update command.
        /// </summary>
        private void HandleChange(Command_Change cmd, Offset offset)
        {
            if (inProgressState.ContainsKey(cmd.Correlation))
            {
                // if the command_change message is received more than once, this is a duplicate
                // message in the log and can simply be ignored.
                Logger.Log("CHANGE", $"ignoring duplicate change command [{cmd.Correlation}]");
                return;
            }

            // if the column value is locked, then fail the command immediately.
            if (locked.ContainsKey(cmd.ColumnValue))
            {
                CompleteChangeRequest(cmd.Correlation, new Exception($"key {cmd.ColumnValue} locked, can't change [{cmd.Correlation}]"));
                return;
            }

            // also abort if this is an add operation and the value exists already.
            if (cmd.ChangeType == Howlett.Kafka.Extensions.Experiment.AddOrUpdate.Add &&
                this.materialized.ContainsKey(cmd.ColumnValue))
            {
                CompleteChangeRequest(cmd.Correlation,new Exception($"key {cmd.ColumnValue} exists, can't add [{cmd.Correlation}]"));
                return;
            }

            // also abort if this is an update operation and the value doesn't exist already.
            if (cmd.ChangeType == Howlett.Kafka.Extensions.Experiment.AddOrUpdate.Update &&
                !this.materialized.ContainsKey(cmd.ColumnValue))
            {
                CompleteChangeRequest(cmd.Correlation, new Exception($"key {cmd.ColumnValue} doesn't exists, can't update [{cmd.Correlation}]"));
                return;
            }

            // AddOrUpdate can be dis-ambiguated at this point, and is not considered further
            // in the workflow.
            var isAddCommand = !materialized.ContainsKey(cmd.ColumnValue) || cmd.ChangeType == Experiment.AddOrUpdate.Add;

            var amalgamatedData = new Dictionary<string, string>(cmd.Data);
            var uniqueColumnValuesToDelete = new Dictionary<string, string>();

            // if this is an update:
            if (!isAddCommand)
            {
                // 1. amalgamate with existing data.
                var existing = materialized[cmd.ColumnValue];
                foreach (var e in existing)
                {
                    if (!amalgamatedData.ContainsKey(e.Key))
                    {
                        amalgamatedData.Add(e.Key, e.Value);
                    }
                }

                // 2. work out unique values (other than this) that have changed -
                //    we need the old values to be removed.
                // 
                //    notes:
                //     1. the current partition column value can't have changed (obviously)
                //     2. even if a unique column value hasn't changed, it
                //        needs to get locked (be included in the operation workflow)
                //        because it's data is changing.
                foreach (var other in this.otherUniqueColumns)
                {
                    if (cmd.Data.ContainsKey(other.Name))
                    {
                        if (cmd.Data[other.Name] != materialized[cmd.ColumnValue][other.Name])
                        {
                            uniqueColumnValuesToDelete.Add(other.Name, cmd.Data[other.Name]);
                        }
                    }
                }
            }
        
            // check that all unique columns have a value.
            foreach (var other in this.otherUniqueColumns)
            {
                if (!amalgamatedData.ContainsKey(other.Name))
                {
                    CompleteChangeRequest(cmd.Correlation, new Exception("change request does not contain value for key {other.Name} [{cmd.Correlation}]"));
                    return;
                }
            }

            // lock this column's value - at this point, we're going to try and apply the change.
            locked.Add(cmd.ColumnValue, cmd.Correlation);

            // prevent a commit of this offset until the final write in the workflow is done.
            blockedForCommit.Add(offset, cmd.Correlation);

            // keep track of info related to this command including the columns we'll be waiting for a verify from.
            var otherList = otherUniqueColumns
                .Select(a => new KeyValuePair<string, string>(a.Name, amalgamatedData[a.Name]))
                .Concat(uniqueColumnValuesToDelete.ToList())
                .Select(a => new NameAndValue { Name = a.Key, Value = a.Value })
                .ToList();
            inProgressState.Add(cmd.Correlation, new InProgressState
                { 
                    ChangeCommandOffset = offset,
                    ColumnValue = cmd.ColumnValue,
                    WaitingVerify = otherList,
                    WaitingAck = otherList,
                    Data = amalgamatedData,
                    ToDelete = uniqueColumnValuesToDelete,
                    ToSet = otherUniqueColumns.Select(a => new KeyValuePair<string, string>(a.Name, amalgamatedData[a.Name])).ToDictionary(a => a.Key, a => a.Value),
                    Verified = true // results from other column partitions get &'d together.
                });

            // finally, send an enter command to the relevant key/values
            // that need locking.
            foreach (var cs in inProgressState[cmd.Correlation].WaitingVerify)
            {
                var enterCommand = new Command_Enter
                {
                    Correlation = cmd.Correlation,
                    IsAddCommand = isAddCommand,
                    ColumnValue = cs.Value,
                    SourceColumnName = this.columnName,
                    SourceColumnValue = cmd.ColumnValue
                };

                var tp = new TopicPartition(
                    tableSpecification.CommandTopicName(cs.Name),
                    Table.Partitioner(cs.Value, numPartitions)
                );

                // TODO: verify producer settings ensure in order, gapless produce.
                //       with a long retry.
                cmdProducer.ProduceAsync(tp, new Message<Null, string> { Value = JsonConvert.SerializeObject(enterCommand, Formatting.Indented) })
                    .FailIfFaulted("CHANGE", $"A fatal problem occured writing a lock command.");
            }
        }

        private void HandleEnter(Command_Enter cmd, Offset offset)
        {
            bool canChange = true;

            // deal with case where value is locked already.
            if (locked.ContainsKey(cmd.ColumnValue))
            {
                if (locked[cmd.ColumnValue] != cmd.Correlation) // dedupe.
                {
                    Logger.Log("ENTER", $"Duplicate lock command received, ignoring [{cmd.Correlation}]");
                    return;
                }
                else
                {
                    Logger.Log("ENTER", $"Attempting to lock key that is already locked: blocking the change operation [{cmd.Correlation}]");
                    canChange = false;
                }
            }

            // deal with case where value is already materialized.
            if (materialized.ContainsKey(cmd.ColumnValue))
            {
                if(cmd.IsAddCommand)
                {
                    Logger.Log("ENTER", $"Attempting to add a new row with unique column '{this.columnName}' value '{cmd.ColumnValue}' that already exists. [{cmd.Correlation}]");
                    canChange = false;
                }

                if (!cmd.IsAddCommand)
                {
                    // in the case of an update, it is fine for the key to exist, if the row corresponds to the one being updated.
                    if (materialized[cmd.ColumnValue][cmd.SourceColumnName] != cmd.SourceColumnValue)
                    {
                        Logger.Log("ENTER", $"Attempting to update a row with unique column '{this.columnName}' value '{cmd.ColumnValue}' that already exists for some other row. [{cmd.Correlation}]");
                        canChange = false;
                    }
                }
            }

            var verifyCommand = new Command_Verify
            {
                Correlation = cmd.Correlation,
                Verified = canChange,
                SourceColumnName = this.columnName,
                SourceColumnValue = cmd.ColumnValue
            };

            var tp = new TopicPartition(
                tableSpecification.CommandTopicName(cmd.SourceColumnName),
                Table.Partitioner(cmd.SourceColumnValue, numPartitions));

            cmdProducer.ProduceAsync(tp, new Message<Null, string> { Value = JsonConvert.SerializeObject(verifyCommand, Formatting.Indented) })
                .FailIfFaulted("ENTER", $"A fatal problem occured writing a verify command.");

            locked.Add(cmd.ColumnValue, cmd.Correlation);

            // don't allow commit until exit.
            blockedForCommit.Add(offset, cmd.Correlation);
        }


        private void HandleVerify(Command_Verify cmd, Offset offset)
        {
            if (!inProgressState.ContainsKey(cmd.Correlation))
            {
                // This could occur in the case of duplicate writes and can be safely ignored.
                Logger.Log("VERIFY", $"Received verify command with no corresponding waitingVerify entry, ignoring [{cmd.Correlation}]");
                return;
            }

            // all verify results must be true for command to succeed.
            inProgressState[cmd.Correlation].Verified &= cmd.Verified;

            // remove the received column from list we're waiting on.
            inProgressState[cmd.Correlation].WaitingVerify = inProgressState[cmd.Correlation]
                .WaitingVerify.Where(a => !(a.Name == cmd.SourceColumnName && a.Value == cmd.SourceColumnValue))
                .ToList();

            // if we're waiting on more, then there's nothing left to do here.
            if (inProgressState[cmd.Correlation].WaitingVerify.Count > 0)
            {
                return;
            }


            // --- verification result has been received for all columns.
            // Logger.Log("VERIFY", $"All verify commands received for: [{cmd.Correlation}]");

            var inProgress = inProgressState[cmd.Correlation];


            // send exit commands for columns that are to be set (or abort).
            foreach (var col in inProgress.ToSet)
            {
                Dictionary<string, string> dataToSet = null;
                if (inProgressState[cmd.Correlation].Verified)
                {
                    dataToSet = new Dictionary<string, string>(inProgress.Data);
                    dataToSet.Add(this.columnName, inProgress.ColumnValue);
                    dataToSet.Remove(col.Key);
                }

                var exitCommand = new Command_Exit
                {
                    Correlation = cmd.Correlation,
                    ColumnValue = inProgress.Data[col.Key],
                    Action = inProgress.Verified ? ActionType.Set : ActionType.Abort,
                    Data = dataToSet,
                    SourceColumnName = this.columnName,
                    SourceColumnValue = inProgress.ColumnValue
                };

                var tp = new TopicPartition(
                    tableSpecification.CommandTopicName(col.Key),
                    Table.Partitioner(exitCommand.ColumnValue, this.numPartitions));

                cmdProducer.ProduceAsync(tp, new Message<Null, string> { Value = JsonConvert.SerializeObject(exitCommand, Formatting.Indented) })
                    .FailIfFaulted("VERIFY", "produce fail");
            }
        
            // send exit commands for column values that are to be deleted (or abort).
            foreach (var col in inProgress.ToDelete)
            {
                var exitCommand = new Command_Exit
                {
                    Correlation = cmd.Correlation,
                    ColumnValue = inProgress.Data[col.Key],
                    Action = inProgress.Verified ? ActionType.Delete : ActionType.Abort,
                    Data = null,
                    SourceColumnName = this.columnName,
                    SourceColumnValue = inProgress.ColumnValue
                };

                var tp = new TopicPartition(
                    tableSpecification.CommandTopicName(col.Key),
                    Table.Partitioner(exitCommand.ColumnValue, this.numPartitions));

                cmdProducer.ProduceAsync(tp, new Message<Null, string> { Value = JsonConvert.SerializeObject(exitCommand, Formatting.Indented) })
                    .FailIfFaulted("VERIFY", "produce fail");
            }

            // if aborting, complete now - there will be no acks.
            if (!inProgressState[cmd.Correlation].Verified)
            {
                CompleteChangeRequest(cmd.Correlation, new Exception("columns verify failed"));
                inProgressState.Remove(cmd.Correlation);
                return;
            }
        }

        private void HandleExit(Command_Exit cmd, Offset offset)
        {
            if (locked.ContainsKey(cmd.ColumnValue))
            {
                if (locked[cmd.ColumnValue] != cmd.Correlation)
                {
                    Logger.Log("EXIT", "correlation doesn't match, unlocking. [{cmd.Correlation}]. expecting: [{locked[cmd.ColumnValue]}]");
                    System.Environment.Exit(1);
                }
            }

            // after this handler, the value is no longer locked
            locked.Remove(cmd.ColumnValue);
            // also, offset is free to progress.
            blockedForCommit.Remove(offset);

            // if the command is aborting, then no ack is expected and we're done.
            if (cmd.Action == ActionType.Abort)
            {
                Logger.Log("EXIT", "Command aborted [{cmd.Correlation}]");
                return;
            }

            // 1. commit new changes values to change logs.

            var tp = new TopicPartition(
                    this.tableSpecification.ChangeLogTopicName(this.columnName),
                    Table.Partitioner(cmd.ColumnValue, this.numPartitions));

            if (cmd.Action == ActionType.Set)
            {
                clProducer.ProduceAsync(tp, new Message<string, string> { Key = cmd.ColumnValue, Value = JsonConvert.SerializeObject(cmd.Data, Formatting.Indented) })
                    .FailIfFaulted("EXIT", "failed to write to changelog");

                if (materialized.ContainsKey(cmd.ColumnValue))
                {
                    materialized[cmd.ColumnValue] = cmd.Data;
                }
                else
                {
                    materialized.Add(cmd.ColumnValue, cmd.Data);
                }
            }
            else if (cmd.Action == ActionType.Delete)
            {
                clProducer.ProduceAsync(tp, new Message<string, string> { Key = cmd.ColumnValue, Value = null })
                    .FailIfFaulted("EXIT", "failed to write tombstone to changelog");

                if (materialized.ContainsKey(cmd.ColumnValue))
                {
                    materialized.Remove(cmd.ColumnValue);
                }
                else
                {
                    Logger.Log("EXIT", "Expecting value to exist in materialized table [{cmd.Correlation}]");
                }
            }

            // 2. send ack commands back.

            var ackCommand = new Command_Ack
            {
                Correlation = cmd.Correlation,
                SourceColumnName = this.columnName,
                SourceColumnValue = cmd.ColumnValue
            };
            
            tp = new TopicPartition(
                this.tableSpecification.CommandTopicName(cmd.SourceColumnName),
                Table.Partitioner(cmd.SourceColumnValue, this.numPartitions));

            cmdProducer.ProduceAsync(tp, new Message<Null, string> { Value = JsonConvert.SerializeObject(ackCommand, Formatting.Indented) })
                .FailIfFaulted("EXIT", "failed to write ack cmd [{cmd.Correlation}]");
        }


        private void HandleAck(Command_Ack cmd, Offset offset)
        {
            // always expect the state corresponding to an ack to exist.
            if (!this.inProgressState.ContainsKey(cmd.Correlation))
            {
                Logger.Log("ACK", $"ERROR!: Ack correlation not found [{cmd.Correlation}]");

                // if the task is known, then that is also unexpected. fail the task with an internal error.
                if (this.inProgressTasks.ContainsKey(cmd.Correlation))
                {
                    CompleteChangeRequest(cmd.Correlation, new Exception($"Ack correlation not found [{cmd.Correlation}]"));
                }

                return;
            }

            var inProgress = this.inProgressState[cmd.Correlation];

            // it's ok if this doesn't exist - may reflect a de-dupe.
            if (inProgress.WaitingAck.Where(a => a.Name == cmd.SourceColumnName && a.Value == cmd.SourceColumnValue).Count() == 1)
            {
                inProgress.WaitingAck = inProgress
                    .WaitingAck.Where(a => !(a.Name == cmd.SourceColumnName && a.Value == cmd.SourceColumnValue))
                    .ToList();
            }
            else
            {
                Logger.Log("ACK", $"ack received, but not waiting for it {cmd.SourceColumnName}, {cmd.SourceColumnValue} [{cmd.Correlation}]");
            }

            if (inProgress.WaitingAck.Count != 0)
            {
                return;
            }

            locked.Remove(inProgress.ColumnValue);
            blockedForCommit.Remove(offset);
            this.inProgressState.Remove(cmd.Correlation);

            CompleteChangeRequest(cmd.Correlation, null);
        }


        private Thread _noCollect; 
        private void StartCommandConsumer(CancellationToken ct)
        {
            _noCollect = new Thread(() =>
            {
                try
                {
                    // the partitions assigned handler does a stacking assignment.
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
                        var cmd = Command.Extract(o);
                        Command.Log(cmd, this.columnName, this.partition);
                        switch(cmd.CommandType)
                        {
                            case CommandType.Change:
                                HandleChange((Command_Change)cmd, cr.Offset);
                                break;
                            case CommandType.Enter:
                                HandleEnter((Command_Enter)cmd, cr.Offset);
                                break;
                            case CommandType.Verify:
                                HandleVerify((Command_Verify)cmd, cr.Offset);
                                break;
                            case CommandType.Exit:
                                HandleExit((Command_Exit)cmd, cr.Offset);
                                break;
                            case CommandType.Ack:
                                HandleAck((Command_Ack)cmd, cr.Offset);
                                break;
                            default:
                                Console.WriteLine($"Unknown command type: {cmd.CommandType}");
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
            lock (inProgressTasks)
            {
                inProgressTasks.Add(correlation, new InProgressTask { TaskCompletionSource = tcs });
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