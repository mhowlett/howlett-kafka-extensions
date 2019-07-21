using System;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;


namespace Howlett.Kafka.Extensions.Experiment
{
    public abstract class Command
    {
        public abstract CommandType CommandType { get; }
        public string Correlation { get; set; }

        public static void Log(Command c, string columnName, int partition)
        {
            var colId = String.Format("{0,15}", $"[{columnName}|{partition}]");

            var summary = c switch
            {
                Command_Change cmd => $"{colId} CHANGE  {cmd.ColumnValue}",
                Command_Enter cmd =>  $"{colId} ENTER   {cmd.ColumnValue}",
                Command_Verify cmd => $"{colId} VERIFY  {cmd.SourceColumnName}",
                Command_Exit cmd =>   $"{colId} EXIT    {cmd.ColumnValue}",
                Command_Ack cmd =>    $"{colId} ACK     {cmd.SourceColumnName}",
                _ => $"unknown command"
            };

            var logLine = String.Format("{0,-60} .." + c.Correlation.Substring(c.Correlation.Length-8), summary);
            Console.WriteLine(logLine);
        }

        public static Command Extract(JObject o)
        {
            var commandType = (CommandType)o.GetValue("CommandType").Value<int>();
            var correlation = o.GetValue("Correlation").Value<string>();

            switch(commandType)
            {
                case CommandType.Change:
                    {
                        var changeType = (AddOrUpdate)o.GetValue("ChangeType").Value<int>();
                        var columnValue = o.GetValue("ColumnValue").Value<string>();
                        var data = (JObject)o.GetValue("Data");

                        Dictionary<string, string> dataAsDict = new Dictionary<string, string>();
                        foreach (var v in data.Descendants())
                        {
                            if (v.GetType() != typeof(JProperty)) continue;
                            dataAsDict.Add(((JProperty)v).Name, ((JProperty)v).Value.ToString());
                        }

                        return new Command_Change
                        {
                            ChangeType = changeType,
                            ColumnValue = columnValue,
                            Correlation = correlation,
                            Data = dataAsDict
                        };
                    }
                case CommandType.Enter:
                    {
                        var isAddCommand = o.GetValue("IsAddCommand").Value<bool>();
                        var columnValue = o.GetValue("ColumnValue").Value<string>();
                        var sourceColumnName = o.GetValue("SourceColumnName").Value<string>();
                        var SourceColumnValue = o.GetValue("SourceColumnValue").Value<string>();
                        
                        return new Command_Enter
                        {
                            IsAddCommand = isAddCommand,
                            ColumnValue = columnValue,
                            Correlation = correlation,
                            SourceColumnName = sourceColumnName,
                            SourceColumnValue = SourceColumnValue
                        };
                    }
                case CommandType.Verify:
                    {
                        var sourceColumnName = o.GetValue("SourceColumnName").Value<string>();
                        var sourceColumnValue = o.GetValue("SourceColumnValue").Value<string>();
                        var verified = o.GetValue("Verified").Value<bool>();

                        return new Command_Verify
                        {
                            Correlation = correlation,
                            SourceColumnName = sourceColumnName,
                            SourceColumnValue = sourceColumnValue,
                            Verified = verified
                        };
                    }
                case CommandType.Exit:
                    {
                        var action = (ActionType)o.GetValue("Action").Value<int>();
                        var columnValue = o.GetValue("ColumnValue").Value<string>();
                        var data = (JObject)o.GetValue("Data");
                        var sourceColumnName = o.GetValue("SourceColumnName").Value<string>();
                        var sourceColumnValue = o.GetValue("SourceColumnValue").Value<string>();

                        Dictionary<string, string> dataAsDict = new Dictionary<string, string>();
                        foreach (var v in data.Descendants())
                        {
                            if (v.GetType() != typeof(JProperty)) continue;
                            dataAsDict.Add(((JProperty)v).Name, ((JProperty)v).Value.ToString());
                        }

                        return new Command_Exit
                        {
                            Correlation = correlation,
                            Action = action,
                            ColumnValue = columnValue,
                            Data = dataAsDict,
                            SourceColumnName = sourceColumnName,
                            SourceColumnValue = sourceColumnValue
                        };
                    }
                case CommandType.Ack:
                    {
                        var sourceColumnName = o.GetValue("SourceColumnName").Value<string>();
                        var sourceColumnValue = o.GetValue("SourceColumnValue").Value<string>();

                        return new Command_Ack
                        {
                            Correlation = correlation,
                            SourceColumnName = sourceColumnName,
                            SourceColumnValue = sourceColumnValue
                        };
                    }
                default:
                    return null;
            }
        }
    }

    public class Command_Change : Command
    {
        public override CommandType CommandType { get => CommandType.Change; }
        public AddOrUpdate ChangeType { get; set; }
        public string ColumnValue { get; set; } // column name is implicit.
        public Dictionary<string, string> Data { get; set; }
    }

    public class Command_Enter : Command
    {
        public override CommandType CommandType { get => CommandType.Enter; }
        public bool IsAddCommand { get; set; }  // whether or not the column can verify, depends on this.
        
        // column name is implicit.
        // if Add, then verify will return true if not exist and no outstanding lock.
        // if Update, then verify will return true if no outstanding lock.
        public string ColumnValue { get; set; }

        public string SourceColumnName { get; set; }
        public string SourceColumnValue { get; set; }
    }

    public class Command_Verify : Command
    {
        public override CommandType CommandType { get => CommandType.Verify; }
        public string SourceColumnName { get; set; }
        public string SourceColumnValue { get; set; } // keep track of this because in the case of change, 2 values will be modified (one add, one delete).
        public bool Verified { get; set; }
    }

    public class Command_Exit : Command
    {
        public override CommandType CommandType { get => CommandType.Exit; }
        public ActionType Action { get; set; }
        public string ColumnValue { get; set; }
        public Dictionary<string, string> Data { get; set; }
        public string SourceColumnName { get; set; }
        public string SourceColumnValue { get; set; }
    }

    public class Command_Ack : Command
    {
        public override CommandType CommandType { get => CommandType.Ack; }
        public string SourceColumnName { get; set; }
        public string SourceColumnValue { get; set; }
    }


}
