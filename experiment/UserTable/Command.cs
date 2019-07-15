using Newtonsoft.Json;
using System.Collections.Generic;


namespace Howlett.Kafka.Extensions.Experiment
{
    public abstract class Command
    {
        public abstract CommandType CommandType { get; }
        public string Correlation { get; set; }
    }

    public class Command_Change : Command
    {
        public override CommandType CommandType { get => CommandType.Change; }
        public AddOrUpdate ChangeType { get; set; }
        public string ColumnValue { get; set; } // column name is implicit.
        public Dictionary<string, string> Data { get; set; }
    }

    public class Command_Lock : Command
    {
        public override CommandType CommandType { get => CommandType.Lock; }
        public AddOrUpdate ChangeType { get; set; }  // whether or not the column can verify, depends on this.
        
        // column name is implicit.
        // if Add, then verify will return true if not exist and no outstanding lock.
        // if Update, then verify will return true if no outstanding lock.
        public string ColumnValue { get; set; }


        public string SourceColumnName { get; set; }
        public int SourceColumnPartition { get; set; }
    }

    public class Command_Verify : Command
    {
        public override CommandType CommandType { get => CommandType.Verify; }
        public string SourceColumnName { get; set; }
        public bool Verified { get; set; }
    }

    public class Command_Unlock : Command
    {
        public override CommandType CommandType { get => CommandType.Unlock; }
        public string ColumnValue { get; set; }
    }

    public class Command_Ack : Command
    {
        public override CommandType CommandType { get => CommandType.Ack; }
        public string SourceColumnName { get; set; }
    }
}
