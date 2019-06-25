using Newtonsoft.Json;
using System.Collections.Generic;


namespace Howlett.Kafka.Extensions.Experiment
{
    public abstract class Command
    {
        public abstract CommandType CommandType { get; }
        public string Correlation { get; set; }
    }

    public class Command_Apply : Command
    {
        public override CommandType CommandType { get => CommandType.Apply; }
        public ChangeType ChangeType { get; set; }
        public string ColumnName { get; set; }
        public object ColumnValue { get; set; }
        public Dictionary<string, object> Data { get; set; }
    }

    public class Command_Verify : Command
    {
        public override CommandType CommandType { get => CommandType.Verify; }
        public bool Verified { get; set; }
    }

}
