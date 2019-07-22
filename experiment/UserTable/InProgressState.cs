using Confluent.Kafka;
using System.Collections.Generic;

namespace Howlett.Kafka.Extensions.Experiment
{
    public class InProgressState_Active
    {
        public Offset ChangeCommandOffset { get; set; }
        public string ColumnValue { get; set; }
        public List<NameAndValue> WaitingVerify { get; set; }
        public List<NameAndValue> WaitingAck { get; set; }
        public Dictionary<string, string> Data { get; set; }
        public Dictionary<string, string> ToDelete { get; set; }
        public Dictionary<string, string> ToSet { get; set; }
        public Dictionary<string, string> OldData { get; set; }
        public List<NameAndValue> VerifyFailed { get; set; }
    }

    public class InProgressState_Secondary
    {
        public Offset EnterCommandOffset { get; set; }
    }
}
