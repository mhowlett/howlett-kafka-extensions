using Newtonsoft.Json;

namespace Howlett.Kafka.Extensions.Experiment
{
    public class ColumnSpecification
    {
        public ColumnSpecification(string name, ColumnType type, bool unique)
        {
            Name = name;
            Type = type;
            Unique = unique;
        }

        public string Name { get; }

        public ColumnType Type { get; }

        public bool Unique { get; }
    }
}
