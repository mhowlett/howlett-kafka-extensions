using Newtonsoft.Json;

namespace Howlett.Kafka.Extensions.Experiment
{
    public class ColumnSpecification
    {
        public ColumnSpecification(string name, bool unique)
        {
            Name = name;
            Unique = unique;
        }

        public string Name { get; }

        public bool Unique { get; }
    }
}
