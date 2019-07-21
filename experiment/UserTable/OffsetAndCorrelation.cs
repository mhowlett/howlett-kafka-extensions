using System;
using System.Collections.Generic;
using Confluent.Kafka;

namespace Howlett.Kafka.Extensions.Experiment
{
    public class OffsetAndCorrelation : IComparable, IComparable<OffsetAndCorrelation>
    {
        public Offset Offset { get; set; }

        public string Correlation { get; set; }

        public int CompareTo(OffsetAndCorrelation obj)
        {
            if (obj == null) { return 1; } // all instances are greater than null.

            if (Offset > obj.Offset)
            {
                return 1;
            }
            if (Offset < obj.Offset)
            {
                return -1;
            }
            return 0;
        }

        public int CompareTo(object obj)
        {
            var bfc = obj as OffsetAndCorrelation;
            if (obj != null)
            {
                return CompareTo(bfc);
            }
            throw new ArgumentException("Object is not an instance of OffsetAndCorrelation");
        }
    }
}
