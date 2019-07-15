using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;


namespace Howlett.Kafka.Extensions.Experiment
{
    public class TableSpecification
    {
        public TableSpecification(string json)
        {
            dynamic o = JsonConvert.DeserializeObject(json);
            Name = o.Name;
            var cols = o.Columns;
            List<ColumnSpecification> cs = new List<ColumnSpecification>();
            foreach (var c in cols)
            {
                var n = c.Name;
                var v = (JToken)c.Value["Type"];
                var v1 = (JToken)c.Value["Unique"];
                var u = (bool)v1.ToObject(typeof(bool));
                var cs1 = new ColumnSpecification(n, u);
                cs.Add(cs1);
            }
            ColumnSpecifications = cs;
        }

        public string Name { get; }

        public List<ColumnSpecification> ColumnSpecifications { get; }
        
        public string ChangeLogTopicName(string column)
        {
            var cs = ColumnSpecifications.Where(cs => cs.Name == column).Single();
            if (!cs.Unique)
            {
                throw new Exception("not unique");
            }
            return $"{Name}_{cs.Name}";
        }

        public string CommandTopicName(string column)
        {
            var cs = ColumnSpecifications.Where(cs => cs.Name == column).Single();
            if (!cs.Unique)
            {
                throw new Exception("not unique");
            }
            return $"__{Name}_{ColumnSpecifications.Where(cs => cs.Name == column).Single().Name}_cmd";
        }

        public List<string> AllCommandTopics()
        {
            return ColumnSpecifications.Where(cs => cs.Unique).Select(cs => cs.Name).Select(cs => $"__{Name}_" + cs + "_cmd").ToList();
        }
    }
}
