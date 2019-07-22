using System;

namespace Howlett.Kafka.Extensions.Experiment
{
    public class Assert
    {
        public static void True(bool cond)
        {
            True(cond, "[null]");
        }

        public static void True(bool cond, string error)
        {
            if (!cond)
            {
                Console.WriteLine(error + " " + System.Environment.StackTrace);
            }
        }
    }
}
