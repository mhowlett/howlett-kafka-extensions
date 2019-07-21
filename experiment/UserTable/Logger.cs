using System;

namespace Howlett.Kafka.Extensions.Experiment
{
    public class Logger
    {
        public static void Log(string facility, string msg)
        {
            Console.WriteLine($"{facility}: {msg}");
        }   
    }
}