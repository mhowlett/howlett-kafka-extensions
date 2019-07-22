using System.Threading.Tasks;

namespace Howlett.Kafka.Extensions.Experiment
{
    class Program
    {
        static async Task Main(string[] args)
        {         
            await Test_BasicAdd.Run();
        }
    }
}
