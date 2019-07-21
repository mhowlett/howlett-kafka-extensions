using System.Threading.Tasks;

namespace Howlett.Kafka.Extensions.Experiment
{
    public class InProgressTask
    {
        public TaskCompletionSource<bool> TaskCompletionSource;
        public bool Verified;
    }
}
