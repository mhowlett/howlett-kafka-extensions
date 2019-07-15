using System.Threading.Tasks;

namespace Howlett.Kafka.Extensions.Experiment
{
    public class WaitingForResult
    {
        public TaskCompletionSource<bool> TaskCompletionSource;
        public bool Verified;
    }
}
