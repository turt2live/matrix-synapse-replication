using System.Threading.Tasks;

namespace Matrix.SynapseInterop.Replication
{
    public interface IReplicationBackend
    {
        Task Reconnect(bool firstAttempt = false);
        void Disconnect();
        void SendRaw(string command);
    }
}
