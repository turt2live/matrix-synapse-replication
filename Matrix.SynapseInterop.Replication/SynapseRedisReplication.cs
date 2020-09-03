using System;
using System.Threading.Tasks;
using Serilog;
using StackExchange.Redis;

namespace Matrix.SynapseInterop.Replication
{
    internal class SynapseRedisReplication : IReplicationBackend
    {
        private static readonly ILogger log = Log.ForContext<SynapseRedisReplication>();

        private ConnectionMultiplexer _redis;
        private string _hostname;

        public string ClientName { get; private set; }
        public ICommandProcessor CommandProcessor { get; set; }
        public event EventHandler Connected; // Fired for reconnections too

        public SynapseRedisReplication(string clientName, string hostname)
        {
            ClientName = clientName;
            _hostname = hostname;
        }

        public async Task Connect(string address, int port)
        {
            log.Information($"Connecting to redis at {address}:{port}");
            _redis = await ConnectionMultiplexer.ConnectAsync($"{address}:{port}");
            log.Information($"Subscribing to channel {_hostname}");
            var chan = await _redis.GetSubscriber().SubscribeAsync(_hostname);
            chan.OnMessage((msg) => this.OnMessage(msg));
            Connected?.Invoke(this, null);
        }

        public void Disconnect()
        {
            _redis.Close();
        }

        public async Task Reconnect(bool firstAttempt = false)
        {
            // no-op
            await Task.Delay(0);
        }

        public void SendRaw(string command)
        {
            log.Debug($"Sending {command}");
            _redis.GetSubscriber().Publish(_hostname, command);
        }

        private void OnMessage(ChannelMessage msg)
        {
            log.Debug($"Received {msg.Message.ToString()}");
            CommandProcessor?.ProcessCommands(msg.Message.ToString());
        }
    }
}
