using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Serilog;

namespace Matrix.SynapseInterop.Replication
{
    internal class SynapseTcpReplication : IReplicationBackend
    {
        private static readonly ILogger log = Log.ForContext<SynapseTcpReplication>();

        private TcpClient _client;
        private string _lastAddress;
        private int _lastPort;
        private Timer _pingTimer;
        private bool _reconnectionInProgress = false;

        public event EventHandler Connected; // Fired for reconnections too
        public event EventHandler Disconnected;

        public string ClientName { get; private set; }

        public ICommandProcessor CommandProcessor { get; set; }

        public SynapseTcpReplication(string clientName)
        {
            ClientName = clientName;
        }

        public async Task Connect(string address, int port)
        {
            _lastAddress = address;
            _lastPort = port;

            await Reconnect(true);
        }

        public void Disconnect()
        {
            _client?.Close();

            // Stop the timer - we'll create a new one if we need to
            _pingTimer?.Change(Timeout.Infinite, Timeout.Infinite);

            Disconnected?.Invoke(this, null);
        }

        public async Task Reconnect(bool firstAttempt = false)
        {
            if (!firstAttempt) log.Information("Lost replication connection - reconnecting");

            Disconnect();

            // Resolve the address
            var dns = await Dns.GetHostEntryAsync(_lastAddress);
            IPAddress ip;

            try
            {
                // TcpClient doesn't support IPV6 :(
                ip = dns.AddressList.First(i => i.AddressFamily == AddressFamily.InterNetwork);
            }
            catch (InvalidOperationException ex)
            {
                throw new InvalidOperationException($"No IPv4 address found for {_lastAddress}", ex);
            }

            // Form a connection
            _client = new TcpClient();
            log.Information("Connecting to replication stream on {ip}:{_lastPort}", ip, _lastPort);
            await _client.ConnectAsync(ip, _lastPort);

            // Name our client
            var name = string.IsNullOrWhiteSpace(ClientName) ? "NETCORESynapseReplication" : ClientName;
            SendRaw("NAME " + name);

            // Start pinging 
            _pingTimer = new Timer(context =>
            {
                try
                {
                    SendPing(context);
                }
                catch (Exception ex)
                {
                    log.Error("Ping failed: {ex}", ex);
                }
            }, null, TimeSpan.FromSeconds(0), TimeSpan.FromSeconds(5));

            // Start the reader
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            Task.Run(() => ReadLoop());
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

            Connected?.Invoke(this, null);
        }

        private async void ReconnectLoop()
        {
            if (_reconnectionInProgress)
            {
                // Do not try to run ReconnectLoop concurrently.
                return;
            }

            _reconnectionInProgress = true;
            var attempt = 1;

            while (true)
            {
                log.Information($"Reconnecting to replication: Attempt {attempt++}");

                try
                {
                    await Reconnect();
                    break;
                }
                catch (Exception ex)
                {
                    log.Error($"Reconnection failed: {ex}");
                    await Task.Delay(TimeSpan.FromSeconds(5));
                }
            }

            _reconnectionInProgress = false;
        }

        private async void ReadLoop()
        {
            try
            {
                var buf = new byte[1024];
                var stream = _client.GetStream();
                var result = new StringBuilder();

                while (_client.Connected)
                {
                    do
                    {
                        var read = stream.Read(buf, 0, buf.Length);
                        result.Append(Encoding.UTF8.GetString(buf, 0, read));
                    } while (stream.DataAvailable);

                    try
                    {
                        string unprocessed = result.ToString();
                        if (CommandProcessor != null)
                        {
                            unprocessed = CommandProcessor.ProcessCommands(result.ToString());
                        }
                        result = new StringBuilder(unprocessed);
                    }
                    catch (Exception ex)
                    {
                        log.Error("Failed to process command: {0}", ex);
                        SendRaw("ERROR Error processing incoming commands");
                        await Reconnect();
                    }
                }
            }
            catch (Exception ex)
            {
                log.Error("Failed to read from replication: {0}", ex);
                ReconnectLoop();
            }
        }

        public void SendRaw(string command)
        {
            var shortCommand = command.Length > 80 ? command.Substring(0, 80) : command;
            log.Debug($"Sending {shortCommand}");

            try
            {
                _client.Client.Send(Encoding.UTF8.GetBytes(command + "\n"));
            }
            catch (Exception ex)
            {
                ReconnectLoop();
                throw new Exception("Failed to send command to Synapse", ex);
            }
        }

        private void SendPing(object context)
        {
            SendRaw("PING " + DateTime.Now.ToBinary());
        }
    }
}
