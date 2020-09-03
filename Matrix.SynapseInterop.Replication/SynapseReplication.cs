using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Matrix.SynapseInterop.Replication.DataRows;
using Serilog;

namespace Matrix.SynapseInterop.Replication
{
    public class SynapseReplication
    {
        internal class ReplicationProcessor : ICommandProcessor
        {
            Func<string, string> f;
            bool newlines;

            internal ReplicationProcessor(Func<string, string> process, bool appendNewline = false)
            {
                f = process;
                newlines = appendNewline;
            }

            public string ProcessCommands(string raw)
            {
                if (newlines) raw = raw + "\n";
                return f(raw);
            }
        }

        private static readonly ILogger log = Log.ForContext<SynapseReplication>();

        private readonly Dictionary<string, ReplicationData>
            _pendingBatches = new Dictionary<string, ReplicationData>();

        private readonly Dictionary<Type, object>
            _streams = new Dictionary<Type, object>(); // object is a ReplicationStream<T>. TODO: Don't do this.

        private IReplicationBackend _connection;

        public SynapseVersion SynapseVersion { get; }
        public string ClientName { get; set; }

        public event EventHandler<string> ServerName;
        public event EventHandler<ReplicationData> RData;
        public event EventHandler<StreamPosition> PositionUpdate;
        public event EventHandler<string> Error;
        public event EventHandler<string> Ping;
        public event EventHandler Connected; // Fired for reconnections too
        public event EventHandler<string> RemoteServerUp; // When a process thinks a remote server may be back online

        public SynapseReplication(SynapseVersion estimatedSynapseVersion = SynapseVersion.Post1130)
        {
            SynapseVersion = estimatedSynapseVersion;
        }

        [Obsolete("Use ConnectTcp() instead")]
        public async Task Connect(string address, int port)
        {
            await ConnectTcp(address, port);
        }

        public async Task ConnectTcp(string address, int port)
        {
            _connection?.Disconnect();
            var conn = new SynapseTcpReplication(this.ClientName);
            _connection = conn;
            conn.CommandProcessor = new ReplicationProcessor(this.ProcessCommands);
            conn.Disconnected += Conn_Disconnected;
            conn.Connected += Conn_Connected;
            await conn.Connect(address, port);
        }

        private void Conn_Connected(object sender, EventArgs e)
        {
            Connected?.Invoke(this, null);
        }

        private void Conn_Disconnected(object sender, EventArgs e)
        {
            if (sender == _connection)
            {
                _pendingBatches.Clear();
            }
        }

        public async Task ConnectRedis(string address, int port, string hostname)
        {
            _connection?.Disconnect();
            var conn = new SynapseRedisReplication(this.ClientName, hostname);
            _connection = conn;
            conn.CommandProcessor = new ReplicationProcessor(this.ProcessCommands, true);
            conn.Connected += Conn_Connected;
            await conn.Connect(address, port);
        }

        public async Task Reconnect(bool firstAttempt = false)
        {
            await _connection.Reconnect();
        }

        public void Disconnect()
        {
            _connection.Disconnect();
        }

        public void SendRaw(string command)
        {
            _connection.SendRaw(command);
        }

        private string ProcessCommands(string raw)
        {
            IEnumerable<string> byLine = raw.Split('\n').Where(c => !string.IsNullOrWhiteSpace(c)).ToArray();

            string unprocessed = "";
            if (!raw.EndsWith('\n') && byLine.Any())
            {
                unprocessed = byLine.Last();
                byLine = byLine.Take(byLine.Count() - 1);
            }

            foreach (var cmd in byLine)
            {
                log.Debug("Received: {0}", cmd);
                if (cmd.StartsWith("SERVER "))
                {
                    if (SynapseVersion > SynapseVersion.Pre1130) AcquireStreamPositions();
                    if (ServerName == null) continue;
                    ServerName(this, cmd.Substring("SERVER ".Length));
                }
                else if (cmd.StartsWith("RDATA "))
                {
                    if (RData == null) continue;
                    var row = cmd.Substring("RDATA ".Length);
                    var rowParts = row.Split(new[] {' '}, 3);

                    var stream = rowParts[0];
                    var position = rowParts[1];
                    var rowData = rowParts[2];

                    if (!_pendingBatches.ContainsKey(stream)) _pendingBatches.Add(stream, new ReplicationData(stream));
                    _pendingBatches[stream].AppendRow(rowData);

                    if (position != "batch")
                    {
                        var rdata = _pendingBatches[stream];
                        rdata.Position = position;
                        _pendingBatches.Remove(stream);
                        RData(this, rdata);
                    }
                }
                else if (cmd.StartsWith("POSITION "))
                {
                    if (PositionUpdate == null) continue;
                    var posParts = cmd.Substring("POSITION ".Length).Split(new[] {' '}, 2);

                    var stream = posParts[0];
                    var position = posParts[1];

                    PositionUpdate(this, new StreamPosition {StreamName = stream, Position = position});
                }
                else if (cmd.StartsWith("REMOTE_SERVER_UP "))
                {
                    if (RemoteServerUp == null) continue;
                    RemoteServerUp(this, cmd.Substring("REMOTE_SERVER_UP ".Length));
                }
                else if (cmd.StartsWith("PING "))
                {
                    if (Ping == null) continue;
                    Ping(this, cmd.Substring("PING ".Length));
                }
                else if (cmd.StartsWith("ERROR "))
                {
                    if (Error == null) continue;
                    Error(this, cmd.Substring("ERROR ".Length));
                }
            }

            return unprocessed;
        }

        [Obsolete("Modern versions of Synapse do not operate in this way anymore")]
        public void SubscribeStream(string streamName, string position)
        {
            if (SynapseVersion > SynapseVersion.Pre1130)
            {
                throw new InvalidOperationException(
                    "The Synapse version chosen does not operate REPLICATE in this way");
            }

            SendRaw("REPLICATE " + streamName + " " + position);
        }

        public void AcquireStreamPositions()
        {
            if (SynapseVersion < SynapseVersion.Post1130)
            {
                throw new InvalidOperationException(
                    "The Synapse version chosen does not operate REPLICATE in this way");
            }

            SendRaw("REPLICATE "); // The space is required: https://github.com/matrix-org/synapse/pull/7323
        }

        public void SendFederationAck(string token)
        {
            SendRaw($"FEDERATION_ACK {token}");
        }

        public void SendRemoteServerUp(string serverName)
        {
            SendRaw($"REMOTE_SERVER_UP {serverName}");
        }

        public ReplicationStream<T> BindStream<T>() where T : IReplicationDataRow
        {
            if (_streams.ContainsKey(typeof(T))) throw new ArgumentException("A stream has already been started");
            if (!_streams.ContainsKey(typeof(T))) _streams.Add(typeof(T), new ReplicationStream<T>(this));
            return (ReplicationStream<T>) _streams[typeof(T)];
        }

        [Obsolete("Modern versions of Synapse do not operate in this way anymore")]
        public ReplicationStream<T> ResumeStream<T>(string fromPosition) where T : IReplicationDataRow
        {
            var stream = BindStream<T>();
            stream.CurrentPosition = fromPosition;
            return stream;
        }
    }
}
