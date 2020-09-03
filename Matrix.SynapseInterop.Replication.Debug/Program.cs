using System;
using Matrix.SynapseInterop.Replication.DataRows;
using Serilog;
using Serilog.Events;

namespace Matrix.SynapseInterop.Replication.Debug
{
    class Program
    {
        private static ReplicationStream<EventStreamRow> _stream;

        static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
                .Filter.ByIncludingOnly(e => e.Level >= LogEventLevel.Debug)
                .WriteTo
                .Console(outputTemplate:
                    "{Timestamp:yy-MM-dd HH:mm:ss.fff} {Level:u3} {SourceContext:lj} {@Properties} {Message:lj}{NewLine}{Exception}")
                .CreateLogger();

            string replicationHost = Environment.GetEnvironmentVariable("Synapse__replicationHost");
            string replicationPort = Environment.GetEnvironmentVariable("Synapse__replicationPort");
            string serverHostname = Environment.GetEnvironmentVariable("Synapse__hostname");
            bool useRedis = Environment.GetEnvironmentVariable("Synapse__redis") == "true";

            if (string.IsNullOrWhiteSpace(replicationHost)) replicationHost = "localhost";
            if (string.IsNullOrWhiteSpace(replicationPort))
            {
                replicationPort = useRedis ? "6379" : "9092";
            }
            if (string.IsNullOrWhiteSpace(serverHostname)) serverHostname = "localhost";

            StartReplicationAsync(replicationHost, int.Parse(replicationPort), useRedis, serverHostname);
            Console.ReadKey(true);
        }

        private static async void StartReplicationAsync(string replicationHost, int replicationPort, bool useRedis, string serverHostname)
        {
            var replication = new SynapseReplication();
            replication.ClientName = "SynapseReplInterop_Debug";
            replication.ServerName += Replication_ServerName;
            replication.Error += Replication_Error;

            _stream = replication.BindStream<EventStreamRow>();
            _stream.DataRow += Stream_DataRow;
            _stream.PositionUpdate += Stream_PositionUpdate;

            if (!useRedis)
            {
                await replication.ConnectTcp(replicationHost, replicationPort);
            }
            else
            {
                await replication.ConnectRedis(replicationHost, replicationPort, serverHostname);
            }
        }

        private static void Replication_Error(object sender, string e)
        {
            Log.Logger.Error(e);
        }

        private static void Stream_DataRow(object sender, EventStreamRow e)
        {
            Log.Logger.Information("Received event {0} ({1}, {2}) from Synapse", e.EventId, e.Kind, e.SynapseVersion);
        }

        private static void Replication_ServerName(object sender, string e)
        {
            Log.Logger.Information("Server name is {0}", e);
        }

        private static void Stream_PositionUpdate(object sender, string position)
        {
            Log.Logger.Information("Stream is at position {0}", position);
        }
    }
}
