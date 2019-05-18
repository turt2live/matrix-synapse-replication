using System;
using Matrix.SynapseInterop.Replication.DataRows;
using Serilog;
using Serilog.Events;

namespace Matrix.SynapseInterop.Replication.Debug
{
    class Program
    {
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

            if (string.IsNullOrWhiteSpace(replicationHost)) replicationHost = "localhost";
            if (string.IsNullOrWhiteSpace(replicationPort)) replicationPort = "9092";

            StartReplicationAsync(replicationHost, int.Parse(replicationPort));
            Console.ReadKey(true);
        }

        private static async void StartReplicationAsync(string replicationHost, int replicationPort)
        {
            var replication = new SynapseReplication();
            replication.ClientName = "SynapseReplInterop_Debug";
            replication.ServerName += Replication_ServerName;

            await replication.Connect(replicationHost, replicationPort);

            var stream = replication.BindStream<EventStreamRow>();
            stream.DataRow += Stream_DataRow;
        }

        private static void Stream_DataRow(object sender, EventStreamRow e)
        {
            Log.Logger.Information("Received event {0} ({1}, {2}) from Synapse", e.EventId, e.Kind, e.SynapseVersion);
        }

        private static void Replication_ServerName(object sender, string e)
        {
            Log.Logger.Information("Server name is {0}", e);
        }
    }
}
