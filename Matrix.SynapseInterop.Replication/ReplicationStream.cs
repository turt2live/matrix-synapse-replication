using System;
using System.Collections.Generic;
using Matrix.SynapseInterop.Replication.DataRows;

namespace Matrix.SynapseInterop.Replication
{
    public class ReplicationStreamName
    {
        [Obsolete("Appears to no longer be in use by Synapse")]
        public static readonly string CURRENT_STATE_DELTAS = "current_state_deltas";
        public static readonly string EVENTS = "events";
        public static readonly string FEDERATION_OUTBOUND_QUEUE = "federation";
        public static readonly string BACKFILL = "backfill";
        public static readonly string PRESENCE = "presence";
        public static readonly string TYPING = "typing";
        public static readonly string RECEIPTS = "receipts";
        public static readonly string PUSH_RULES = "push_rules";
        public static readonly string PUSHERS = "pushers";
        public static readonly string CACHES = "caches";
        public static readonly string PUBLIC_ROOMS = "public_rooms";
        public static readonly string DEVICE_LISTS = "device_lists";
        public static readonly string TO_DEVICE = "to_device";
        public static readonly string TAG_ACCOUNT_DATA = "tag_account_data";
        public static readonly string ACCOUNT_DATA = "account_data";
        public static readonly string GROUPS = "groups";
        public static readonly string USER_SIGNATURES = "user_signature";
    }

    public class ReplicationStream<T> where T : IReplicationDataRow
    {
        private static readonly Dictionary<Type, string> DATA_ROW_STREAM_NAMES = new Dictionary<Type, string>
        {
            {typeof(EventStreamRow), ReplicationStreamName.EVENTS},
            {typeof(FederationStreamRow), ReplicationStreamName.FEDERATION_OUTBOUND_QUEUE},
            {typeof(ReceiptStreamRow), ReplicationStreamName.RECEIPTS},
        };

        private static readonly Dictionary<string, Func<string, IReplicationDataRow>> DATA_ROW_FACTORIES =
            new Dictionary<string, Func<string, IReplicationDataRow>>
            {
                {ReplicationStreamName.EVENTS, EventStreamRow.FromRaw},
                {ReplicationStreamName.FEDERATION_OUTBOUND_QUEUE, FederationStreamRow.FromRaw},
                {ReplicationStreamName.RECEIPTS, ReceiptStreamRow.FromRaw},
            };

        private readonly SynapseReplication _replicationHost;

        private string _position;

        public string StreamName { get; }

        public string CurrentPosition
        {
            get => _position;
            internal set
            {
                _position = value;
                PositionUpdate?.Invoke(this, _position);
            }
        }

        /// <summary>
        /// Creates a new replication stream
        /// </summary>
        /// <param name="replicationHost">The replication host instance.</param>
        /// <param name="resumeFrom">DEPRECATED. Modern Synapse versions do not support this feature.</param>
        internal ReplicationStream(SynapseReplication replicationHost, string resumeFrom = StreamPosition.LATEST)
        {
            _replicationHost = replicationHost;

            StreamName = DATA_ROW_STREAM_NAMES[typeof(T)];
            if (string.IsNullOrWhiteSpace(StreamName)) throw new ArgumentException("No stream for data row type");

            if (string.IsNullOrWhiteSpace(resumeFrom)) resumeFrom = StreamPosition.LATEST;
            CurrentPosition = resumeFrom;

            _replicationHost.RData += ReplicationHost_RData;
            _replicationHost.PositionUpdate += ReplicationHost_PositionUpdate;

            SubscribeToStreams();

            _replicationHost.Connected += ReplicationHost_Connected;
        }

        private void ReplicationHost_Connected(object sender, EventArgs e)
        {
            SubscribeToStreams();
        }

        public event EventHandler<T> DataRow;
        public event EventHandler<string> PositionUpdate;

        private void ReplicationHost_PositionUpdate(object sender, StreamPosition e)
        {
            if (e.StreamName == StreamName) CurrentPosition = e.Position;
        }

        private void ReplicationHost_RData(object sender, ReplicationData e)
        {
            if (e.SteamName != StreamName) return;

            CurrentPosition = e.Position;

            foreach (var row in e.RawRows)
            {
                var dataRow = DATA_ROW_FACTORIES[StreamName](row);
                DataRow?.Invoke(this, (T) dataRow);
            }
        }

        private void SubscribeToStreams()
        {
            // Do not try and subscribe if the Synapse version doesn't support it
            if (_replicationHost.SynapseVersion > SynapseVersion.Pre1130) return;

            // We are knowingly using an Obsolete function here
#pragma warning disable 618
            _replicationHost.SubscribeStream(StreamName, CurrentPosition);
#pragma warning restore 618
        }

        [Obsolete("Modern versions of Synapse do not operate in this way anymore")]
        public void ForcePosition(string newPosition)
        {
            _replicationHost.SubscribeStream(StreamName, newPosition);
            CurrentPosition = newPosition; // set position after advertising it, just in case of failure
        }
    }
}
