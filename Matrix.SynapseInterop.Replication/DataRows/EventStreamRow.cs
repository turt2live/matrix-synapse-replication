using System.Collections.Generic;
using Newtonsoft.Json;
using System;
using Newtonsoft.Json.Linq;

namespace Matrix.SynapseInterop.Replication.DataRows
{
    public class EventStreamRow : IReplicationDataRow
    {
        public enum RowKind
        {
            Unknown,
            Event,
            State,
        }

        public enum RowVersion
        {
            Pre0994, // <0.99.4
            Post0994, // >=0.99.4
        }

        // Event + State
        public RowKind Kind { get; private set; }
        public RowVersion SynapseVersion { get; private set; }
        public string RoomId { get; private set; }
        public string EventId { get; private set; }
        public string EventType { get; private set; }
        public string StateKey { get; private set; } // nullable for Events, not State

        // Just Events
        public string RedactsEventId { get; private set; } // nullable

        private EventStreamRow() { }

        public static EventStreamRow FromRaw(string rawDataString)
        {
            var parsed = JsonConvert.DeserializeObject<List<dynamic>>(rawDataString);

            // Synapse 0.99.4+ sends ["ev", [...]] - the 'ev' represents the kind of event.
            RowKind kind = RowKind.Unknown;
            if (parsed[0] == "ev") kind = RowKind.Event;
            if (parsed[0] == "state") kind = RowKind.State;

            var secondArgType = (parsed[1] as object).GetType().FullName;
            RowVersion version = secondArgType == typeof(JArray).FullName ? RowVersion.Post0994 : RowVersion.Pre0994;

            // We have to convert the JArray to something useful
            var rowData = version == RowVersion.Post0994 ? ((JArray) parsed[1]).ToObject<List<dynamic>>() : new List<dynamic>();

            switch (kind)
            {
                case RowKind.Event:
                    return new EventStreamRow
                    {
                        Kind = kind,
                        SynapseVersion = version,
                        EventId = rowData[0],
                        RoomId = rowData[1],
                        EventType = rowData[2],
                        StateKey = rowData[3],
                        RedactsEventId = rowData[4]
                    };
                case RowKind.State:
                    return new EventStreamRow
                    {
                        Kind = kind,
                        SynapseVersion = version,
                        RoomId = rowData[0],
                        EventType = rowData[1],
                        StateKey = rowData[2],
                        EventId = rowData[3],
                    };
                default:
                    if (version == RowVersion.Pre0994)
                    {
                        return new EventStreamRow
                        {
                            Kind = kind,
                            SynapseVersion = version,
                            EventId = parsed[0],
                            RoomId = parsed[1],
                            EventType = parsed[2],
                            StateKey = parsed[3],
                            RedactsEventId = parsed[4]
                        };
                    }
                    else
                    {
                        return new EventStreamRow {Kind = kind, SynapseVersion = version};
                    }
            }
        }
    }
}
