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

        // Event + State
        public RowKind Kind { get; private set; }
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

            // We have to convert the JArray to something useful
            var rowData = ((JArray) parsed[1]).ToObject<List<dynamic>>();

            switch (kind)
            {
                case RowKind.Event:
                    return new EventStreamRow
                    {
                        Kind = kind,
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
                        RoomId = rowData[0],
                        EventType = rowData[1],
                        StateKey = rowData[2],
                        EventId = rowData[3],
                    };
                default:
                    return new EventStreamRow {Kind = kind};
            }
        }
    }
}
