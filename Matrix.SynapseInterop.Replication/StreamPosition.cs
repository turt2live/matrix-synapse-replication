namespace Matrix.SynapseInterop.Replication
{
    public class StreamPosition
    {
        public const string LATEST = "NOW";

        public string StreamName { get; internal set; }
        public string Position { get; internal set; }

        internal StreamPosition() { }
    }
}
