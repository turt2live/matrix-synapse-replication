namespace Matrix.SynapseInterop.Replication
{
    internal interface ICommandProcessor
    {
        /// <summary>
        /// Processes raw commands direct from the replication stream.
        /// </summary>
        /// <param name="raw">The raw input from the stream</param>
        /// <returns>Any unprocessed portions of the string, or an empty string if nothing</returns>
        string ProcessCommands(string raw);
    }
}
