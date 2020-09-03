# matrix-synapse-replication
A .NET Core library for interacting with Replication Streams in Synapse

TODO: Documentation on how to use this thing.

## Supported Synapse versions

| Library version | Synapse versions |
|-----------------|------------------|
| 0.1.x           | 0.99.3           |
| 0.2.x           | 0.99.3, 0.99.4   |
| 0.2.4           | 0.99.3, 0.99.4, 0.99.5   |
| 0.3.x           | 1.12.3           |
| 0.4.x           | 1.19.1 (redis only) |

**Check the project's README on Github for an up to date list.**

## Development notes

After creating a `SynapseReplication` class, bind all the streams *before* calling `Connect`. This is because the replication protocol will
not have accurate stream positions ahead of the connection attempt and therefore will be told what the stream position is. Once the replication
class gets a server name from Synapse, it will request the position of all streams, thereby starting replication.

**Note**: Older versions of Synapse (1.12.3 and earlier) do not operate in this way. In these older versions, Synapse accepts a stream position
ahead of connection which allows the replication stream to 'catch up', however now that is not possible. Instead, the replication stream handler
(meaning you) will have to catch up async to the replication stream, likely meaning database access. Newer Synapses will only start from the
latest stream position and move forward from there.

The Synapse version you are targetting can be specified in the `SynapseReplication` constructor. Do note that the enum targets a range of Synapse
versions based on the best knowledge at the time of writing - future versions may be introduced which your application will need to handle. **Please
update this library carefully as not all updates will support all Synapse versions safely.** By default, the most recent version range for Synapse
is targetted.
