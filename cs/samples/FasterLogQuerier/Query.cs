using System;

namespace FasterLogQuerier
{
    public class Query
    {
        public ulong SourceId { get; }

        public ulong MinTimestamp { get; }

        public ulong MaxTimestamp { get; }

        public Query(ulong source, ulong minTs, ulong maxTs)
        {
            if (minTs >= maxTs)
            {
                throw new Exception($"Invalid query timerange: [{minTs}, {maxTs})");
            }
            SourceId = source;
            MinTimestamp = minTs;
            MaxTimestamp = maxTs;
        }
    }
}
