using System.Collections.Generic;
using System.Buffers.Binary;
using System.Linq;
using System;

namespace FasterLogQuerier
{
    public class ExperimentStart
    {
        public HashSet<ulong> Sources { get; }

        public ExperimentStart(HashSet<ulong> s)
        {
            Sources = s;
        }

        public byte[] Encode()
        {
            var bytes = new List<byte>();
            var ulongBytes = new byte[8];

            // write # of sources
            BinaryPrimitives.WriteUInt64BigEndian(ulongBytes.AsSpan(), (uint)Sources.Count());
            bytes.AddRange(ulongBytes);

            // write each source
            foreach (var source in Sources)
            {
                BinaryPrimitives.WriteUInt64BigEndian(ulongBytes.AsSpan(), source);
                bytes.AddRange(ulongBytes);
            }

            return bytes.ToArray();
        }

        public static ExperimentStart Decode(byte[] bytes)
        {
            if (bytes.Length < 8)
            {
                throw new Exception("ExperimentStart bytes does not contain size");
            }

            var size = BinaryPrimitives.ReadUInt64BigEndian(new ReadOnlySpan<byte>(bytes, 0, 8));

            if (bytes.Length < (int)size + 8)
            {
                throw new Exception("ExperimentStart bytes does not contain all sources");
            }

            var sources = new HashSet<ulong>();
            for (var i = 0; i < (int)size; i++)
            {
                var start = 8 + i * 8;
                var source = BinaryPrimitives.ReadUInt64BigEndian(new ReadOnlySpan<byte>(bytes, start, 8));
                if (!sources.Add(source))
                {
                    throw new Exception("ExperimentStart bytes contains duplicate sources");
                }
            }

            return new ExperimentStart(sources);
        }
    }

    public class Query
    {
        public ulong SourceId { get; }

        public ulong MinTimestamp { get; }

        public ulong MaxTimestamp { get; }

        // Optional. 0 if not set.
        public ulong NextBlockAddr { get; set; }

        public Query(ulong source, ulong minTs, ulong maxTs)
        {
            if (minTs >= maxTs)
            {
                throw new Exception($"Invalid query timerange: [{minTs}, {maxTs})");
            }
            SourceId = source;
            MinTimestamp = minTs;
            MaxTimestamp = maxTs;
            NextBlockAddr = 0;
        }

        public bool IsNewQuery()
        {
            return NextBlockAddr == 0;
        }

        public bool HasNextBlockAddr()
        {
            return NextBlockAddr != 0;
        }

        public byte[] Encode()
        {
            var bytes = new List<byte>();
            var ulongBytes = new byte[8];

            BinaryPrimitives.WriteUInt64BigEndian(ulongBytes.AsSpan(), SourceId);
            bytes.AddRange(ulongBytes);

            BinaryPrimitives.WriteUInt64BigEndian(ulongBytes.AsSpan(), MinTimestamp);
            bytes.AddRange(ulongBytes);

            BinaryPrimitives.WriteUInt64BigEndian(ulongBytes.AsSpan(), MaxTimestamp);
            bytes.AddRange(ulongBytes);

            BinaryPrimitives.WriteUInt64BigEndian(ulongBytes.AsSpan(), NextBlockAddr);
            bytes.AddRange(ulongBytes);

            return bytes.ToArray();
        }

        public static Query Decode(Span<byte> bytes)
        {
            if (bytes.Length < 32)
            {
                throw new Exception("Query bytes is too small");
            }

            var source = BinaryPrimitives.ReadUInt64BigEndian(bytes.Slice(0, 8));
            var minTs = BinaryPrimitives.ReadUInt64BigEndian(bytes.Slice(8, 8));
            var maxTs = BinaryPrimitives.ReadUInt64BigEndian(bytes.Slice(16, 8));
            var nextBlockAddr = BinaryPrimitives.ReadUInt64BigEndian(bytes.Slice(24, 8));

            var q = new Query(source, minTs, maxTs);
            q.NextBlockAddr = nextBlockAddr;

            return q;
        }
    }

    public class BlockReply
    {
        bool IsActive { get; set; }
        byte[] block;

        public BlockReply(byte[] b)
        {
            block = b;
        }

        public BlockReply()
        {
        }

        public static BlockReply InactiveReply()
        {
            BlockReply b = new BlockReply();
            b.IsActive = false;
            return b;
        }

        public static bool IsServerActive(byte[] bytes)
        {
            if (bytes.Length == 0)
            {
                throw new Exception("Cannot determine BlockReply.IsActive on an empty byte array");
            }

            return bytes[0] == 1;
        }

        public byte[] Encode()
        {
            if (block != null)
            {
                var bytes = new byte[block.Length + 1];
                bytes[0] = IsActive ? (byte)1 : (byte)0;
                Buffer.BlockCopy(block, 0, bytes, 1, block.Length);
                return bytes;
            }
            else
            {
                var bytes = new byte[1];
                bytes[0] = IsActive ? (byte)1 : (byte)0;
                return bytes;
            }

        }
    }
}
