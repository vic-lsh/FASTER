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

}
