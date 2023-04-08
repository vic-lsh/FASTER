using System.Collections.Generic;
using System.Buffers.Binary;
using System;
using System.Text;
using System.Linq;

namespace FasterLogData
{

    using Value = Dictionary<string, string>;

    public enum OtelType : byte
    {
        Metric,
        Log,
        Span,
        PerfTrace
    }

    public enum ValueType : byte
    {
        String,
        Int,
        Uint,
        Float,
        Bool,
        Null
    }

    public static class StringExtension
    {
        public static OtelType ToOtelType(this string s) =>
            s switch
            {
                "Metric" => OtelType.Metric,
                "Log" => OtelType.Log,
                "Span" => OtelType.Span,
                "PerfTrace" => OtelType.PerfTrace,
                _ => throw new Exception($"invalid OtelType string '{s}'"),
            };

        public static ValueType ToValueType(this string s) =>
            s switch
            {
                "String" => ValueType.String,
                "Int" => ValueType.Int,
                "Uint" => ValueType.Uint,
                "Float" => ValueType.Float,
                "Bool" => ValueType.Bool,
                "Null" => ValueType.Null,
                _ => throw new Exception("invalid ValueType string"),
            };
    }

    public class Serializer
    {
        public static byte[] IntToBigEndianBytes(int n)
        {
            var bytes = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan<byte>(), n);
            return bytes;
        }

        public static byte[] LongToBigEndianBytes(long n)
        {
            var bytes = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan<byte>(), n);
            return bytes;
        }

        public static byte[] UlongToBigEndianBytes(ulong n)
        {
            var bytes = new byte[8];
            BinaryPrimitives.WriteUInt64BigEndian(bytes.AsSpan<byte>(), n);
            return bytes;
        }

        public static byte[] UintToBigEndianBytes(uint n)
        {
            var bytes = new byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(bytes.AsSpan<byte>(), n);
            return bytes;
        }

        public static byte[] ShortToBigEndianBytes(short n)
        {
            var bytes = new byte[2];
            BinaryPrimitives.WriteInt16BigEndian(bytes.AsSpan<byte>(), n);
            return bytes;
        }
    }

    public class Point
    {
        public string otel_type { get; set; }

        public ulong timestamp { get; set; }

        public Dictionary<string, Value> attributes;

        public Value[] values { get; set; }

        public static readonly int TYPE_OFFSET = 6;

        public static readonly int SOURCE_ID_OFFSET = 8;

        public static readonly int TIMESTAMP_OFFSET = 16;

        public static byte[] Serialize(Point point)
        {
            // Serialization format
            //
            //  0   1   2   3   4   5   6   7
            // |---|---|---|---|---|---|---|---|
            // | sample size   |hdr sz |typ|   |
            // |         source id             |
            // |         timestamp             |
            // | var1 type&idx | var2 type&idx |
            // |     ...       |     ...       |
            // |     ...       |     ...       |
            // |    Sample variable data ...   |
            //
            // where type&idx uses the first byte to indicate the variable's
            // type, and the remaining 3 bytes to store the variable offset.
            //
            // To access a variable's data, offset into `hdr sz + var offset`.
            //
            // If the variable is a string, the first 4 bytes of the variable
            // is the length of the variable.

            List<byte> bytes = new List<byte>();

            var otelType = point.otel_type.ToOtelType();
            var sourceId = Point.calculateSourceId(point.attributes);

            // sample size (to be written at the end)
            bytes.AddRange(Enumerable.Repeat((byte)0, 4));

            var headerSize = point.calcHeaderSize();
            bytes.AddRange(Serializer.ShortToBigEndianBytes(headerSize));
            bytes.Add((byte)otelType);
            bytes.Add((byte)0); // padding

            bytes.AddRange(Serializer.UlongToBigEndianBytes(sourceId));
            bytes.AddRange(Serializer.UlongToBigEndianBytes(point.timestamp));

            List<(ValueType, byte[])> serialized = new List<(ValueType, byte[])>();
            foreach (var value in point.values)
            {
                var (type, valueBytes) = Point.serializeValueEntry(value);
                serialized.Add((type, valueBytes));
                bytes.AddRange(Enumerable.Repeat((byte)0, 4)); // reserved space for (type & idx)
            }

            for (int i = 0; i < serialized.Count; i++)
            {
                var varOffset = bytes.Count;
                if (varOffset >= 16_777_216)
                {
                    throw new Exception("Sample is too large. Sample maximum size is 2^24.");
                }

                var varOffsetBytes = Serializer.UintToBigEndianBytes((uint)varOffset);

                // steal the offset's first byte to store variable type
                varOffsetBytes[0] = (byte)serialized[i].Item1;

                var start = Point.getEntryOffsetLoc(i);
                for (var j = 0; j < 4; j++)
                {
                    bytes[start + j] = varOffsetBytes[j];
                }

                var (type, valueBytes) = serialized[i];
                if (type == ValueType.String)
                {
                    bytes.AddRange(Serializer.UintToBigEndianBytes((uint)valueBytes.Length));
                }
                bytes.AddRange(valueBytes);

            }

            // write sample size
            var sampleSizeBytes = Serializer.UintToBigEndianBytes((uint)bytes.Count);
            for (int i = 0; i < 4; i++)
            {
                bytes[i] = sampleSizeBytes[i];
            }

            return bytes.ToArray();
        }

        // TODO
        // public static Point Deserialize(byte[] bytes) { }

        private static ulong calculateSourceId(Dictionary<string, Value> attrs)
        {
            var stringIdentifier = attrsToString(attrs);
            var strBytes = ASCIIEncoding.ASCII.GetBytes(stringIdentifier);
            var hashBytes = System.Security.Cryptography.MD5.Create().ComputeHash(strBytes);

            // take the first 8 bytes as the hash -- ignore the rest
            var hash = BinaryPrimitives.ReadUInt64BigEndian(new Span<byte>(hashBytes, 0, 8));

            return hash;
        }

        private static string attrsToString(Dictionary<string, Value> attrs)
        {
            var ret = "";

            foreach (KeyValuePair<string, Value> kvp in attrs)
            {
                if (kvp.Value.Count != 1)
                {
                    throw new Exception("Attribute value should be represented as a single kv pair where the key is a type");
                }

                // concat the real key-value pair
                ret += kvp.Key;
                foreach (KeyValuePair<string, string> pair in kvp.Value)
                {
                    ret += pair.Value;
                }
            }

            return ret;
        }

        public static bool IsType(Span<byte> serialized, OtelType type)
        {
            return serialized[TYPE_OFFSET] == (byte)type;
        }

        public static ulong GetSourceIdFromSerialized(Span<byte> serialized)
        {
            if (serialized.Length < SOURCE_ID_OFFSET + 8)
            {
                throw new Exception("Corrupted serialized sample: sample does not contain a source ID");
            }

            var id = BinaryPrimitives.ReadUInt64BigEndian(serialized.Slice(SOURCE_ID_OFFSET, 8));
            return id;
        }

        public static ulong GetTimestampFromSerialized(Span<byte> serialized)
        {
            if (serialized.Length < TIMESTAMP_OFFSET + 8)
            {
                throw new Exception("Corrupted serialized sample: sample does not contain a timestamp");
            }

            var ts = BinaryPrimitives.ReadUInt64BigEndian(serialized.Slice(TIMESTAMP_OFFSET, 8));
            return ts;
        }

        public static void UpdateSerializedPointTimestamp(Span<byte> serialized, ulong newTimestamp)
        {
            BinaryPrimitives.WriteUInt64BigEndian(serialized.Slice(TIMESTAMP_OFFSET, 8), newTimestamp);
        }

        public static uint GetSampleSize(Span<byte> serialized)
        {
            if (serialized.Length < 4)
            {
                throw new Exception("Corrupted serialized sample: sample does not contain its size");
            }

            var sampleSize = BinaryPrimitives.ReadUInt32BigEndian(serialized.Slice(0, 4));
            return sampleSize;
        }

        private short calcHeaderSize()
        {
            var sz = 0;

            sz += 4; // sample size
            sz += 2; // header size
            sz += 1; // sample type
            sz += 1; // padding
            sz += 8; // source id
            sz += 8; // timestamp
            sz += 4 * values.Length; // size for (type & idx) per variable

            if (sz > 65535)
            {
                throw new Exception("Point too large");
            }

            return (short)sz;
        }

        private static int getEntryOffsetLoc(int index)
        {
            return 4 /* sample size */
                + 2 /* header size */
                + 1 /* otel type */
                + 1 /* padding */
                + 8 /* source id */
                + 8 /* timestamp */
                + index * 4 /* prev (type & idx) entries */;
        }

        private static ValueType getValueType(Value val)
        {
            return val.Keys.First().ToValueType();
        }


        private static (ValueType, byte[]) serializeValueEntry(Value val)
        {
            var entry = val.First();
            var valueType = entry.Key.ToValueType();
            var valueBytes = valueType switch
            {
                ValueType.String => System.Text.Encoding.UTF8.GetBytes(entry.Value),
                ValueType.Int => Serializer.LongToBigEndianBytes(long.Parse(entry.Value)),
                ValueType.Uint => Serializer.UlongToBigEndianBytes(ulong.Parse(entry.Value)),
                ValueType.Float => BitConverter.GetBytes(double.Parse(entry.Value)),
                ValueType.Bool => BitConverter.GetBytes(bool.Parse(entry.Value)),
                _ => throw new Exception("Serializing null value not supported yet"),
            };

            return (valueType, valueBytes);
        }
    }
}
