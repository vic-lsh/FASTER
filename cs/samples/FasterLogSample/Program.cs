// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Channels;
using System.Buffers.Binary;
using System.Text;
using System.Diagnostics;
using System.Threading;
using System.Collections.Generic;
using K4os.Compression.LZ4;
using FASTER.core;
using System.Linq;
//using System.Text.Json;
using Newtonsoft.Json;

namespace FasterLogSample
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

    class Serializer
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

        public static bool IsType(byte[] serialized, OtelType type)
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

        public static void UpdateSerializedPointTimestamp(byte[] serialized, ulong newTimestamp)
        {
            BinaryPrimitives.WriteUInt64BigEndian(new Span<byte>(serialized, TIMESTAMP_OFFSET, 8), newTimestamp);
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

    class DataLoader
    {

        public static List<Point> LoadSamples(string filePath)
        {
            List<Point> points = new List<Point>();
            var rand = new Random();
            var erred = 0;
            foreach (string line in System.IO.File.ReadLines(filePath))
            {
                try
                {
                    var shouldInclude = rand.NextDouble() < 0.10;
                    if (shouldInclude)
                    {
                        var point = JsonConvert.DeserializeObject<Point>(line);
                        if (point.otel_type == "PerfTrace")
                        {
                            points.Add(point);
                            if (points.Count % 100000 == 0)
                            {
                                Console.WriteLine("loaded {0} samples", points.Count);
                            }
                            if (points.Count == 1000000)
                            {
                                break;
                            }
                        }
                    }
                }
                catch (Exception)
                {
                    Console.WriteLine("Failed to parse {0}", line);
                    erred++;
                }
            }
            Console.WriteLine("Failed to read {0} samples", erred);

            return points;
        }

        public static List<byte[]> LoadSerializedSamples(string filePath)
        {
            var points = new List<byte[]>();
            var rand = new Random();
            var erred = 0;
            foreach (string line in System.IO.File.ReadLines(filePath))
            {
                try
                {
                    // var shouldInclude = rand.NextDouble() < 0.10;
                    if (true)
                    {
                        var point = JsonConvert.DeserializeObject<Point>(line);
                        if (point.otel_type == "PerfTrace")
                        {
                            var sp = Point.Serialize(point);
                            points.Add(sp);
                            if (points.Count % 100000 == 0)
                            {
                                Console.WriteLine("loaded {0} samples", points.Count);
                            }
                        }
                    }
                }
                catch (Exception)
                {
                    Console.WriteLine("Failed to parse {0}", line);
                    erred++;
                }
            }
            Console.WriteLine("Failed to read {0} samples", erred);

            return points;
        }

        public static List<(ulong, byte[])> LoadSerializedSamplesWithTimestamp(string filePath)
        {
            var points = new List<(ulong, byte[])>();
            var ulongBytes = new byte[8];

            ulong totalSize = 0;

            using (BinaryReader reader = new BinaryReader(new FileStream(filePath, FileMode.Open)))
            {
                while (true)
                {
                    var bytesRead = reader.Read(ulongBytes, 0, 8);
                    if (bytesRead == 0)
                    {
                        break;
                    }
                    if (bytesRead != 8)
                    {
                        throw new Exception("failed to read timestamp delta bytes");
                    }
                    var tsDelta = BinaryPrimitives.ReadUInt64BigEndian(ulongBytes.AsSpan<byte>());

                    if (reader.Read(ulongBytes, 0, 8) != 8)
                    {
                        throw new Exception("failed to read serialized binary size bytes");
                    }
                    var serializationSize = BinaryPrimitives.ReadUInt64BigEndian(ulongBytes.AsSpan<byte>());

                    var serialized = new byte[serializationSize];
                    if (reader.Read(serialized, 0, (int)serializationSize) != (int)serializationSize)
                    {
                        throw new Exception("Failed to read the entire serialized sample");
                    }
                    points.Add((tsDelta, serialized));
                    totalSize += (ulong)serialized.Length;

                    if (points.Count % 1000000 == 0)
                    {
                        Console.WriteLine("loaded {0} samples", points.Count);
                    }

                }
            }

            Console.WriteLine("Read {0} samples", points.Count);
            Console.WriteLine("Total size {0} ", totalSize);

            return points;
        }

        public static List<(ulong, byte[])> SaveSerializedSamplesToFile(string filePath)
        {
            ulong firstTimestamp = 0;
            var points = new List<(ulong, byte[])>();
            var rand = new Random();
            var erred = 0;
            var numRead = 0;
            var outputFile = "serialized_samples";

            File.Delete(outputFile);
            using (var outfile = new FileStream(outputFile, FileMode.Append))
            {
                foreach (string line in System.IO.File.ReadLines(filePath))
                {
                    try
                    {
                        var point = JsonConvert.DeserializeObject<Point>(line);

                        if (firstTimestamp == 0)
                        {
                            // first point
                            firstTimestamp = point.timestamp;
                        }

                        var tsDelta = point.timestamp - firstTimestamp;
                        var sp = Point.Serialize(point);

                        var deltaBytes = Serializer.UlongToBigEndianBytes(tsDelta);
                        outfile.Write(deltaBytes, 0, deltaBytes.Length);
                        var serializedLenBytes = Serializer.UlongToBigEndianBytes((ulong)sp.Length);
                        outfile.Write(serializedLenBytes, 0, serializedLenBytes.Length);
                        outfile.Write(sp, 0, sp.Length);
                        numRead++;

                        if (numRead % 100000 == 0)
                        {
                            Console.WriteLine("loaded {0} samples", numRead);
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Failed to parse {0}, exp {1}", line, e);
                        erred++;
                    }
                }
            }

            Console.WriteLine("Failed to read {0} samples", erred);

            return points;
        }

    }

    public class Program
    {
        const int sampleBatchSize = 1 << 20;
        const int compressedBatchSize = 1 << 22;

        const double CyclesPerNanosecond = 2.7;

        static long samplesGenerated = 0;
        static long samplesDropped = 0;
        static long samplesWritten = 0;
        static ulong lastIngestAddress = 0;

        static FasterLog log;

        /// <summary>
        /// Main program entry point
        /// </summary>
        static void Main()
        {
            Console.WriteLine("stopwatch resolution {0}", Stopwatch.IsHighResolution);
            Console.WriteLine("stopwatch frequency {0}", Stopwatch.Frequency);
            long nanosecPerTick = (1000L * 1000L * 1000L) / Stopwatch.Frequency;
            Console.WriteLine("nanos per tick {0}", nanosecPerTick);


            var pointsSerialized = DataLoader.LoadSerializedSamplesWithTimestamp("serialized_samples_saved");
            // var pointsSerialized = SaveSerializedSamplesToFile("/home/fsolleza/data/telemetry-samples");

            var perfSources = GetPerfSourceIds(pointsSerialized);
            Console.WriteLine($"Perf source ids: {perfSources.Count}");

            // Create settings to write logs and commits at specified local path
            using var config = new FasterLogSettings("./FasterLogSample", deleteDirOnDispose: false);
            config.MemorySize = 1L << 30;
            config.PageSize = 1L << 24;

            // FasterLog will recover and resume if there is a previous commit found
            log = new FasterLog(config);

            // WriteCompressed(pointsSerialized);

            var monitor = new Thread(() => MonitorThread());
            var writer = new Thread(() => WriteReplay(pointsSerialized));

            monitor.Start();
            writer.Start();
            monitor.Join();
            writer.Join();
        }


        static void WriteReplay(List<(ulong, byte[])> pointsSerialized)
        {
            var ch = Channel.CreateBounded<byte[]>(10_000);

            var producer = new Thread(() => WriteThread(ch.Reader, pointsSerialized));
            var consumer = new Thread(() => BatchThread(ch.Writer, pointsSerialized));

            producer.Start();
            consumer.Start();
        }

        static void WriteThread(ChannelReader<byte[]> ch, List<(ulong, byte[])> pointsSerialized)
        {
            var sampleBatch = new byte[sampleBatchSize];
            var sampleBatchOffset = 0;
            var samplesBatched = 0;
            ulong prevLogicalAddress = 0;

            var compressBuf = new byte[8 /* prev block logical addr */ + LZ4Codec.MaximumOutputSize(sampleBatchSize)];

            while (true)
            {
                try
                {
                    if (ch.TryRead(out var point))
                    {
                        if (sampleBatchOffset + point.Length < sampleBatch.Length)
                        {
                            Buffer.BlockCopy(point, 0, sampleBatch, sampleBatchOffset, point.Length);
                            sampleBatchOffset += point.Length;
                            samplesBatched++;
                        }
                        else
                        {
                            var encodedLength = LZ4Codec.Encode(
                                sampleBatch, 0, sampleBatchOffset,
                                compressBuf, 8, compressBuf.Length - 8);

                            var prevAddrBytes = Serializer.UlongToBigEndianBytes(prevLogicalAddress);
                            Buffer.BlockCopy(prevAddrBytes, 0, compressBuf, 0, prevAddrBytes.Length);

                            prevLogicalAddress = (ulong)log.Enqueue(new ReadOnlySpan<byte>(compressBuf, 0, 8 + encodedLength));
                            Interlocked.Exchange(ref lastIngestAddress, prevLogicalAddress);

                            Interlocked.Add(ref samplesWritten, samplesBatched);
                            samplesBatched = 0;
                            sampleBatchOffset = 0;
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("writer exception: {0}", e);
                }
            }
        }

        static void BatchThread(ChannelWriter<byte[]> ch, List<(ulong, byte[])> pointsSerialized)
        {
            var batchSize = 10;
            var generated = 0;
            var dropped = 0;

            // var idx = 2_575_366;
            var idx = 0;
            var firstTimestamp = pointsSerialized[idx].Item1;

            Stopwatch sw = new Stopwatch();
            sw.Start();
            var lastMs = sw.ElapsedMilliseconds;
            var startNs = (ulong)Stopwatch.GetTimestamp();

            while (true)
            {
                var start = idx;
                var end = Math.Min(start + batchSize, pointsSerialized.Count());
                var endTimestamp = pointsSerialized[end - 1].Item1;
                while (end < pointsSerialized.Count() && pointsSerialized[end].Item1 == endTimestamp)
                {
                    end++;
                }

                for (var i = start; i < end; i++)
                {
                    Point.UpdateSerializedPointTimestamp(pointsSerialized[i].Item2, (ulong)Stopwatch.GetTimestamp());

                    if (ch.TryWrite(pointsSerialized[i].Item2))
                    {
                        generated++;
                    }
                    else
                    {
                        dropped++;
                    }
                }
                idx = end;
                Interlocked.Exchange(ref samplesGenerated, generated);

                if (idx == pointsSerialized.Count())
                {
                    break;
                }

                var elapsedNs = (ulong)Stopwatch.GetTimestamp() - startNs;
                var expected = pointsSerialized[idx].Item1 - firstTimestamp;

                if (expected > elapsedNs)
                {
                    while (expected > (ulong)Stopwatch.GetTimestamp() - startNs)
                    {
                    }
                }
                else
                {
                    while (pointsSerialized[idx].Item1 < elapsedNs && idx < pointsSerialized.Count())
                    {
                        idx++;
                        dropped++;
                    }
                    Interlocked.Exchange(ref samplesDropped, dropped);

                    if (idx == pointsSerialized.Count())
                    {
                        break;
                    }
                }
            }

            Console.WriteLine("DONE, dropped {0}", dropped);
            ch.Complete();
        }

        static void MonitorThread()
        {
            long lastWritten = 0;
            long lastGenerated = 0;
            long lastDropped = 0;

            Stopwatch sw = new Stopwatch();
            sw.Start();
            var lastMs = sw.ElapsedMilliseconds;

            while (true)
            {
                Thread.Sleep(1000);
                var written = Interlocked.Read(ref samplesWritten);
                var generated = Interlocked.Read(ref samplesGenerated);
                var dropped = Interlocked.Read(ref samplesDropped);
                var now = sw.ElapsedMilliseconds;

                var lastAddr = Interlocked.Read(ref lastIngestAddress);

                var genRate = (generated - lastGenerated) / ((now - lastMs) / 1000);
                var writtenRate = (written - lastWritten) / ((now - lastMs) / 1000);
                var dropRate = (dropped - lastDropped) / ((now - lastMs) / 1000);

                Console.WriteLine("gen rate: {0}, write rate: {1}, drop rate: {2}, lastaddr: {3}",
                        genRate, writtenRate, dropRate, lastAddr);

                lastMs = now;
                lastWritten = written;
                lastGenerated = generated;
                lastDropped = dropped;
            }
        }

        static void WriteCompressed(List<(ulong, byte[])> pointsSerialized)
        {
            double totalBytes = 0.0;
            double lastTotalBytes = 0;
            double uncompressedBytes = 0.0;
            double lastUncompressedBytes = 0.0;
            long lastMs = 0;
            var samplesWritten = 0;
            var lastWritten = 0;
            var durationMs = 5000;

            long compressMs = 0;
            long compressCopyMs = 0;

            var sampleBatch = new byte[sampleBatchSize];
            var sampleBatchOffset = 0;
            var compressedBatch = new byte[compressedBatchSize];
            var compressedBatchOffset = 0;

            Stopwatch sw = new();
            sw.Start();

            while (true)
            {
                foreach (var point in pointsSerialized)
                {
                    if (sampleBatchOffset + point.Item2.Length <= sampleBatch.Length)
                    {
                        Buffer.BlockCopy(point.Item2, 0, sampleBatch, sampleBatchOffset, point.Item2.Length);
                        sampleBatchOffset += point.Item2.Length;
                        samplesWritten++;
                    }
                    else
                    {
                        // compress
                        Stopwatch cpStopWatch = new();
                        cpStopWatch.Start();
                        var target = new byte[LZ4Codec.MaximumOutputSize(sampleBatch.Length) + 4];
                        var encodedLength = LZ4Codec.Encode(
                            sampleBatch, 0, sampleBatch.Length,
                            target, 4, target.Length - 4);
                        compressMs += cpStopWatch.ElapsedMilliseconds;

                        BinaryPrimitives.WriteUInt32BigEndian(target.AsSpan<byte>(), (uint)encodedLength);
                        var writeLength = 4 + encodedLength;

                        if (writeLength + compressedBatchOffset > compressedBatch.Length)
                        {
                            // compressed batch is full, write to FasterLog
                            log.Enqueue(new ReadOnlySpan<byte>(compressedBatch, 0, compressedBatchOffset));
                            compressedBatchOffset = 0;
                        }

                        uncompressedBytes += (double)sampleBatchOffset;
                        totalBytes += (double)encodedLength;

                        Buffer.BlockCopy(target, 0, compressedBatch, compressedBatchOffset, writeLength);
                        compressedBatchOffset += writeLength;
                        sampleBatchOffset = 0;

                        var now = sw.ElapsedMilliseconds;
                        if (now - lastMs > durationMs)
                        {
                            var secs = (now - lastMs) / 1000.0;
                            var sps = (samplesWritten - lastWritten) / secs;
                            var mb = (totalBytes - lastTotalBytes) / 1000000.0;
                            var mbps = mb / secs;
                            var mbUncompressed = (uncompressedBytes - lastUncompressedBytes) / 1000000.0;
                            var mbpsUncompressed = mbUncompressed / secs;

                            Console.WriteLine("In thread total mb written: {0}, sps: {1}, mbps: {2}, secs: {3}, mbps uncomp {4}, compress ms {5}, compcpy ms {6}",
                                    mb, sps, mbps, secs, mbpsUncompressed, compressMs, compressCopyMs);
                            lastMs = now;
                            lastTotalBytes = totalBytes;
                            lastUncompressedBytes = uncompressedBytes;
                            lastWritten = samplesWritten;
                            compressMs = 0;
                            compressCopyMs = 0;
                        }
                    }
                }
            }
        }

        static void WriteUncompressed(List<(ulong, byte[])> pointsSerialized)
        {
            double totalBytes = 0.0;
            double lastTotalBytes = 0;
            long lastMs = 0;
            var samplesWritten = 0;
            var lastWritten = 0;
            var durationMs = 5000;

            Stopwatch sw = new();
            sw.Start();

            while (true)
            {
                foreach (var point in pointsSerialized)
                {
                    log.Enqueue(point.Item2);
                    totalBytes += (double)point.Item2.Length;
                    samplesWritten++;

                    var now = sw.ElapsedMilliseconds;
                    if (now - lastMs > durationMs)
                    {
                        var secs = (now - lastMs) / 1000.0;
                        var sps = (samplesWritten - lastWritten) / secs;
                        var mb = (totalBytes - lastTotalBytes) / 1000000.0;
                        var mbps = mb / secs;
                        Console.WriteLine("In thread total mb written: {0}, sps: {1}, mbps: {2}, secs: {3}", mb, sps, mbps, secs);
                        lastMs = now;
                        lastTotalBytes = totalBytes;
                        lastWritten = samplesWritten;
                    }
                }
            }

        }

        static HashSet<ulong> GetPerfSourceIds(List<(ulong, byte[])> pointsSerialized)
        {
            var sourceIds = new HashSet<ulong>();

            foreach ((_, var pointBytes) in pointsSerialized)
            {
                if (Point.IsType(pointBytes, OtelType.PerfTrace))
                {
                    sourceIds.Add(Point.GetSourceIdFromSerialized(pointBytes));
                }
            }

            return sourceIds;
        }

        static void CommitThread()
        {
            //Task<LinkedCommitInfo> prevCommitTask = null;
            while (true)
            {
                Thread.Sleep(5);
                log.Commit(true);

                // Async version
                // await log.CommitAsync();

                // Async version that catches all commit failures in between
                //try
                //{
                //    prevCommitTask = await log.CommitAsync(prevCommitTask);
                //}
                //catch (CommitFailureException e)
                //{
                //    Console.WriteLine(e);
                //    prevCommitTask = e.LinkedCommitInfo.nextTcs.Task;
                //}
            }
        }

        private static bool Different(ReadOnlySpan<byte> b1, ReadOnlySpan<byte> b2)
        {
            return !b1.SequenceEqual(b2);
        }
    }
}
