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
using MessagePack;

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

    }

    //[MessagePackObject]
    public class Point
    {
        // [Key(0)]
        //[IgnoreMember]
        public string otel_type { get; set; }
        //[Key(0)]
        public ulong timestamp { get; set; }
        // [Key(2)]
        //[IgnoreMember]
        public Dictionary<string, Value> attributes; // { get; set; }
        //[Key(1)]
        public Value[] values { get; set; }

        public static byte[] Serialize(Point point)
        {
            List<byte> bytes = new List<byte>();

            var otelType = point.otel_type.ToOtelType();
            var sourceId = Point.calculateSourceId(point.attributes);

            bytes.Add(point.calcHeaderSizeAsByte());
            bytes.Add((byte)otelType);

            bytes.AddRange(Serializer.UlongToBigEndianBytes(sourceId));
            bytes.AddRange(Serializer.UlongToBigEndianBytes(point.timestamp));

            List<(ValueType, byte[])> serialized = new List<(ValueType, byte[])>();
            foreach (var value in point.values)
            {
                var (type, valueBytes) = Point.serializeValueEntry(value);
                bytes.Add((byte)type);
                serialized.Add((type, valueBytes));
                bytes.AddRange(Enumerable.Repeat((byte)0, 4)); // offset
            }

            for (int i = 0; i < serialized.Count; i++)
            {
                var loc = bytes.Count;
                var locBytes = Serializer.UintToBigEndianBytes((uint)loc);
                var start = Point.getEntryOffsetLoc(i);
                for (int j = 0; j < locBytes.Length; j++)
                {
                    bytes[start + j] = locBytes[j];
                }

                var (type, valueBytes) = serialized[i];
                if (type == ValueType.String)
                {
                    bytes.AddRange(Serializer.UintToBigEndianBytes((uint)valueBytes.Length));
                }
                bytes.AddRange(valueBytes);

            }

            var arr = bytes.ToArray();
            return arr;
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

        public ulong GetSourceIdFromSerialized(byte[] serialized)
        {
            var sourceIdOffset = getSourceIdOffset();
            if (serialized.Length < sourceIdOffset + 8)
            {
                throw new Exception("Corrupted serialized sample: sample does not contain a source ID");
            }

            var id = BinaryPrimitives.ReadUInt64BigEndian(new Span<byte>(serialized, sourceIdOffset, 8));
            return id;
        }

        private static int getSourceIdOffset()
        {
            return 1 /* header size */
                + 1 /* otel type */;
            // source id is next
        }

        public ulong GetTimestampFromSerialized(byte[] serialized)
        {
            var tsOffset = getTimestampOffset();
            if (serialized.Length < tsOffset + 8)
            {
                throw new Exception("Corrupted serialized sample: sample does not contain a timestamp");
            }

            var ts = BinaryPrimitives.ReadUInt64BigEndian(new Span<byte>(serialized, tsOffset, 8));
            return ts;
        }

        private static int getTimestampOffset()
        {
            return 1 /* header size */
                + 1 /* otel type */
                + 8 /* source id */;
            // timestamp is next
        }

        private byte calcHeaderSizeAsByte()
        {
            var sz = 0;
            sz += 1; // header size
            sz += 1; // otel type
            sz += 8; // source id
            sz += 8; // timestamp
            sz += 5 * values.Length; // value (type, offset)s

            if (sz > 255)
            {
                throw new Exception("Point too large");
            }

            return (byte)sz;
        }

        private static int getEntryOffsetLoc(int index)
        {
            return 1 /* header size */
                + 1 /* otel type */
                + 8 /* source id */
                + 8 /* timestamp */
                + index * 5 /* prev entries */
                + 1 /* this entry's type */;
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

    public class Program
    {
        const int sampleBatchSize = 1 << 20;
        const int compressedBatchSize = 1 << 22;

        const double CyclesPerNanosecond = 2.7;

        static long samplesGenerated = 0;
        static long samplesDropped = 0;
        static long samplesWritten = 0;

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


            var pointsSerialized = LoadSerializedSamplesWithTimestamp("serialized_samples_saved");
            // var pointsSerialized = SaveSerializedSamplesToFile("/home/fsolleza/data/telemetry-samples");

            // Create settings to write logs and commits at specified local path
            using var config = new FasterLogSettings("./FasterLogSample", deleteDirOnDispose: false);
            config.MemorySize = 1L << 30;
            config.PageSize = 1L << 24;

            // FasterLog will recover and resume if there is a previous commit found
            log = new FasterLog(config);

            // WriteCompressed(pointsSerialized);

            new Thread(() => MonitorThread()).Start();
            WriteReplay(pointsSerialized);
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

            var compressBuf = new byte[LZ4Codec.MaximumOutputSize(sampleBatchSize)];

            while (true)
            {
                try
                {
                    if (ch.TryRead(out var point))
                    {
                        // log.Enqueue(point);
                        // samplesBatched++;
                        // if (samplesBatched == 1000)
                        // {
                        //     Interlocked.Add(ref samplesWritten, samplesBatched);
                        //     samplesBatched = 0;
                        // }

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
                                compressBuf, 4, compressBuf.Length - 4);
                            log.Enqueue(new ReadOnlySpan<byte>(compressBuf, 0, encodedLength));
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

                var genRate = (generated - lastGenerated) / ((now - lastMs) / 1000);
                var writtenRate = (written - lastWritten) / ((now - lastMs) / 1000);
                var dropRate = (dropped - lastDropped) / ((now - lastMs) / 1000);

                Console.WriteLine("gen rate: {0}, write rate: {1}, drop rate: {2}",
                        genRate, writtenRate, dropRate);

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

        static List<Point> LoadSamples(string filePath)
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

        static List<byte[]> LoadSerializedSamples(string filePath)
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
                            // if (points.Count == 1000000)
                            // {
                            //     break;
                            // }
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

        static List<(ulong, byte[])> LoadSerializedSamplesWithTimestamp(string filePath)
        {
            var points = new List<(ulong, byte[])>();
            var ulongBytes = new byte[8];

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

                    if (points.Count % 1000000 == 0)
                    {
                        Console.WriteLine("loaded {0} samples", points.Count);
                    }
                    // if (points.Count >= 2_000_000)
                    // {
                    //     break;
                    // }

                }
            }

            Console.WriteLine("Read {0} samples", points.Count);

            return points;
        }

        static List<(ulong, byte[])> SaveSerializedSamplesToFile(string filePath)
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
                        // Console.WriteLine("sample {0} delta {1} delta cycles {2}", numRead, point.timestamp - firstTimestamp, deltaInCycles);
                        var sp = Point.Serialize(point);
                        // points.Add((point.timestamp - firstTimestamp, sp));

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
                        // if (numRead == 1_000_000)
                        // {
                        //     break;
                        // }
                    }
                    catch (Exception)
                    {
                        Console.WriteLine("Failed to parse {0}", line);
                        erred++;
                    }
                }
            }

            Console.WriteLine("Failed to read {0} samples", erred);

            return points;
        }


        static List<byte[]> SerializeAll(List<Point> points)
        {
            var pointsSerialized = new List<byte[]>();
            ulong totalSize = 0;
            foreach (var point in points)
            {
                var p = Point.Serialize(point);
                totalSize += (ulong)p.Count();
                pointsSerialized.Add(p);
            }
            Console.WriteLine("Total bytes to write: {0}", totalSize);
            return pointsSerialized;
        }

        static List<(ulong, byte[])> SerializeAllWithTimestamps(List<Point> points)
        {
            var firstTimestamp = points[0].timestamp;
            var pointsSerialized = new List<(ulong, byte[])>();
            ulong totalSize = 0;
            foreach (var point in points)
            {
                var ts = point.timestamp;
                var deltaInCycles = nanosToCycles(ts - firstTimestamp);
                var p = Point.Serialize(point);
                totalSize += (ulong)p.Count();
                pointsSerialized.Add((deltaInCycles, p));
            }
            Console.WriteLine("Total bytes to write: {0}", totalSize);
            return pointsSerialized;
        }

        static ulong cyclesToNanos(ulong cycles)
        {
            return (ulong)((double)(cycles) / CyclesPerNanosecond);
        }

        static ulong nanosToCycles(ulong nanos)
        {
            return (ulong)((double)nanos * CyclesPerNanosecond);
        }

        static void LogWriterThread(Barrier barr, List<Point> points, int start, int end)
        {
            barr.SignalAndWait();
            for (var i = start; i < end; i++)
            {
                log.Enqueue(MessagePackSerializer.Serialize(points[i]));
            }
            barr.SignalAndWait();
        }

        static void PreSerializedLogWriterThread(Barrier barr, List<(ulong, byte[])> points, int start, int end)
        {
            barr.SignalAndWait();
            Stopwatch sw = new();
            double total_bytes = 0.0;
            sw.Start();
            for (var i = start; i < end; i++)
            {
                log.Enqueue(points[i].Item2);
                total_bytes += (double)points[i].Item2.Length;
            }
            var secs = sw.ElapsedMilliseconds / 1000.0;
            var sps = points.Count / secs;
            var mb = total_bytes / 1000000.0;
            var mbps = mb / secs;
            Console.WriteLine("In thread total mb written: {0}, sps: {1}, mbps: {2}", mb, sps, mbps);
            barr.SignalAndWait();
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
