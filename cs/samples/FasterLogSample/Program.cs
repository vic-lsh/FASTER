// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Buffers.Binary;
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

            bytes.AddRange(Point.ulongToBigEndianBytes(sourceId));
            bytes.AddRange(Point.ulongToBigEndianBytes(point.timestamp));

            List<(ValueType, byte[])> serialized = new List<(ValueType, byte[])>();
            foreach (var value in point.values)
            {
                var (type, valueBytes) = Point.serializeValueEntry(value);
                //var (type, valueBytes) = ValueAll.Serialize(value);
                bytes.Add((byte)type);
                serialized.Add((type, valueBytes));
                bytes.AddRange(Enumerable.Repeat((byte)0, 4)); // offset
            }

            for (int i = 0; i < serialized.Count; i++)
            {
                var loc = bytes.Count;
                var locBytes = Point.uintToBigEndianBytes((uint)loc);
                var start = Point.getEntryOffsetLoc(i);
                for (int j = 0; j < locBytes.Length; j++)
                {
                    bytes[start + j] = locBytes[j];
                }

                var (type, valueBytes) = serialized[i];
                if (type == ValueType.String)
                {
                    bytes.AddRange(Point.uintToBigEndianBytes((uint)valueBytes.Length));
                }
                bytes.AddRange(valueBytes);

            }

            var arr = bytes.ToArray();
            //Console.WriteLine("Otel type: {0}, Serialized Size: {1}", point.otel_type,  arr.Count());
            return arr;
        }

        // TODO
        // public static Point Deserialize(byte[] bytes) { }

        private static ulong calculateSourceId(Dictionary<string, Value> attrs)
        {
            // TODO
            return 0;
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
                ValueType.Int => Point.longToBigEndianBytes(long.Parse(entry.Value)),
                ValueType.Uint => Point.ulongToBigEndianBytes(ulong.Parse(entry.Value)),
                ValueType.Float => BitConverter.GetBytes(double.Parse(entry.Value)),
                ValueType.Bool => BitConverter.GetBytes(bool.Parse(entry.Value)),
                _ => throw new Exception("Serializing null value not supported yet"),
            };

            return (valueType, valueBytes);
        }

        private static byte[] intToBigEndianBytes(int n)
        {
            var bytes = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan<byte>(), n);
            return bytes;
        }

        private static byte[] longToBigEndianBytes(long n)
        {
            var bytes = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan<byte>(), n);
            return bytes;
        }

        private static byte[] ulongToBigEndianBytes(ulong n)
        {
            var bytes = new byte[8];
            BinaryPrimitives.WriteUInt64BigEndian(bytes.AsSpan<byte>(), n);
            return bytes;
        }

        private static byte[] uintToBigEndianBytes(uint n)
        {
            var bytes = new byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(bytes.AsSpan<byte>(), n);
            return bytes;
        }

    }

    public class Program
    {
        const int sampleBatchSize = 1 << 20;
        const int compressedBatchSize = 1 << 22;

        static FasterLog log;

        /// <summary>
        /// Main program entry point
        /// </summary>
        static void Main()
        {
            var points = LoadSamples("/home/fsolleza/data/telemetry-samples");
            Console.WriteLine("Number of samples {0}", points.Count);
            var pointsSerialized = new List<byte[]>();
            ulong totalSize = 0;
            foreach (var point in points)
            {
                var p = Point.Serialize(point);
                totalSize += (ulong)p.Count();
                pointsSerialized.Add(p);
            }
            Console.WriteLine("Total bytes to write: {0}", totalSize);

            // Create settings to write logs and commits at specified local path
            using var config = new FasterLogSettings("./FasterLogSample", deleteDirOnDispose: false);
            config.MemorySize = 1L << 31;

            // FasterLog will recover and resume if there is a previous commit found
            log = new FasterLog(config);
            WriteCompressed(pointsSerialized);
        }

        static void WriteCompressed(List<byte[]> pointsSerialized)
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
                    if (sampleBatchOffset + point.Length <= sampleBatch.Length)
                    {
                        Buffer.BlockCopy(point, 0, sampleBatch, sampleBatchOffset, point.Length);
                        sampleBatchOffset += point.Length;
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

        static void WriteUncompressed(List<byte[]> pointsSerialized)
        {
            while (true)
            {
                foreach (var point in pointsSerialized)
                {
                    // log.Enqueue(point);
                    // totalBytes += (double)point.Length;
                    // samplesWritten++;

                    // var now = sw.ElapsedMilliseconds;
                    // if (now - lastMs > durationMs)
                    // {
                    //     var secs = (now - lastMs) / 1000.0;
                    //     var sps = (samplesWritten - lastWritten) / secs;
                    //     var mb = (totalBytes - lastTotalBytes) / 1000000.0;
                    //     var mbps = mb / secs;
                    //     Console.WriteLine("In thread total mb written: {0}, sps: {1}, mbps: {2}, secs: {3}", mb, sps, mbps, secs);
                    //     lastMs = now;
                    //     lastTotalBytes = totalBytes;
                    //     lastWritten = samplesWritten;
                    // }
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
                        points.Add(point);
                        if (points.Count % 100000 == 0)
                        {
                            Console.WriteLine("loaded {0} samples", points.Count);
                        }
                        // if (points.Count == 1000000)
                        if (points.Count == 500000)
                        {
                            break;
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

        static List<byte[]> SerializeAll(List<Point> points)
        {
            List<byte[]> serialized = new List<byte[]>();

            foreach (var p in points)
            {
                try
                {
                    serialized.Add(Point.Serialize(p));
                }
                catch (Exception)
                {

                }
            }

            return serialized;
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

        static void PreSerializedLogWriterThread(Barrier barr, List<byte[]> points, int start, int end)
        {
            barr.SignalAndWait();
            Stopwatch sw = new();
            double total_bytes = 0.0;
            sw.Start();
            for (var i = start; i < end; i++)
            {
                log.Enqueue(points[i]);
                total_bytes += (double)points[i].Length;
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
