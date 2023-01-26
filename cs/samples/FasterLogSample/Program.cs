// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Threading;
using System.Collections.Generic;
using FASTER.core;
using System.Linq;
using Newtonsoft.Json;
using MessagePack;

namespace FasterLogSample
{
    using Value = Dictionary<string, string>;

    public enum OtelType : byte
    {
        Metric,
        Log,
        Span
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
                _ => throw new Exception("invalid OtelType string"),
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

    [MessagePackObject]
    public class Point
    {

        // [Key(0)]
        [IgnoreMember]
        public string otel_type;
        [Key(0)]
        public ulong timestamp;
        // [Key(2)]
        [IgnoreMember]
        public Dictionary<string, Value> attributes;
        [Key(1)]
        public Value[] values;

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

            return bytes.ToArray();
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
                ValueType.Int => Point.intToBigEndianBytes(Int32.Parse(entry.Value)),
                ValueType.Uint => Point.uintToBigEndianBytes(UInt32.Parse(entry.Value)),
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

        private static byte[] uintToBigEndianBytes(uint n)
        {
            var bytes = new byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(bytes.AsSpan<byte>(), n);
            return bytes;
        }

        private static byte[] ulongToBigEndianBytes(ulong ul)
        {
            var bytes = new byte[8];
            BinaryPrimitives.WriteUInt64BigEndian(bytes.AsSpan<byte>(), ul);
            return bytes;
        }

    }

    public class Program
    {
        // Entry length can be between 1 and ((1 << FasterLogSettings.PageSizeBits) - 4)
        const int entryLength = 1 << 10;
        static readonly byte[] staticEntry = new byte[entryLength];
        static FasterLog log;
        static FasterLogScanIterator iter;

        /// <summary>
        /// Main program entry point
        /// </summary>
        static void Main()
        {
            var points = LoadSamples("/home/fsolleza/data/telemetry-samples");
            Console.WriteLine("Number of samples {}", points.Count);

            // Populate entry being inserted
            for (int i = 0; i < entryLength; i++)
            {
                staticEntry[i] = (byte)i;
            }

            // Create settings to write logs and commits at specified local path
            using var config = new FasterLogSettings("./FasterLogSample", deleteDirOnDispose: true);

            // FasterLog will recover and resume if there is a previous commit found
            log = new FasterLog(config);

            using (iter = log.Scan(log.BeginAddress, long.MaxValue))
            {
                var numThreads = 1;
                while (numThreads <= 16)
                {
                    Barrier barr = new Barrier(numThreads + 1);
                    List<Thread> threads = new List<Thread>();
                    int elemsPerThread = (points.Count - 1) / (numThreads + 1);
                    for (int i = 0; i < numThreads; i++)
                    {
                        int start = i * elemsPerThread;
                        int end = Math.Min(start + elemsPerThread, points.Count);
                        var w = new Thread(new ThreadStart(() => LogWriterThread(barr, points, start, end)));
                        w.Start();
                        threads.Add(w);
                    }
                    barr.SignalAndWait();
                    Stopwatch sw = new();
                    sw.Start();

                    barr.SignalAndWait();
                    var elapsed = sw.ElapsedMilliseconds;

                    foreach (var t in threads)
                    {
                        t.Join();
                    }
                    var throughput = 1000 * (ulong)points.Count / (ulong)elapsed;
                    Console.WriteLine("Throughput for {0} threads: {1} samples/sec", numThreads, throughput);

                    numThreads = numThreads * 2;
                }

            }
        }

        static List<Point> LoadSamples(string filePath)
        {
            List<Point> points = new List<Point>();
            var rand = new Random();

            using (StreamReader sr = new StreamReader(filePath))
            using (JsonTextReader reader = new JsonTextReader(sr)
            {
                SupportMultipleContent = true
            })
            {
                var serializer = JsonSerializer.CreateDefault(new JsonSerializerSettings
                {
                    NullValueHandling = NullValueHandling.Ignore
                });

                var erred = 0;
                while (reader.Read())
                {
                    if (reader.TokenType == JsonToken.StartObject)
                    {
                        try
                        {
                            var shouldInclude = rand.NextDouble() < 0.05;
                            if (shouldInclude)
                            {
                                var point = serializer.Deserialize<Point>(reader);
                                points.Add(point);
                                if (points.Count % 100000 == 0)
                                {
                                    Console.WriteLine("Loaded {0} samples", points.Count);
                                }
                            }
                        }
                        catch (Exception)
                        {
                            // Console.WriteLine("{0}", e);
                            erred++;
                        }
                    }
                }

                Console.WriteLine("Failed to read {0} samples", erred);
            }

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

        private struct ReadOnlySpanBatch : IReadOnlySpanBatch
        {
            private readonly int batchSize;
            public ReadOnlySpanBatch(int batchSize) => this.batchSize = batchSize;
            public ReadOnlySpan<byte> Get(int index) => staticEntry;
            public int TotalEntries() => batchSize;
        }
    }
}
