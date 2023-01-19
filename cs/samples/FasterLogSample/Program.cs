// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Diagnostics;
using System.Threading;
using System.Collections.Generic;
using FASTER.core;
using Newtonsoft.Json;

namespace FasterLogSample
{
    using Value = Dictionary<string, string>;

    public class Point
    {
        public string otel_type;
        public ulong timestamp;
        public Dictionary<string, Value> attributes;
        public Value[] values;
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
            LoadSamples("/home/fsolleza/data/telemetry-samples-small");

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
                // Log writer thread: create as many as needed
                new Thread(new ThreadStart(LogWriterThread)).Start();

                // Threads for iterator scan: create as many as needed
                new Thread(() => ScanThread()).Start();

                // Threads for reporting, commit
                new Thread(new ThreadStart(ReportThread)).Start();
                var t = new Thread(new ThreadStart(CommitThread));
                t.Start();
                t.Join();
            }
        }

        static void LoadSamples(string filePath)
        {
            List<Point> points = new List<Point>();

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
                            var point = serializer.Deserialize<Point>(reader);
                            points.Add(point);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("{0}", e);
                            erred++;
                        }
                    }
                }

                Console.WriteLine("Failed to read {0} samples", erred);
            }
        }


        static void LogWriterThread()
        {
            while (true)
            {
                // TryEnqueue - can be used with throttling/back-off
                // Accepts byte[] and ReadOnlySpan<byte>
                while (!log.TryEnqueue(staticEntry, out _)) ;

                // Synchronous blocking enqueue
                // Accepts byte[] and ReadOnlySpan<byte>
                // log.Enqueue(entry);

                // Batched enqueue - batch must fit on one page
                // Add this to class:
                //   static readonly ReadOnlySpanBatch spanBatch = new ReadOnlySpanBatch(10);
                // while (!log.TryEnqueue(spanBatch, out _)) ;
            }
        }

        static void ScanThread()
        {
            byte[] result;

            while (true)
            {
                while (!iter.GetNext(out result, out _, out _))
                {
                    if (iter.Ended) return;
                    iter.WaitAsync().AsTask().GetAwaiter().GetResult();
                }

                // Memory pool variant:
                // iter.GetNext(pool, out IMemoryOwner<byte> resultMem, out int length, out long currentAddress)

                if (Different(result, staticEntry))
                    throw new Exception("Invalid entry found");

                // Example of random read from given address
                // (result, _) = log.ReadAsync(iter.CurrentAddress).GetAwaiter().GetResult();

                // Truncate until start of most recently read page
                log.TruncateUntilPageStart(iter.NextAddress);

                // Truncate log until after most recently read entry
                // log.TruncateUntil(iter.NextAddress);
            }

            // Example of recoverable (named) iterator:
            // using (iter = log.Scan(log.BeginAddress, long.MaxValue, "foo"))
        }

        static void ReportThread()
        {
            long lastTime = 0;
            long lastValue = log.TailAddress;
            long lastIterValue = log.BeginAddress;

            Stopwatch sw = new();
            sw.Start();

            while (true)
            {
                Thread.Sleep(5000);

                var nowTime = sw.ElapsedMilliseconds;
                var nowValue = log.TailAddress;

                Console.WriteLine("Append Throughput: {0} MB/sec, Tail: {1}",
                    (nowValue - lastValue) / (1000 * (nowTime - lastTime)), nowValue);
                lastValue = nowValue;

                if (iter != null)
                {
                    var nowIterValue = iter.NextAddress;
                    Console.WriteLine("Scan Throughput: {0} MB/sec, Iter pos: {1}",
                        (nowIterValue - lastIterValue) / (1000 * (nowTime - lastTime)), nowIterValue);
                    lastIterValue = nowIterValue;
                }

                lastTime = nowTime;
            }
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
