// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Net.Sockets;
using System.Buffers;
using System.Threading.Channels;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Threading;
using System.Collections.Generic;
using K4os.Compression.LZ4;
using FASTER.core;
using System.Linq;
using FasterLogQuerier;
using FasterLogData;

namespace FasterLogSample
{
    public class Program
    {
        const int sampleBatchSize = 1 << 20;
        const int compressedBatchSize = 1 << 22;

        const double CyclesPerNanosecond = 2.7;

        static long samplesGenerated = 0;
        static long chanFullDropped = 0;
        static long timeLagDropped = 0;

        static long samplesWritten = 0;
        static ulong lastIngestAddress = 0;

        static long dataReady = 0;
        static long completed = 0;

        // static readonly int TRACING_START_INDEX = 2_575_366;
        // static readonly int QUERY_START_INDEX = 32_912_623;

        static FasterLog log;

        static HashSet<ulong> allSources;
        static HashSet<ulong> perfSources;

        /// <summary>
        /// Main program entry point
        /// </summary>
        static void Main()
        {
            var queryServer = new Thread(() => QueryServer());
            queryServer.Start();

            var pointsSerialized = DataLoader.LoadSerializedSamplesWithTimestamp("data/serialized_samples");
            // var pointsSerialized = DataLoader.SaveSerializedSamplesToFile("/home/fsolleza/data/telemetry-samples");

            (allSources, perfSources) = GetSourceIds(pointsSerialized);
            Console.WriteLine($"Perf source ids: {perfSources.Count}");


            // Create settings to write logs and commits at specified local path
            using var config = new FasterLogSettings("./FasterLogSample", deleteDirOnDispose: false);
            config.MemorySize = 1L << 30;
            config.PageSize = 1L << 24;
            config.AutoCommit = true;

            // FasterLog will recover and resume if there is a previous commit found
            log = new FasterLog(config);

            var monitor = new Thread(() => MonitorThread());
            var writer = new Thread(() => WriteReplay(pointsSerialized));
            monitor.Start();
            writer.Start();

            monitor.Join();
            writer.Join();
            queryServer.Join();
        }

        static void RewriteTimestamps(List<(ulong, byte[])> pointsSerialized, ulong baseTs)
        {
            foreach (var (delta, point) in pointsSerialized)
            {
                Point.UpdateSerializedPointTimestamp(point, baseTs + delta);
            }
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

            var writeStart = Stopwatch.GetTimestamp();

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

                            // var enqStart = Stopwatch.GetTimestamp();
                            prevLogicalAddress = (ulong)log.Enqueue(new ReadOnlySpan<byte>(compressBuf, 0, 8 + encodedLength));
                            // var end = Stopwatch.GetTimestamp();
                            // enqueueTime.Add((end - writeStart, end - enqStart));

                            Interlocked.Exchange(ref lastIngestAddress, prevLogicalAddress);

                            Interlocked.Add(ref samplesWritten, samplesBatched);
                            samplesBatched = 0;
                            sampleBatchOffset = 0;
                        }
                    }
                    else
                    {
                        if (ch.Count == 0 && Interlocked.Read(ref completed) == 1)
                            break;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("writer exception: {0}", e);
                }
            }
        }

        static void BatchWithTimeLimit(ChannelWriter<byte[]> ch, List<(ulong, byte[])> pointsSerialized, int durationMs)
        {
            var batchSize = 1024;
            var generated = 0;
            var chDropped = 0;
            var lagDropped = 0;

            // var idx = 2_575_366;
            var idx = 0;
            var firstTimestamp = pointsSerialized[idx].Item1;

            Stopwatch sw = new Stopwatch();
            sw.Start();
            var lastMs = sw.ElapsedMilliseconds;
            var startNs = (ulong)Stopwatch.GetTimestamp();

            while (sw.ElapsedMilliseconds < durationMs)
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
                        chDropped++;
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
                        lagDropped++;
                    }
                    Interlocked.Exchange(ref timeLagDropped, lagDropped);
                    Interlocked.Exchange(ref chanFullDropped, chDropped);
                }
            }
        }

        static void BatchThread(ChannelWriter<byte[]> ch, List<(ulong, byte[])> pointsSerialized)
        {
            Console.WriteLine("WARMUP BEGINS");
            BatchWithTimeLimit(ch, pointsSerialized, 120_000);
            Console.WriteLine("WARMUP ENDS");

            GC.Collect();
            GC.WaitForPendingFinalizers();

            long DELAY_NS = 1_000_000_000L * 20;
            var baseTs = (ulong)(Stopwatch.GetTimestamp() + DELAY_NS);

            RewriteTimestamps(pointsSerialized, baseTs);
            if (Stopwatch.GetTimestamp() > (long)baseTs)
            {
                throw new Exception("Bad replay base timestamp: timestamp rewriting took longer than expected");
            }
            while (Stopwatch.GetTimestamp() < (long)baseTs) { }

            Interlocked.Exchange(ref dataReady, 1);

            Interlocked.Exchange(ref samplesWritten, 0);
            Interlocked.Exchange(ref samplesGenerated, 0);
            Interlocked.Exchange(ref chanFullDropped, 0);
            Interlocked.Exchange(ref timeLagDropped, 0);

            var batchSize = 1024;
            var generated = 0;
            var chDropped = 0;
            var lagDropped = 0;

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
                        chDropped++;
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
                        lagDropped++;
                    }
                    Interlocked.Exchange(ref timeLagDropped, lagDropped);
                    Interlocked.Exchange(ref chanFullDropped, chDropped);

                    if (idx == pointsSerialized.Count())
                    {
                        break;
                    }
                }
            }

            Console.WriteLine("DONE, dropped {0}", lagDropped + chDropped);
            ch.Complete();
            Interlocked.Exchange(ref completed, 1);
        }

        static void MonitorThread()
        {
            long lastWritten = 0;
            long lastGenerated = 0;
            long lastChanDropped = 0;
            long lastLagDropped = 0;

            Stopwatch sw = new Stopwatch();
            sw.Start();
            var lastMs = sw.ElapsedMilliseconds;

            while (Interlocked.Read(ref completed) == 0)
            {
                Thread.Sleep(1000);
                var written = Interlocked.Read(ref samplesWritten);
                var generated = Interlocked.Read(ref samplesGenerated);
                var chDrop = Interlocked.Read(ref chanFullDropped);
                var lagDrop = Interlocked.Read(ref timeLagDropped);
                var now = sw.ElapsedMilliseconds;

                var genRate = (generated - lastGenerated) / ((now - lastMs) / 1000);
                var writtenRate = (written - lastWritten) / ((now - lastMs) / 1000);
                var chanDropRate = (chDrop - lastChanDropped) / ((now - lastMs) / 1000);
                var lagDropRate = (lagDrop - lastLagDropped) / ((now - lastMs) / 1000);

                Console.WriteLine("gen rate: {0}, write rate: {1}, chan drop rate: {2}, lag drop rate: {3}",
                        genRate, writtenRate, chanDropRate, lagDropRate);

                lastMs = now;
                lastWritten = written;
                lastGenerated = generated;
                lastChanDropped = chDrop;
                lastLagDropped = lagDrop;
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

        static (HashSet<ulong>, HashSet<ulong>) GetSourceIds(List<(ulong, byte[])> pointsSerialized)
        {
            var perfSourceIds = new HashSet<ulong>();
            var sourceIds = new HashSet<ulong>();

            foreach ((_, var pointBytes) in pointsSerialized)
            {
                var id = Point.GetSourceIdFromSerialized(pointBytes);
                sourceIds.Add(id);
                if (Point.IsType(pointBytes, OtelType.PerfTrace))
                {
                    perfSourceIds.Add(id);
                }
            }

            return (sourceIds, perfSourceIds);
        }

        static void QueryThread(HashSet<ulong> sources)
        {
            Random random = new Random();
            ulong queryDurNs = 10_000_000_000;

            Thread.Sleep(245 * 1_000);
            // Thread.Sleep(15 * 1_000);

            Console.WriteLine("QUERY BEGINS");

            var decompressBuf = new byte[sampleBatchSize];

            while (Interlocked.Read(ref completed) == 0)
            {
                var source = sources.ElementAt(random.Next(sources.Count));
                var maxTs = (ulong)Stopwatch.GetTimestamp();
                var minTs = maxTs - queryDurNs;

                try
                {
                    Stopwatch sw = new Stopwatch();
                    sw.Start();

                    var samplesFound = ProcessQuery(new Query(source, minTs, maxTs), decompressBuf, out int blocksScanned);

                    Console.WriteLine($"Query for ID {source} took {sw.ElapsedMilliseconds} ms, found {samplesFound} samples, scanned {blocksScanned} blocks");
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Query exception {e}");
                }
            }
        }

        static int ProcessQuery(Query q, byte[] decmpBuffer, out int blocksScanned)
        {
            blocksScanned = 0;
            var sourceSampleCount = 0;

            // wait until data within the queried timerange is available
            var succeeded = PollUntilMinTimestamp(q.SourceId, q.MaxTimestamp, decmpBuffer, out ulong reverseScanStartAddr);

            var currAddr = reverseScanStartAddr;
            while (true)
            {
                var (block, length) = log.ReadAsync((long)currAddr, MemoryPool<byte>.Shared).GetAwaiter().GetResult();
                if (block == null)
                {
                    throw new Exception($"read null block at {currAddr}, committed until {log.CommittedUntilAddress}");
                }
                blocksScanned++;
                var blockSpan = block.Memory.Span.Slice(0, length);

                var decodedSize = LZ4Codec.Decode(blockSpan.Slice(8), decmpBuffer.AsSpan());
                if (decodedSize < 0)
                {
                    throw new Exception($"Failed to decode compressed block, block size {blockSpan.Length}, decomp buffer length {decmpBuffer.Length}");
                }

                var done = ProcessBlockSamples(new Span<byte>(decmpBuffer, 0, decodedSize), q, out int count);
                sourceSampleCount += count;

                if (done)
                {
                    break;
                }

                var next = BinaryPrimitives.ReadUInt64BigEndian(blockSpan.Slice(0, 8));
                // Console.WriteLine($"blocks scanned {blocksScanned}, curr {currAddr}, next {next}");

                if (next == currAddr || next == 0)
                {
                    break;
                }
                if (next > currAddr)
                {
                    throw new Exception($"next block to scan has a higher address {next} than curr address {currAddr}");
                }
                currAddr = next;
                block.Dispose();
            }

            return sourceSampleCount;
        }

        // Queries the block by scanning, returning whether the query is done.
        static bool ProcessBlockSamples(Span<byte> sampleBytes, Query q, out int sourceSampleCount)
        {
            sourceSampleCount = 0;
            var done = false;
            uint sampleOffset = 0;
            while (true)
            {
                if (sampleOffset >= sampleBytes.Length)
                {
                    return done;
                }

                var currSample = sampleBytes.Slice((int)sampleOffset);
                var ts = Point.GetTimestampFromSerialized(currSample);
                if (ts < q.MinTimestamp)
                {
                    //  this is the last block that needs to be queried
                    done = true;
                }
                if (Point.GetSourceIdFromSerialized(currSample) == q.SourceId)
                {
                    sourceSampleCount++;
                }
                sampleOffset += Point.GetSampleSize(currSample);
            }
        }

        public static void QueryServer()
        {
            TcpListener server = null;

            try
            {
                Int32 port = 13000;
                IPAddress localAddr = IPAddress.Parse("127.0.0.1");

                server = new TcpListener(localAddr, port);
                server.Start();
                Console.WriteLine("Query server started");

                while (true)
                {
                    var conn = server.AcceptTcpClient();
                    conn.NoDelay = true;
                    new Thread(() => HandleConn(conn)).Start();
                }
            }
            catch (SocketException e)
            {
                Console.WriteLine("SocketException: {0}", e);
            }
            finally
            {
                server.Stop();
            }
        }

        static void HandleConn(TcpClient client)
        {
            byte[] decodeBuf = new byte[1L << 20];
            var decompressBuf = new byte[sampleBatchSize];
            using (client)
            {
                NetworkStream stream = client.GetStream();

                while (Interlocked.Read(ref dataReady) == 0)
                {
                    Thread.Sleep(10);
                }

                var expStart = new ExperimentStart();
                expStart.PerfSources = perfSources;
                expStart.Sources = allSources;
                var expStartBytes = expStart.Encode();
                Write(stream, expStartBytes);

                while (true)
                {
                    uint msgSize = 0;
                    try
                    {
                        msgSize = ReadMessage(stream, decodeBuf);
                    }
                    catch (Exception)
                    {
                        break;
                    }
                    var query = Query.Decode(new Span<byte>(decodeBuf, 0, (int)msgSize));
                    HandleQuery(stream, query, decompressBuf);
                }
                Console.WriteLine("Query serving thread: experiment completed");
            }
        }

        static void HandleQuery(NetworkStream stream, Query q, byte[] buf)
        {
            if (q.IsNewQuery())
            {
                // Console.WriteLine($"new query {q.SourceId} {q.MinTimestamp} {q.MaxTimestamp} {q.NextBlockAddr}");
                HandleNewQuery(stream, q, buf);
            }
            else
            {
                ReadBlockAndReply(stream, q.NextBlockAddr, buf);
            }
        }

        static void HandleNewQuery(NetworkStream stream, Query q, byte[] buf)
        {
            var sw = new Stopwatch();
            sw.Start();
            var succeeded = PollUntilMinTimestamp(q.SourceId, q.MinTimestamp, buf, out ulong reverseScanStartAddr);
            // Console.WriteLine($"poll took {sw.ElapsedMilliseconds} ms");

            if (!succeeded)
            {
                Console.WriteLine("Poll failed");
                var reply = BlockReply.InactiveReply();
                Write(stream, reply.Encode());
                return;
            }
            ReadBlockAndReply(stream, reverseScanStartAddr, buf);
        }

        static void ReadBlockAndReply(NetworkStream stream, ulong addr, byte[] buf)
        {
            // Console.WriteLine($"Going to read at {addr}");
            var (block, length) = log.ReadAsync((long)addr, MemoryPool<byte>.Shared).GetAwaiter().GetResult();
            if (block == null || length == 0)
            {
                throw new Exception($"read null block at {addr}, committed until {log.CommittedUntilAddress}");
            }
            var blockSpan = block.Memory.Span.Slice(0, length);

            // construct payload
            buf[0] = 1; // active byte
            blockSpan.CopyTo(new Span<byte>(buf, 1, blockSpan.Length));

            Write(stream, new Span<byte>(buf, 0, blockSpan.Length + 1));
            block.Dispose();
        }

        static bool PollUntilMinTimestamp(ulong source, ulong timestamp, byte[] decompressed, out ulong address)
        {
            address = 0;
            ulong lastAddr = 0;
            ulong currAddr = 0;
            var repeats = 3;

            // while (repeats > 0)
            while (true)
            {
                // read distinct block addr
                while (true)
                {
                    currAddr = Interlocked.Read(ref lastIngestAddress);
                    if (currAddr != lastAddr)
                    {
                        lastAddr = currAddr;
                        break;
                    }
                    Thread.Sleep(10);
                }

                // Console.WriteLine($"poll scanning {currAddr}");

                Span<byte> blockSpan;
                IMemoryOwner<byte> block;
                int length;
                while (true)
                {
                    (block, length) = log.ReadAsync((long)currAddr, MemoryPool<byte>.Shared).GetAwaiter().GetResult();
                    if (block != null && length != 0)
                    {
                        // Console.WriteLine("got nonnull, breaking");
                        blockSpan = block.Memory.Span.Slice(0, length);
                        break;
                    }
                    Thread.Sleep(10);
                    // Console.WriteLine($"Addr {currAddr} is null, latest {log.TailAddress}, commited until {log.CommittedUntilAddress}, begin {log.BeginAddress}");
                }

                var decodedSize = LZ4Codec.Decode(blockSpan.Slice(8), decompressed.AsSpan());
                if (decodedSize < 0)
                {
                    throw new Exception($"Failed to decode compressed block, block span size {blockSpan.Length}");
                }

                // scan until we find a sample that is later than our target timestamp
                uint sampleOffset = 0;
                while (true)
                {
                    // Console.WriteLine($"offset {sampleOffset}, len {block.Length}");
                    if (sampleOffset >= blockSpan.Length)
                    {
                        // finished scanning the block
                        break;
                    }

                    var currSample = blockSpan.Slice((int)sampleOffset);
                    // if (Point.GetSourceIdFromSerialized(currSample) == source)
                    // {
                    var ts = Point.GetTimestampFromSerialized(currSample);
                    if (ts > timestamp)
                    {
                        // Console.WriteLine($"ts {ts} is greater than ts {timestamp} at offset {sampleOffset}");
                        // samples have monotonically increasing timestamp
                        address = currAddr;
                        return true;
                    }
                    // }

                    var sampleSize = Point.GetSampleSize(currSample);
                    if (sampleSize == 0)
                    {
                        throw new Exception("Sample size cannot be zero");
                    }
                    sampleOffset += sampleSize;
                }

                block.Dispose();
                repeats--;
            }

            // return false;
        }

        private static byte[] ReadBlockAt(ulong address)
        {
            var ctr = 0;
            using (var iter = log.Scan((long)address, long.MaxValue, scanUncommitted: true))
            {
                byte[] buffer;
                int bufLen;
                while (!iter.GetNext(out buffer, out bufLen, out _, out _))
                {
                    Thread.Sleep(10);
                    // iter.WaitAsync().GetAwaiter().GetResult();
                    Console.WriteLine($"waiting {ctr}");
                    ctr++;
                }

                if (buffer.Length != bufLen)
                {
                    Console.WriteLine($"warn: buffer len is {buffer.Length} but buflen is {bufLen}");
                }

                return buffer;
            }
        }

        static void Write(NetworkStream stream, byte[] bytes)
        {
            Write(stream, bytes.AsSpan());
        }

        static void Write(NetworkStream stream, ReadOnlySpan<byte> bytes)
        {
            var sizeBytes = new byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(sizeBytes.AsSpan(), (uint)bytes.Length);

            stream.Write(sizeBytes, 0, 4);
            stream.Write(bytes);
        }

        static uint ReadMessage(NetworkStream stream, byte[] bytes)
        {
            ReadNBytes(stream, bytes, 4);
            var size = BinaryPrimitives.ReadUInt32BigEndian(new Span<byte>(bytes, 0, 4));
            ReadNBytes(stream, bytes, (int)size);
            return size;
        }

        static void ReadNBytes(NetworkStream stream, byte[] bytes, int size)
        {
            if (bytes.Length < size)
            {
                throw new Exception($"Buffer is too small: buffer size {bytes.Length} requested size {size}");
            }

            var start = 0;
            var remaining = size;
            while (remaining > 0)
            {
                var nread = stream.Read(bytes, start, remaining);
                start += nread;
                remaining -= nread;
                if (remaining > 0 && nread == 0)
                {
                    throw new Exception($"Stream terminated before reading the full message: requested {size}, {remaining} not received");
                }
            }
        }
    }
}
