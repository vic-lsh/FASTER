using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.Buffers.Binary;
using System.Threading;
using System.Collections.Generic;
using K4os.Compression.LZ4;
using System.Linq;
using FasterLogData;
using System.Collections.Concurrent;

namespace FasterLogQuerier
{
    class Request
    {
        public byte[] Payload { get; }
        public int FlowId { get; }

        public Request(byte[] payload, int id)
        {
            Payload = payload;
            FlowId = id;
        }
    }

    public class Program
    {

        static readonly string SERVER = "172.31.21.70";
        static readonly int PORT = 13000;
        static readonly int NUM_QUERIERS = 2;

        static void Main()
        {
            var conn = new QueryServerConn(SERVER, PORT, numFlows: NUM_QUERIERS + 1);
            new Thread(() => conn.Serve()).Start();

            var id = 0;

            // var firstClient = new QueryClient(id++, conn, usePerfSources: false);
            // // Thread.Sleep(120_000);
            // Thread.Sleep(20_000);
            // Console.WriteLine("low rate client begins query");
            // new Thread(() =>
            // {
            //     var sw = new Stopwatch();
            //     sw.Start();
            //     while (sw.ElapsedMilliseconds < 60_000)
            //     {
            //         firstClient.DoQuery();
            //     }
            // }).Start();

            Thread.Sleep(123_000);
            Console.WriteLine("perf clients begin query");

            var thrs = new List<Thread>();
            for (var i = 0; i < NUM_QUERIERS; i++)
            {
                var t = new Thread(() =>
                {
                    var c = new QueryClient(id++, conn, usePerfSources: true);
                    var sw = new Stopwatch();
                    sw.Start();
                    while (sw.ElapsedMilliseconds < 150_000)
                    {
                        c.DoQuery();
                    }
                });
                thrs.Add(t);
                t.Start();
            }

            foreach (var t in thrs)
            {
                t.Join();
            }
        }
    }

    class QueryServerConn : IDisposable
    {
        public HashSet<ulong> Sources;
        public HashSet<ulong> PerfSources;

        TcpClient Client;
        NetworkStream Stream;
        byte[] Buf;

        ConcurrentQueue<Request> reqs = new ConcurrentQueue<Request>();
        ConcurrentQueue<byte[]>[] responses = new ConcurrentQueue<byte[]>[11];

        public QueryServerConn(string server, int port, int numFlows)
        {
            responses = new ConcurrentQueue<byte[]>[numFlows];
            for (var i = 0; i < responses.Length; i++)
            {
                responses[i] = new ConcurrentQueue<byte[]>();
            }

            try
            {
                Client = ConnectWithRetry(server, port);
                Client.NoDelay = true;
                Console.WriteLine("Client connected");
                Stream = Client.GetStream();
                Buf = new byte[1L << 21];

                // ExperimentStart message
                var msgSize = ReadMessage(Stream, Buf);
                var expStartMsg = ExperimentStart.Decode(Buf);
                PerfSources = expStartMsg.PerfSources;
                Sources = expStartMsg.Sources;
                Console.WriteLine($"Choosing from {Sources.Count} sources and {PerfSources.Count} perf sources");
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception while starting a query server conn: {0}", e);
            }
        }

        public void Serve()
        {
            while (true)
            {
                if (reqs.TryDequeue(out var request))
                {
                    Write(Stream, request.Payload);

                    var size = ReadMessage(Stream, Buf);

                    var response = new byte[size];
                    Buffer.BlockCopy(Buf, 0, response, 0, (int)size);
                    responses[request.FlowId].Enqueue(response);
                }
            }
        }

        public void Write(Request req)
        {
            reqs.Enqueue(req);
        }

        public byte[] Read(int flowID)
        {
            while (true)
            {
                if (responses[flowID].TryDequeue(out var result))
                {
                    return result;
                }
            }
        }

        public void Dispose()
        {
            Client.Dispose();
        }

        static TcpClient ConnectWithRetry(string server, int port, int retry = 10)
        {
            while (retry > 0)
            {
                try
                {
                    TcpClient client = new TcpClient(server, port);
                    return client;
                }
                catch (Exception)
                {
                    Thread.Sleep(1000);
                    retry--;
                }
            }

            throw new Exception("Failed to connect to server");
        }

        static void Write(NetworkStream stream, byte[] bytes)
        {
            var sizeBytes = new byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(sizeBytes.AsSpan(), (uint)bytes.Length);

            stream.Write(sizeBytes, 0, 4);
            stream.Write(bytes, 0, bytes.Length);
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

    class QueryClient
    {
        static readonly ulong LOOKBACK_NS = 10_000_000_000;

        Random Rand = new Random();
        QueryServerConn Conn;
        int Id;
        HashSet<ulong> Sources;

        public QueryClient(int id, QueryServerConn conn, bool usePerfSources)
        {
            Id = id;
            Conn = conn;

            try
            {
                if (usePerfSources)
                {
                    Sources = conn.PerfSources;
                    Console.WriteLine($"Choosing from {conn.PerfSources.Count} perf sources");
                }
                else
                {
                    Sources = conn.Sources;
                    Console.WriteLine($"Choosing from {conn.Sources.Count} sources");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("QueryClientException: {0}", e);
            }
        }

        // Issue and process a query. Returning whether the client should continue
        // issue another query.
        public bool DoQuery()
        {
            var sampleCounts = 0;
            var blocksScanned = 0;
            var query = MakeNewQuery();

            Console.WriteLine($"Issuing query on source {query.SourceId} {query.MinTimestamp} {query.MaxTimestamp}");

            var startNs = Stopwatch.GetTimestamp();
            Stopwatch sw = new Stopwatch();
            sw.Start();
            while (true)
            {
                Write(query.Encode());

                var start = Stopwatch.GetTimestamp();
                var buf = Read();
                if (!BlockReply.IsServerActive(buf))
                {
                    return false;
                }
                // if (blocksScanned % 50 == 0)
                // {
                //     Console.WriteLine("Read {0} ns", Stopwatch.GetTimestamp() - start);
                // }

                // the first byte is the active byte
                var block = new Span<byte>(buf, 1, (int)buf.Length - 1);

                start = Stopwatch.GetTimestamp();
                var done = ProcessBlock(block, query, out ulong nextAddr, out int sourceSampleCount);
                // if (blocksScanned % 50 == 0)
                // {
                //     Console.WriteLine("Process {0} ns", Stopwatch.GetTimestamp() - start);
                // }
                sampleCounts += sourceSampleCount;
                blocksScanned++;
                if (done)
                {
                    break;
                }

                // ask server for this block
                query.NextBlockAddr = nextAddr;
            }

            Console.WriteLine($"start {startNs} dur {sw.ElapsedMilliseconds} ms {sampleCounts} samples {blocksScanned} blocks scanned");

            return true;
        }

        bool ProcessBlock(Span<byte> block, Query q, out ulong nextAddr, out int sourceSampleCount)
        {
            nextAddr = 0;
            sourceSampleCount = 0;

            var decmpBuffer = new byte[1L << 20];

            // first 8 bytes is the next addr
            var decodedSize = LZ4Codec.Decode(block.Slice(8), decmpBuffer.AsSpan());
            if (decodedSize < 0)
            {
                throw new Exception($"Failed to decode compressed block, block size {block.Length}, decomp buffer length {decmpBuffer.Length}");
            }

            var done = ProcessBlockSamples(new Span<byte>(decmpBuffer, 0, decodedSize), q, out sourceSampleCount);

            if (!done)
            {
                nextAddr = BinaryPrimitives.ReadUInt64BigEndian(block.Slice(0, 8));
                if (nextAddr == 0)
                {
                    return true; // done
                }
            }

            return done;
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

        Query MakeNewQuery()
        {
            var maxTs = (ulong)Stopwatch.GetTimestamp();
            var minTs = maxTs - LOOKBACK_NS;
            return new Query(ChooseNewSource(), minTs, maxTs);
        }

        ulong ChooseNewSource()
        {
            return Sources.ElementAt(Rand.Next(Sources.Count));
        }

        void Write(byte[] bytes)
        {
            Conn.Write(new Request(bytes, Id));
        }

        byte[] Read()
        {
            return Conn.Read(Id);
        }
    }
}
