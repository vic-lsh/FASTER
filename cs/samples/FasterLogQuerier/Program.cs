using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.Buffers.Binary;
using System.Threading;
using System.Collections.Generic;
using K4os.Compression.LZ4;
using System.Linq;
using FasterLogData;

namespace FasterLogQuerier
{
    public class Program
    {

        static readonly string SERVER = "127.0.0.1";
        static readonly int PORT = 13000;
        static readonly int NUM_QUERIERS = 10;

        static void Main()
        {
            // var c = new QueryClient(SERVER, PORT);

            var firstClient = new QueryClient(SERVER, PORT, usePerfSources: false);
            Thread.Sleep(150_000);
            Console.WriteLine("low rate client begins query");
            new Thread(() =>
            {
                var sw = new Stopwatch();
                sw.Start();
                while (sw.ElapsedMilliseconds < 115_000)
                {
                    firstClient.DoQuery();
                }
            }).Start();

            Thread.Sleep(115_000);
            Console.WriteLine("perf clients begin query");

            var thrs = new List<Thread>();
            for (var i = 0; i < NUM_QUERIERS; i++)
            {
                var t = new Thread(() =>
                {
                    var c = new QueryClient(SERVER, PORT, usePerfSources: true);
                    var sw = new Stopwatch();
                    sw.Start();
                    // while (sw.ElapsedMilliseconds < 70_000)
                    while (sw.ElapsedMilliseconds < 115_000)
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

    class QueryClient : IDisposable
    {
        // static readonly ulong LOOKBACK_NS = 10_000_000_000;
        static readonly ulong LOOKBACK_NS = 10_000_000_000;

        Random rand = new Random();
        HashSet<ulong> sources;
        byte[] buf;
        TcpClient client;
        NetworkStream stream;

        public QueryClient(string server, int port)
        {
            try
            {
                client = ConnectWithRetry(server, port);
                Console.WriteLine("Client connected");
                stream = client.GetStream();

                buf = new byte[1L << 21];

                var input = new byte[4];
                for (var i = 0; i < input.Length; i++)
                {
                    input[i] = 0;
                }

                var itr = 0;
                while (true)
                {
                    var start = Stopwatch.GetTimestamp();
                    // Write(stream, input);
                    stream.Write(input, 0, 4);

                    ReadMessage(stream, buf);
                    // ReadNBytes(stream, buf, 165000);
                    var end = Stopwatch.GetTimestamp();
                    if (itr % 20 == 0)
                    {
                        Console.WriteLine("{0}", end - start);
                    }
                    itr++;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("QueryClientException: {0}", e);
            }

        }

        public QueryClient(string server, int port, bool usePerfSources)
        {
            try
            {
                client = ConnectWithRetry(server, port);
                client.NoDelay = true;
                Console.WriteLine("Client connected");
                stream = client.GetStream();

                buf = new byte[1L << 21];

                // ExperimentStart message
                var msgSize = ReadMessage(stream, buf);
                var expStartMsg = ExperimentStart.Decode(buf);
                if (usePerfSources)
                {
                    sources = expStartMsg.PerfSources;
                    Console.WriteLine($"Choosing from {expStartMsg.PerfSources.Count} perf sources");
                }
                else
                {
                    sources = expStartMsg.Sources;
                    Console.WriteLine($"Choosing from {expStartMsg.Sources.Count} sources");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("QueryClientException: {0}", e);
            }
        }

        public void Dispose()
        {
            client.Dispose();
        }

        // Issue and process a query. Returning whether the client should continue
        // issue another query.
        public bool DoQuery()
        {
            return DoQueryInternal();
            //             try
            //             {
            //                 return DoQueryInternal();
            //             }
            //             catch (Exception e)
            //             {
            //                 Console.WriteLine($"Query exception: {e}");
            //                 return true;
            //             }
        }

        bool DoQueryInternal()
        {
            var sampleCounts = 0;
            var blocksScanned = 0;
            var query = MakeNewQuery();

            Console.WriteLine($"Issuing query on source {query.SourceId} {query.MinTimestamp} {query.MaxTimestamp}");

            Stopwatch sw = new Stopwatch();
            sw.Start();
            while (true)
            {
                Write(stream, query.Encode());

                var start = Stopwatch.GetTimestamp();
                var msgSize = ReadMessage(stream, buf);
                if (!BlockReply.IsServerActive(buf))
                {
                    return false;
                }
                // if (blocksScanned % 50 == 0)
                // {
                //     Console.WriteLine("Read {0} ns", Stopwatch.GetTimestamp() - start);
                // }

                // the first byte is the active byte
                var block = new Span<byte>(buf, 1, (int)msgSize - 1);

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
                // if (blocksScanned % 10 == 0)
                // {
                //     Console.WriteLine($"processing block {blocksScanned}, next addr {nextAddr}");
                // }
            }

            Console.WriteLine($"Query done, {sw.ElapsedMilliseconds} ms {sampleCounts} samples {blocksScanned} blocks scanned");

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
            return sources.ElementAt(rand.Next(sources.Count));
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
    }
}
