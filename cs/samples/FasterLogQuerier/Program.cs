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
            for (var i = 0; i < NUM_QUERIERS; i++)
            {
                new Thread(() => new QueryClient(SERVER, PORT)).Start();
            }
        }

    }

    class QueryClient
    {
        // static readonly ulong LOOKBACK_NS = 10_000_000_000;
        static readonly ulong LOOKBACK_NS = 1_000_000_000;

        // static readonly int WAIT_DUR = 5_000;
        static readonly int WAIT_DUR = 245_000;

        Random rand = new Random();
        HashSet<ulong> sources;

        public QueryClient(string server, int port)
        {
            try
            {
                using TcpClient client = ConnectWithRetry(server, port);
                Console.WriteLine("Client connected");
                NetworkStream stream = client.GetStream();

                var buf = new byte[1L << 21];

                // ExperimentStart message
                var msgSize = ReadMessage(stream, buf);
                var expStartMsg = ExperimentStart.Decode(buf);
                sources = expStartMsg.Sources;
                Console.WriteLine($"Choosing from {expStartMsg.Sources.Count} sources");

                Thread.Sleep(WAIT_DUR);

                while (DoQuery(stream, buf))
                {
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("QueryClientException: {0}", e);
            }
        }

        // Issue and process a query. Returning whether the client should continue
        // issue another query.
        bool DoQuery(NetworkStream stream, byte[] buf)
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

                var msgSize = ReadMessage(stream, buf);
                if (!BlockReply.IsServerActive(buf))
                {
                    return false;
                }

                // the first byte is the active byte
                var block = new Span<byte>(buf, 1, (int)msgSize - 1);

                var done = ProcessBlock(block, query, out ulong nextAddr, out int sourceSampleCount);
                sampleCounts += sourceSampleCount;
                blocksScanned++;
                if (done)
                {
                    Console.WriteLine("Finished query processing");
                    break;
                }

                // ask server for this block
                query.NextBlockAddr = nextAddr;
                Console.WriteLine($"processing block {blocksScanned}, next addr {nextAddr}");
            }

            Console.WriteLine($"Query done, {sw.ElapsedMilliseconds} ms {sampleCounts} samples {blocksScanned} blocks scanned");

            return true;
        }

        bool ProcessBlock(Span<byte> block, Query q, out ulong nextAddr, out int sourceSampleCount)
        {
            // Console.WriteLine($"Processing block of size {block.Length}");

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
            // Console.WriteLine($"Processing decompressed samples of size {sampleBytes.Length}");

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
                // Console.WriteLine($"Count {sourceSampleCount} offset {sampleOffset} Len {sampleBytes.Length}");
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
            // Console.WriteLine($"Reading message of size {size}");
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
                // Console.WriteLine($"reading size {remaining} at offset {start}");
                var nread = stream.Read(bytes, start, remaining);
                // Console.WriteLine($"read {nread} bytes");
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
