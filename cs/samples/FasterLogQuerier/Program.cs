using System;
using System.Net.Sockets;
using System.Buffers.Binary;

namespace FasterLogQuerier
{
    public class Program
    {
        static void Main()
        {
            Client();
        }

        static void Client()
        {
            try
            {
                Int32 port = 13000;
                string server = "127.0.0.1";

                using TcpClient client = new TcpClient(server, port);
                NetworkStream stream = client.GetStream();

                var buf = new byte[1L << 21];

                // ExperimentStart message
                var msgSize = ReadMessage(stream, buf);
                var expStartMsg = ExperimentStart.Decode(buf);
                Console.WriteLine($"Choosing from {expStartMsg.Sources.Count} sources");
            }
            catch (Exception e)
            {
                Console.WriteLine("QueryClientException: {0}", e);
            }
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
