using System;
using System.Net.Sockets;
using System.Net;
using System.Threading;
using FASTER.core;
using System.Collections.Generic;
using System.Buffers.Binary;
using FasterLogQuerier;

namespace FasterLogSample
{
    public class QueryServer
    {
        public static void Run(FasterLog log, HashSet<ulong> perfSources)
        {
            TcpListener server = null;
            try
            {
                Int32 port = 13000;
                IPAddress localAddr = IPAddress.Parse("127.0.0.1");

                server = new TcpListener(localAddr, port);
                server.Start();
                Console.WriteLine("server started");

                while (true)
                {
                    var conn = server.AcceptTcpClient();
                    new Thread(() => HandleConn(conn, log, perfSources)).Start();
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

        static void HandleConn(TcpClient client, FasterLog log, HashSet<ulong> perfSources)
        {
            Byte[] bytes = new Byte[256];
            using (client)
            {
                Console.WriteLine("Connected!");

                // Get a stream object for reading and writing
                NetworkStream stream = client.GetStream();

                // int i;

                // // Loop to receive all the data sent by the client.
                // while ((i = stream.Read(bytes, 0, bytes.Length)) != 0)
                // {
                //     // Translate data bytes to a ASCII string.
                //     data = System.Text.Encoding.ASCII.GetString(bytes, 0, i);
                //     Console.WriteLine("Received: {0}", data);

                //     // Process the data sent by the client.
                //     data = data.ToUpper();

                //     byte[] msg = System.Text.Encoding.ASCII.GetBytes(data);

                //     // Send back a response.
                //     stream.Write(msg, 0, msg.Length);
                //     Console.WriteLine("Sent: {0}", data);
                // }

                var expStart = new ExperimentStart(perfSources);
                var expStartBytes = expStart.Encode();
                Write(stream, expStartBytes);
            }
        }

        static void Write(NetworkStream stream, byte[] bytes)
        {
            var sizeBytes = new byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(sizeBytes.AsSpan(), (uint)bytes.Length);

            stream.Write(sizeBytes, 0, 4);
            stream.Write(bytes, 0, bytes.Length);
        }
    }
}
