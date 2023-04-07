using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.IO;
using System.Buffers.Binary;

namespace FasterLogData
{
    public class DataLoader
    {

        public static List<Point> LoadSamples(string filePath)
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

        public static List<byte[]> LoadSerializedSamples(string filePath)
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

        public static List<(ulong, byte[])> LoadSerializedSamplesWithTimestamp(string filePath)
        {
            var points = new List<(ulong, byte[])>();
            points.EnsureCapacity(450_000_000);
            var ulongBytes = new byte[8];

            ulong totalSize = 0;

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
                    totalSize += (ulong)serialized.Length;

                    if (points.Count % 1000000 == 0)
                    {
                        Console.WriteLine("loaded {0} samples", points.Count);
                    }
                }
            }

            Console.WriteLine("Read {0} samples", points.Count);
            Console.WriteLine("Total size {0} ", totalSize);

            return points;
        }

        public static List<(ulong, byte[])> SaveSerializedSamplesToFile(string filePath)
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
                        var sp = Point.Serialize(point);

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
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Failed to parse {0}, exp {1}", line, e);
                        erred++;
                    }
                }
            }

            Console.WriteLine("Failed to read {0} samples", erred);

            return points;
        }
    }
}
