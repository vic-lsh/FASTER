using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Newtonsoft.Json;
using System.IO;
using System.Buffers.Binary;

namespace FasterLogData
{
    public struct PointRef
    {
        public ulong TsDelta;
        IntPtr heap;
        int index;
        int len;

        public PointRef(IntPtr _heap, int _index, int _len, ulong _tsDelta)
        {
            heap = _heap;
            index = _index;
            len = _len;
            TsDelta = _tsDelta;
        }

        public Span<byte> GetSpan()
        {
            //return new Span<byte>(heap, index, len);
            Span<byte> sp;
            unsafe
            {
                sp = new Span<byte>(heap.ToPointer(), Int32.MaxValue);
            }
            return sp.Slice(index, len);
        }
    }

    public struct UnmanagedArray
    {
        public int length;
        public int capacity;
        public IntPtr ptr;
    }

    public class DataLoader
    {
        static readonly int NUM_SAMPLES = 447_055_756;

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

        public static void LoadSerializedSamplesUnmanaged(string filePath)
        {
            // 100gb data = 50 * Int32.MaxValue
            // need to read bytes s.t. sample bytes are not split across arrays
            var nArrays = 50;
            var arraysPtr = Marshal.AllocHGlobal(nArrays * Marshal.SizeOf<IntPtr>());

            // two arrays of size < Int32.MaxValue:
            // array containing points into a span of bytes in an array in arraysPtr (IntPtr of array + offset)
            // array containing length of span being pointed to
            var nSamples = 500_000_000; // larger than the number of samples we have, should probably be accurate
            var samplesBytesPtr = Marshal.AllocHGlobal(nSamples * Marshal.SizeOf<IntPtr>());
            var samplesLenPtr = Marshal.AllocHGlobal(nSamples * Marshal.SizeOf<Int32>());

            //var
            //Span<UnmanagedArray> arraysSpan;
            //unsafe
            //{
            //	arraysSpan = new Span<UnmanagedArray>(arraysPtr.ToPointer(), capacity);
            //}

            //arraysSpan[length].capacity = Int32.MaxValue;
            //arraysSpan[length].ptr = Marshal.AllocHGlobal(Int32.MaxValue);
            //arraysSpan[length].length = 0;

            //using (BinaryReader reader = new BinaryReader(new FileStream(filePath, FileMode.Open)))
            //{
            //	while (true)
            //	{
            //	}
            //}

        }

        public static PointRef[] LoadSerializedSamplesWithTimestamp(string filePath)
        {
            //var heapSize = 2_000_000_000;

            var points = GC.AllocateArray<PointRef>(NUM_SAMPLES);
            var pointsIdx = 0;
            Span<byte> currHeap;
            var arraysPtr = Marshal.AllocHGlobal(Int32.MaxValue);
            unsafe
            {
                currHeap = new Span<byte>(arraysPtr.ToPointer(), Int32.MaxValue);
            }
            //var currHeap = GC.AllocateArray<byte>(heapSize);
            var heapOffset = 0;

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

                    //if (heapOffset + (int)serializationSize > currHeap.Length) // avoid wrapping addition
                    if (currHeap.Length - heapOffset < (int)serializationSize)
                    {
                        Console.WriteLine("Allocating another {0}", Int32.MaxValue);
                        arraysPtr = Marshal.AllocHGlobal(Int32.MaxValue);
                        unsafe
                        {
                            currHeap = new Span<byte>(arraysPtr.ToPointer(), Int32.MaxValue);
                        }
                        heapOffset = 0;
                    }

                    if (reader.Read(currHeap.Slice(heapOffset, (int)serializationSize)) != (int)serializationSize)
                    {
                        throw new Exception("Failed to read the entire serialized sample");
                    }
                    var pointRef = new PointRef(arraysPtr, heapOffset, (int)serializationSize, tsDelta);
                    heapOffset += (int)serializationSize;

                    //Console.WriteLine("Here3");
                    points[pointsIdx++] = pointRef;

                    totalSize += serializationSize;

                    if (pointsIdx % 1000000 == 0)
                    {
                        Console.WriteLine("loaded {0} samples", pointsIdx);
                    }
                    if (pointsIdx == NUM_SAMPLES)
                    {
                        break;
                    }
                }
            }

            Console.WriteLine("Read {0} samples", pointsIdx);
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
