using System;
using BenchmarkDotNet.Running;
using benchmarks.Ingestors.Generic;

namespace adt_auto_ingestor_benchmarks
{
    class Program
    {
        static void Main(string[] args)
        {
            BenchmarkRunner.Run<GenericTwinIdProviderBenchmarks>();
        }
    }
}
