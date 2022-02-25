``` ini

BenchmarkDotNet=v0.13.1, OS=Windows 10.0.22557
Unknown processor
.NET SDK=6.0.200
  [Host]     : .NET Core 3.1.22 (CoreCLR 4.700.21.56803, CoreFX 4.700.21.57101), X64 RyuJIT
  DefaultJob : .NET Core 3.1.22 (CoreCLR 4.700.21.56803, CoreFX 4.700.21.57101), X64 RyuJIT


```
|              Method |     Mean |     Error |    StdDev | Rank |  Gen 0 | Allocated |
|-------------------- |---------:|----------:|----------:|-----:|-------:|----------:|
| BenchmarkIdProvider | 1.510 μs | 0.0296 μs | 0.0479 μs |    1 | 0.5951 |      2 KB |
