# 1brc in Swift

Original repo: https://github.com/gunnarmorling/1brc/

This is an attempt to implement this challenge in Swift. I took the original requirements & test data, and 
tried to make it run in reasonable time.

Right now it takes ~6.5s on MacBook Pro M1 to process the data:

```sh
hyperfine .build/release/1brc-swift                                                                                                        ok  4s  16:50:04
Benchmark 1: .build/release/1brc-swift
  Time (mean ± σ):      6.515 s ±  0.060 s    [User: 52.862 s, System: 4.566 s]
  Range (min … max):    6.460 s …  6.645 s    10 runs
```

And ~8.8s on 10K version of the data:

```sh
hyperfine .build/release/1brc-swift                                                                                                         ok  15:37:24
Benchmark 1: .build/release/1brc-swift
  Time (mean ± σ):      8.786 s ±  0.097 s    [User: 72.891 s, System: 5.290 s]
  Range (min … max):    8.685 s …  9.037 s    10 runs
```

## How to run

1. Generate measurements.txt (or measurements3.txt for 10k version) file using script from the original repo.
2. Run the following command:
```sh
swift build -c release && time .build/release/1brc-swift /path/to/measurements.txt
```

