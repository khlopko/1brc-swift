# 1brc in Swift

Original repo: https://github.com/gunnarmorling/1brc/

This is an attempt to implement this challenge in Swift. I took the original requirements & test data, and 
tried to make it run in reasonable time.

Right now processes in ~7.7s on MacBook Pro M1:

```sh
hyperfine .build/release/1brc-swift                                                                                                        ok  4s  16:50:04
Benchmark 1: .build/release/1brc-swift
  Time (mean ± σ):      7.712 s ±  0.026 s    [User: 65.545 s, System: 4.366 s]
  Range (min … max):    7.676 s …  7.753 s    10 runs
```

## How to run
```sh
swift build -c release && time .build/release/1brc-swift > /dev/null
```

