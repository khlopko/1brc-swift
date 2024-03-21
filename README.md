# 1brc in Swift

Original repo: https://github.com/gunnarmorling/1brc/

This is an attempt to implement this challenge in Swift. I took the original requirements & test data, and 
tried to make it run in reasonable time.

Right now processes in ~9s on MacBook Pro M1:

```sh
hyperfine .build/release/1brc-swift                                                                                                      2 err  16:51:47
Benchmark 1: .build/release/1brc-swift
  Time (mean ± σ):      9.160 s ±  0.351 s    [User: 65.159 s, System: 3.869 s]
  Range (min … max):    8.904 s … 10.077 s    10 runs
```

