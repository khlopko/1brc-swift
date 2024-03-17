// The Swift Programming Language
// https://docs.swift.org/swift-book

import Foundation

@main
struct Main {
    struct Measurement {
        let name: String
        var min: Int32
        var avg: Int32
        var max: Int32
        var count: Int32
    }

    static func main() {
        let limit = 1_000_000_000
        print("Running aganst \(limit.formatted()) records")

        let total = CFAbsoluteTimeGetCurrent()

        var station: [UInt8] = Array(repeating: 0, count: 40)
        var temp: Int32 = 0
        var results: [Int32: Measurement] = Dictionary(minimumCapacity: 500)
        var i = 0
        var k = 0
        var hash: Int32 = 0
        let mask: Int32 = 0x7FFFFFFF

        let data = try! Data(contentsOf: URL(fileURLWithPath: "../1brc/measurements.txt"))
        print("File size: \(data.count.formatted()) bytes")
        let file = fopen("../1brc/measurements.txt", "r")
        skipComments(file: file)
        var c: UInt8 = 0
        var pos: fpos_t = 0
        fgetpos(file, &pos)
        var p = Int(pos)
        func fgetc() -> UInt8 {
            defer { p += 1 }
            return data[p]
        }
        while i < limit {
            if p >= data.count {
                break
            }
            c = fgetc()
            k = 0
            hash = 0
            while c != 59 {
                station[k] = c
                hash = 31 &* hash &+ Int32(c)
                k += 1
                c = fgetc()
            }
            hash &= mask
            c = fgetc()
            temp = 0
            if c == 45 {
                temp = -1
                c = fgetc()
            }
            while c != 10 {
                if c == 46 {
                    c = fgetc() // skip the dot
                }
                temp = temp * 10 + Int32(c - 48)
                if c == 46 {
                    break
                }
                c = fgetc()
            }
            if results[hash] != nil {
                results[hash]!.min = min(results[hash]!.min, temp)
                results[hash]!.max = max(results[hash]!.max, temp)
                results[hash]!.avg += temp
                results[hash]!.count += 1
            } else {
                results[hash] = Measurement(
                    name: String(cString: station.compactMap { $0 > 0 ? UInt8($0) : nil } + [0]), 
                    min: temp,
                    avg: temp,
                    max: temp,
                    count: 1
                )
            }
            i += 1
            if i % 50_000_000 == 0 {
                print("Processed \(i.formatted()) records")
            }
        }
        print("{", terminator: "")
        let out = results.sorted { $0.value.name < $1.value.name }.map { (_, value) in
            String(
                format: "%@=%.1f/%.1f/%.1f", 
                value.name,
                Double(value.min) / 10.0,
                Double(value.avg) / 10.0 / Double(value.count),
                Double(value.max) / 10.0
            )
        }.joined(separator: ", ")
        print(out, terminator: "")
        print("}")
        let duration = CFAbsoluteTimeGetCurrent() - total
        let timeValue = Int(round(duration))
        let minutes = Int(timeValue / 60)
        let seconds = timeValue % 60
        print("Time elapsed: \(minutes):\(String(format: "%02d", seconds))")
    }

    @inline(__always)
    private static func skipComments(file: UnsafeMutablePointer<FILE>?) {
        while fgetc(file) == 35 {
            while fgetc(file) != 10 {
                // skip the comment
            }
        }
        fseek(file, -1, SEEK_CUR)
    }
}
