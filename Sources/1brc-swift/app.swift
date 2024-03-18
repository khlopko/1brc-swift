// The Swift Programming Language
// https://docs.swift.org/swift-book

import Foundation

let path = "../1brc/measurements.txt"

@main
struct Main {

    static func main() async throws {
        let begin = CFAbsoluteTimeGetCurrent()
        defer {
            let duration = CFAbsoluteTimeGetCurrent() - begin
            print("Time elapsed: \(duration) seconds")
        }

        let totalChunks: UInt64 = 1024
        guard let h = FileHandle(forReadingAtPath: path) else {
            return
        }
        defer { h.closeFile() }
        try h.seekToEnd()
        let max = try h.offset()
        let chunkSize = UInt64(ceil(Double(max) / Double(totalChunks)))

        try h.seek(toOffset: 0)
        var chunks: [(start: UInt64, size: UInt64)] = []
        var offset: UInt64 = 0
        var chunkStart: UInt64 = 0
        var c = try h.read(upToCount: 1)?[0]
        while offset < max {
            assert(c != 10, "Byte is \(c!)")
            chunkStart = offset
            offset += chunkSize
            try h.seek(toOffset: offset)
            while offset < max && c != 10 {
                c = try h.read(upToCount: 1)?[0]
            }
            offset = try h.offset()
            chunks.append((chunkStart, offset - chunkStart))
            c = try h.read(upToCount: 1)?[0]
        }
        try h.seek(toOffset: 0)
        let results = await withTaskGroup(of: [Int: Measurement].self) { group in
            for (i, p) in chunks.enumerated() {
                group.addTask {
                    let (start, size) = p
                    let reader = ReadByChunks()
                    let data = await reader.run(i, offset: start, chunkSize: size)
                    let parser = ParseLines()
                    let lines = await parser.run(data: data)
                    let collector = PartialCollect()
                    return await collector.run(lines: lines)
                }
            }
            var partialResults: [Int: Measurement] = Dictionary(minimumCapacity: 500)
            var i = 0
            for await result in group {
                i += 1
                for (name, measurement) in result {
                    let hash = name.hashValue
                    if let existing = partialResults[hash] {
                        partialResults[hash] = Measurement(
                            name: existing.name,
                            min: min(existing.min, measurement.min),
                            avg: existing.avg + measurement.avg,
                            max: Swift.max(existing.max, measurement.max),
                            count: existing.count + measurement.count
                        )
                    } else {
                        partialResults[hash] = measurement
                    }
                }
            }
            print("Processed \(i.formatted()) chunks")
            return partialResults
        }
        Measurement.display(results: results)
    }

}

struct Measurement {
    let name: Data
    var min: Int32
    var avg: Int32
    var max: Int32
    var count: Int32
}

extension Measurement {
    @inlinable
    static func display(results: borrowing [Int: Measurement]) {
        print("{", terminator: "")
        let prepared: [(String, Measurement)] = results.map { (String(data: $1.name, encoding: .utf8)!, $1) }
        let out: String = prepared
            .sorted { lhs, rhs in
                lhs.0 < rhs.0
            }
            .map { (name, value) -> String in
                String(
                    format: "%@=%.1f/%.1f/%.1f", 
                    name, 
                    Double(value.min) / 10.0,
                    Double(value.avg) / 10.0 / Double(value.count),
                    Double(value.max) / 10.0
                )
            }
            .joined(separator: ", ")
        print(out, terminator: "")
        print("}")
    }
}

actor ReadByChunks {
    let h = FileHandle(forReadingAtPath: path)!

    @inlinable
    func run(_ chi: Int, offset: UInt64, chunkSize: UInt64) -> Data {
        try! h.seek(toOffset: offset)
        let data = try! h.read(upToCount: Int(chunkSize))!
        return data
    }
}

actor ParseLines {
    @inlinable
    func run(data: borrowing Data) -> [(name: Data, temp: Int32)] {
        var lines: [(name: Data, temp: Int32)] = []
        lines.reserveCapacity(500_000)
        var i = 0
        var temp: Int32 = 0
        var sign: Int32 = 1
        var k = i
        var name = Data()
        while i < data.count {
            k = i
            while data[i] != 59 {
                i += 1
            }
            name = data[k..<i]
            i += 1 // skip the semicolon
            temp = 0
            sign = 1
            if data[i] == 45 {
                sign = -1
                i += 1
            }
            while data[i] != 10 {
                if data[i] == 46 {
                    i += 1
                }
                temp = temp * 10 + Int32(data[i] - 48)
                i += 1
            }
            temp *= sign
            lines.append((name, temp))
            i += 1
        }
        return lines
    }
}

actor PartialCollect {
    @inlinable
    func run(lines: [(name: Data, temp: Int32)]) -> [Int: Measurement] {
        var results: [Int: Measurement] = Dictionary(minimumCapacity: 500)
        for line in lines {
            let hash = line.name.hashValue
            if let measurement = results[hash] {
                results[hash] = Measurement(
                    name: line.name,
                    min: min(measurement.min, line.temp),
                    avg: measurement.avg + line.temp,
                    max: Swift.max(measurement.max, line.temp),
                    count: measurement.count + 1
                )
            } else {
                results[hash] = Measurement(name: line.name, min: line.temp, avg: line.temp, max: line.temp, count: 1)
            }
        }
        return results
    }
}

