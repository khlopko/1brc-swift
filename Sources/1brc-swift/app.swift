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

        let results = await withTaskGroup(of: [Int: Measurement].self) { group in
            let totalChunks: UInt64 = 128
            let file = fopen(path, "r")!
            defer {
                fclose(file)
            }
            fseek(file, 0, SEEK_END)
            let max = ftell(file)
            let chunkSize = Int(ceil(Double(max) / Double(totalChunks)))
            rewind(file)
            var offset: Int = 0
            var chunkStart: Int = 0
            var chunkN = 0
            var c = fgetc(file)
            while feof(file) == 0 {
                chunkStart = offset
                offset += chunkSize
                fseek(file, offset, SEEK_SET)
                while feof(file) == 0 && c != 10 {
                    c = fgetc(file)
                }
                offset = ftell(file)
                if offset > max {
                    offset = max
                }
                let size = offset - chunkStart
                group.addTask { [chunkN, chunkStart] in
                    let reader = ReadByChunks()
                    let data = await reader.run(chunkN, offset: chunkStart, chunkSize: size)
                    let parser = ParseLines()
                    let partialResult = await parser.run(data: data)
                    return partialResult
                }
                c = fgetc(file)
                chunkN += 1
            }
            var partialResults: [Int: Measurement] = Dictionary(minimumCapacity: 500)
            for await result in group {
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
            return partialResults
        }
        Measurement.display(results: results)
    }

}

struct Measurement {
    let name: ArraySlice<UInt8>
    var min: Int32
    var avg: Int32
    var max: Int32
    var count: Int32
}

extension Measurement {
    @inlinable
    static func display(results: borrowing [Int: Measurement]) {
        print("{", terminator: "")
        let prepared: [(String, Measurement)] = results.map { (String(cString: Array($1.name) + [0]), $1) }
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
    let file: UnsafeMutablePointer<FILE> = fopen(path, "r")!

    func run(_ chi: Int, offset: Int, chunkSize: Int) -> [UInt8] {
        fseek(file, offset, SEEK_SET)
        let buffer = UnsafeMutableRawPointer.allocate(byteCount: chunkSize, alignment: MemoryLayout<UInt8>.alignment)
        fread(buffer, 1, chunkSize, file)
        let bytes = buffer.bindMemory(to: UInt8.self, capacity: chunkSize)
        let data = Array(UnsafeBufferPointer(start: bytes, count: chunkSize))
        buffer.deallocate()
        return data
    }
}

actor ParseLines {
    func run(data: borrowing [UInt8]) -> [Int: Measurement] {
        var results: [Int: Measurement] = Dictionary(minimumCapacity: 500)
        var i = 0
        var temp: Int32 = 0
        var sign: Int32 = 1
        var k = i
        var name: ArraySlice<UInt8> = []
        let mask: Int = 0x7FFFFFFF
        while i < data.count {
            k = i
            var hash: Int = 0
            while data[i] != 59 {
                hash = 31 &* hash &+ Int(data[i])
                i += 1
            }
            hash &= mask
            name = data[k..<i]
            i += 1 // skip the semicolon
            temp = 0
            sign = 1
            if data[i] == 45 {
                sign = -1
                i += 1
            }
            while i < data.count && data[i] != 10 {
                if data[i] == 46 {
                    i += 1
                }
                temp = temp * 10 + Int32(data[i] - 48)
                i += 1
            }
            temp *= sign
            if let measurement = results[hash] {
                results[hash] = Measurement(
                    name: name,
                    min: min(measurement.min, temp),
                    avg: measurement.avg + temp,
                    max: Swift.max(measurement.max, temp),
                    count: measurement.count + 1
                )
            } else {
                results[hash] = Measurement(name: name, min: temp, avg: temp, max: temp, count: 1)
            }
            i += 1
        }
        return results
    }
}

