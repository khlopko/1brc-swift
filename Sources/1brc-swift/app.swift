// The Swift Programming Language
// https://docs.swift.org/swift-book

import Foundation

let path = "../1brc/measurements.txt"

@main
struct Main {

    static func main() async throws {
        let results = await withTaskGroup(of: [Int: Measurement].self) { group in
            let totalChunks: UInt64 = 2048
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
                for (key, measurement) in result {
                    if let existing = partialResults[key] {
                        partialResults[key] = Measurement(
                            name: existing.name,
                            min: min(existing.min, measurement.min),
                            avg: existing.avg + measurement.avg,
                            max: Swift.max(existing.max, measurement.max),
                            count: existing.count + measurement.count
                        )
                    } else {
                        partialResults[key] = measurement
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
    let min: Int
    let avg: Int
    let max: Int
    let count: Int
}

extension Measurement {
    @inline(__always)
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

    @inline(__always)
    func run(_ chi: Int, offset: Int, chunkSize: Int) -> [UInt8] {
        let file: UnsafeMutablePointer<FILE> = fopen(path, "r")!
        fseek(file, offset, SEEK_SET)
        let buffer = UnsafeMutableRawPointer.allocate(byteCount: chunkSize, alignment: MemoryLayout<UInt8>.alignment)
        fread(buffer, 1, chunkSize, file)
        let bytes = buffer.bindMemory(to: UInt8.self, capacity: chunkSize)
        let data = Array(UnsafeBufferPointer(start: bytes, count: chunkSize))
        buffer.deallocate()
        fclose(file)
        return data
    }
}

actor ParseLines {
    @inline(__always)
    func run(data: consuming [UInt8]) -> [Int: Measurement] {
        var results: [Int: Measurement] = Dictionary(minimumCapacity: 500)
        var i = 0
        var temp: Int = 0
        var sign: Int = 1
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
            while data[i] != 46 {
                temp = temp * 10 + Int(data[i]) - 48
                i += 1
            }
            i += 1 // skip the dot
            temp = temp * 10 + Int(data[i]) - 48
            temp *= sign
            i += 1 // skip the newline
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

