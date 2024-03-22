// 1brc implementation in Swift
// (c) Kyrylo Khlopko

import Foundation

@main
struct Main {
    static func main() async {
        let path = "../1brc/measurements.txt"
        let app = App(path: path)
        await app.run()
    }
}

typealias PartialResult = [UInt64: Measurement]

struct Measurement {
    let name: ArraySlice<UInt8>
    let min: Int
    let avg: Int
    let max: Int
    let count: Int
}

struct App {
    let path: String

    @inline(__always)
    func run() async {
        let results = await withTaskGroup(of: PartialResult.self) { group in
            await process(inside: &group)
        }
        let printer = Printer()
        printer.run(on: results.values)
    }

    @inline(__always)
    private func process(inside group: inout TaskGroup<PartialResult>) async -> PartialResult {
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
            group.addTask { [chunkStart] in
                let reader = ReadByChunks(path: path)
                let data = await reader.run(offset: chunkStart, chunkSize: size)
                let parser = ParseLines()
                let partialResult = await parser.run(data: data)
                return partialResult
            }
            c = fgetc(file)
        }
        var partialResults = PartialResult(minimumCapacity: 500)
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
}

actor ReadByChunks {
    private let path: String

    init(path: String) {
        self.path = path
    }

    @inline(__always)
    func run(offset: Int, chunkSize: Int) -> [UInt8] {
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

private let fnv1aBasis: UInt64 = 14695981039346656037
private let fnv1aPrime: UInt64 = 1099511628211

actor ParseLines {
    @inline(__always)
    func run(data: consuming [UInt8]) -> PartialResult {
        var results = PartialResult(minimumCapacity: 500)
        var i = 0
        var temp: Int = 0
        var sign: Int = 1
        var k = i
        var name: ArraySlice<UInt8> = []
        while i < data.count {
            k = i
            var hash = fnv1aBasis
            while data[i] != 59 {
                hash ^= UInt64(data[i])
                hash = hash.multipliedReportingOverflow(by: fnv1aPrime).partialValue
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
            while data[i] != 46 {
                temp = temp * 10 + Int(data[i]) - 48
                i += 1
            }
            i += 1 // skip the dot
            temp = temp * 10 + Int(data[i]) - 48
            temp *= sign
            i += 1 // skip the newline
            switch results[hash] {
            case let .some(measurement):
                results[hash] = Measurement(
                    name: measurement.name,
                    min: min(measurement.min, temp),
                    avg: measurement.avg + temp,
                    max: Swift.max(measurement.max, temp),
                    count: measurement.count + 1
                )
            case .none:
                results[hash] = Measurement(name: name, min: temp, avg: temp, max: temp, count: 1)
            }
            i += 1
        }
        return results
    }
}

/// Prints the final results to stdout.
struct Printer {
    @inline(__always)
    func run(on results: any Collection<Measurement>) {
        print("{", terminator: "")
        let prepared: [(String, Measurement)] = results.map { (String(cString: Array($0.name) + [0]), $0) }
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

