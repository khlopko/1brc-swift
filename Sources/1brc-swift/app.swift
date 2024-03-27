// 1brc implementation in Swift
// (c) Kyrylo Khlopko

import Foundation // formatted printing
import System // file descriptor

@main
struct Main {
    static func main() async {
        let path = CommandLine.arguments.last!
        let app = App(path: path)
        await app.run()
    }
}


// 10k version has 10k unique stations, so we need a relative capacity of the dictionary,
// and since dictionary usually grows by doubling, we need to set the initial capacity as power of two.
private let capacity = 1 << 14

// FNV-1a hash constants for 64-bit hash
private let fnv1aBasis: UInt64 = 14695981039346656037
private let fnv1aPrime: UInt64 = 1099511628211

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
        let totalChunks: Int64 = 2048
        let fd = try! FileDescriptor.open(path, .readOnly)
        defer {
            try! fd.close()
        }
        let max = try! fd.seek(offset: 0, from: .end)
        let chunkSize = max / totalChunks + 1
        var chunkStart: Int64 = 0
        var offset: Int64 = 0
        @inline(__always)
        func getc() -> UInt8 {
            let buffer = UnsafeMutableRawBufferPointer.allocate(byteCount: 1, alignment: MemoryLayout<UInt8>.alignment)
            defer {
                buffer.deallocate()
            }
            let pos = try! fd.read(fromAbsoluteOffset: offset, into: buffer)
            offset += Int64(pos)
            return buffer.bindMemory(to: UInt8.self)[0]
        }
        var c = getc()
        let readers = (0..<10).map { _ in ReadByChunks(fd: fd) }
        while offset < max {
            chunkStart = offset - 1
            offset += chunkSize
            while offset < max && c != 10 {
                c = getc()
            }
            if offset > max {
                offset = max
            }
            let size = offset - chunkStart
            if size == 0 {
                break
            }
            group.addTask { [chunkStart] in
                let reader = readers[Int(chunkStart) % 10]
                let data = await reader.run(offset: chunkStart, chunkSize: Int(size))
                let parser = ParseLines()
                let partialResult = await parser.run(data: data)
                return partialResult
            }
            c = getc()
        }
        var partialResults = PartialResult(minimumCapacity: capacity)
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
    private let fd: FileDescriptor

    @inline(__always)
    init(fd: FileDescriptor) {
        self.fd = fd
    }

    @inline(__always)
    func run(offset: Int64, chunkSize: Int) -> [UInt8] {
        let buffer = UnsafeMutableRawBufferPointer.allocate(byteCount: chunkSize, alignment: MemoryLayout<UInt8>.alignment)
        defer {
            buffer.deallocate()
        }
        _ = try! fd.read(fromAbsoluteOffset: offset, into: buffer)
        let bytes = buffer.bindMemory(to: UInt8.self)
        let data = Array(bytes)
        return data
    }
}


actor ParseLines {
    @inline(__always)
    func run(data: consuming [UInt8]) -> PartialResult {
        var results = PartialResult(minimumCapacity: capacity)
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

