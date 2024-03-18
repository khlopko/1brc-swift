// swift-tools-version: 5.10

import PackageDescription

let package = Package(
    name: "1brc-swift",
    platforms: [
        .macOS(.v12),
    ],
    products: [
        .executable(
            name: "1brc-swift",
            targets: ["1brc-swift"]),
    ],
    targets: [
        .executableTarget(
            name: "1brc-swift",
            swiftSettings: [.enableExperimentalFeature("StrictConcurrency=complete")]
        )
    ]
)
