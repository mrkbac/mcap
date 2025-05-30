// swift-tools-version:5.5

import PackageDescription

let package = Package(
  name: "mcap",
  platforms: [.macOS(.v10_15), .iOS(.v13)], // for async/await
  products: [
    .library(name: "MCAP", targets: ["MCAP"]),
  ],
  dependencies: [
    .package(url: "https://github.com/apple/swift-docc-plugin", from: "1.4.3"),
    .package(url: "https://github.com/apple/swift-algorithms", from: "1.2.1"),

    // Use pre-release version for Heap
    .package(url: "https://github.com/apple/swift-collections", .upToNextMajor(from: "1.1.4")),
  ],
  targets: [
    .target(
      name: "MCAP",
      dependencies: [
        "CRC",
        .product(name: "HeapModule", package: "swift-collections"),
        .product(name: "Algorithms", package: "swift-algorithms"),
      ],
      path: "swift/mcap"
    ),
    .testTarget(name: "unit-tests", dependencies: ["MCAP"], path: "swift/test"),
    .executableTarget(name: "conformance", dependencies: ["MCAP"], path: "swift/conformance"),

    .target(name: "CRC", dependencies: [], path: "swift/crc"),
    .testTarget(name: "crc-tests", dependencies: ["CRC"], path: "swift/crc-tests"),
  ]
)
