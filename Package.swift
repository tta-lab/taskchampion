// swift-tools-version: 5.9

import PackageDescription

let package = Package(
    name: "TaskChampionFFI",
    platforms: [
        .iOS(.v13),
    ],
    products: [
        .library(
            name: "TaskChampionFFI",
            targets: ["TaskChampionFFI"]
        ),
    ],
    targets: [
        // Generated Swift bindings that call into the C FFI layer
        .target(
            name: "TaskChampionFFI",
            dependencies: ["taskchampion_ffiFFI"],
            path: "Sources/TaskChampionFFI"
        ),
        // Pre-built static library + C headers (produced by scripts/build_xcframework.sh)
        .binaryTarget(
            name: "taskchampion_ffiFFI",
            path: "taskchampion_ffiFFI.xcframework"
        ),
    ]
)
