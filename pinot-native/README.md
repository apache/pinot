# pinot-native

Native (Rust) acceleration kernels for Pinot's query execution path, exposed via
JNI. See `docs/native/phase-1-design.md` and `RUST_REWRITE_DESIGN.md` for the
overall plan and design rationale.

## Status

**Phase 0 / 1.A POC.** Only `SUM(LONG)` is wired through to a native kernel.
The native engine is opt-in, off by default.

## Building

Requires:

- Rust toolchain (stable, 1.75+)
- JDK 21+ (for the Java side)
- Maven 3.6+

From repo root:

```sh
./mvnw -pl pinot-native -am package
```

This invokes `cargo build --release` against `pinot-native/native/Cargo.toml`,
producing `pinot-native/native/target/release/libpinot_native.{dylib,so}`. The
Java tests are configured to find that file via the `pinot.native.lib.path`
system property.

To skip the Rust build (e.g. on a machine without Cargo):

```sh
./mvnw -pl pinot-native -am package -DskipNativeBuild
```

The Java side will then load with `isAvailable() == false` and callers will
fall back to the Java aggregation path.

## Layout

```
pinot-native/
├── pom.xml
├── native/                             Cargo workspace
│   ├── Cargo.toml                      workspace root
│   ├── ffi/                            JNI bindings (cdylib → libpinot_native)
│   └── kernels/                        pure Rust SIMD kernels
└── src/
    ├── main/java/org/apache/pinot/nativeengine/agg/
    │   ├── PinotNativeAgg.java         static native method declarations
    │   └── NativeLibLoader.java        lib resolution (dev path / classpath / java.library.path)
    └── test/java/...                   TestNG smoke tests
```

## Loading the native library

`NativeLibLoader` (called from `PinotNativeAgg`'s static initializer) tries
the following sources in order:

1. The path in system property `pinot.native.lib.path`, if set.
2. A classpath resource `/native/<os>-<arch>/lib<name>.<ext>`, extracted to
   a temp file and `System.load`-ed. This is the path used in packaged JARs
   (not yet wired in this POC).
3. `System.loadLibrary("pinot_native")` — last-resort fall-through to
   `java.library.path`.

If all three fail, `PinotNativeAgg.isAvailable()` returns `false` and the
class is unusable; callers must check this before invoking any native method.

## Adding a kernel

1. Add the pure Rust implementation under `native/kernels/src/`.
2. Add the JNI binding under `native/ffi/src/lib.rs`. Symbol name must match
   `Java_<fully_qualified_class>_<methodName>`. Wrap in `panic::catch_unwind`.
3. Declare `static native` on `PinotNativeAgg`.
4. Add a unit test in `src/test/java`.
5. Wire it into the operator path under `pinot-core` (see Phase 1 design doc
   for the integration pattern).
