Pint Perf Package
===
Pinot perf package contains a set of performance benchmark for Pinot components.

Note: this package will pull `org.openjdk.jmh:jmh-core`, which is based on `GPL 2 license`.

# Steps for running benchmark

1. Build the source
```
$ cd <root_source_code>/pinot-perf
$ mvn package -DskipTests
```
2. The above cmd will generate `target/pinot-perf-pkg`

3. Run benchmark using generated scripts
```
$ cd target/pinot-perf-pkg/bin
$ ./pinot-BenchmarkDictionary.sh
```