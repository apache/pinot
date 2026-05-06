# pinot-plugin-verifier

Standalone tool for verifying that every plugin shipped in a built Pinot distribution
loads correctly through `PluginManager`. Run it as a separate JVM with `lib/pinot-all.jar`
on its classpath and `-Dplugins.dir=plugins/` set, so realm isolation is genuinely
exercised — not masked by Maven's all-plugins-on-classpath test layout.

## Why this can't be a TestNG test

A surefire-driven test runs inside the build's classpath, where every plugin module is
already a transitive compile dep of the test module's classloader. `-Dplugins.dir=`
"works" only as a side effect — `PluginManager` populates realms, but the realm copies of
classes never actually win against the system classloader copies the test classpath
provides. The verifier's whole point is to test the production layout (one fat
`pinot-all.jar` + isolated `plugins/<type>/<plugin>/`), so it has to be a separate
process.

## Running against a built distribution

```bash
mvn -Pbin-dist -P!pinot-fastdev install -DskipTests   # produces apache-pinot-VERSION-bin/
build/bin/verify-plugins.sh                           # default: all checks against ./build
build/bin/verify-plugins.sh build/ --check input-format,fs
build/bin/verify-plugins.sh build/ --strict-realm
build/bin/verify-plugins.sh build/ --plugin org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory
build/bin/verify-plugins.sh build/ --verbose
```

> **Disable `pinot-fastdev` when building the distribution to verify.** That profile sets
> `shade.phase.prop=none`, which suppresses the per-plugin shaded jar that the assembly copies
> into `plugins/<type>/<name>/`. Production `bin-dist` builds always run with it disabled —
> match that here. Local dev environments that auto-activate it via
> `~/.m2/settings.xml` need the explicit `-P!pinot-fastdev` flag (or
> `-Dshade.phase.prop=package`); CI builds typically don't.

`--verbose` prints `getProtectionDomain().getCodeSource().getLocation()` for each loaded
class — useful for confirming whether a class came from `pinot-all.jar` (system
classloader) or from a plugin realm under `plugins/`.

## Flags

| flag | meaning |
|---|---|
| `--check <types>` | Comma-separated subset of: `input-format`, `stream`, `fs`, `metrics`. Default: `all` |
| `--plugin <fqcn>` | Restrict to the verifier(s) targeting this single FQCN |
| `--strict-realm` | Look up via `pluginName:FQCN`. Bare-FQCN form (default) relies on the realm walk added by apache/pinot#18386; strict form works on both old and new code |
| `--verbose`, `-v` | Print per-class `CodeSource` so you can see which jar/realm each class came from |

## What each check exercises

| check | API |
|---|---|
| `input-format` | `PluginManager.createInstance(<RecordReader-fqcn>)` for every shipped Avro/CSV/JSON/Parquet/ORC/Protobuf/Thrift/Arrow/Confluent reader |
| `fs` | `PluginManager.createInstance(<PinotFS-fqcn>)` for S3/GCS/ADLS/HDFS |
| `stream` | `PluginManager.createInstance(<StreamConsumerFactory-fqcn>)` for Kafka 3.0 / Kinesis / Pulsar |
| `metrics` | `PluginManager.loadServices(PinotMetricsFactory.class)` — the realm-aware ServiceLoader walk that `PinotMetricUtils.initializePinotMetricsFactory` uses |

## Limits

- The verifier instantiates plugins but does not connect to external systems (Kafka
  brokers, S3 buckets, etc.). Catches "the plugin won't even load" regressions, which is
  the realm-isolation concern. Doesn't catch "the plugin loads but talks wrong to S3" —
  that's what integration tests are for.
- It can only assert things visible from `pinot-spi`-level APIs. Phase-3 / shading
  concerns ("the plugin's bundled jackson is dead weight") are inferred from the
  `--verbose` `CodeSource` output rather than asserted directly.

## Adding new checks

Implement `org.apache.pinot.verifier.checks.Check`, register it in the registry map at
the top of `PluginVerifier.run()`. The check should exercise whatever entry point a real
broker/server/minion uses to instantiate that kind of plugin — not a custom shortcut.
