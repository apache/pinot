<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# Spark 4 ↔ Pinot — End-to-end Docker Tutorial

This walkthrough brings up Apache Pinot in Docker, builds the Pinot Spark 4 connector, and
exercises both the **read** and **write** paths from a Spark 4 driver. Every step below was
validated against `apachepinot/pinot:latest` + `apache/spark:4.0.0`.

If you only need the one-liner: point Spark 4 at a running Pinot cluster, drop the shaded
connector jar on the classpath, prepend a recent `commons-lang3`, and use
`spark.read.format("pinot")` / `spark.write.format("pinot")`. The rest of this doc explains the
why behind each switch.

---

## 1. Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Docker | 20.10+ | Tested on 28.x. `docker info` must succeed. |
| JDK | 21+ | Only required if you build the connector locally. |
| Maven wrapper | bundled (`./mvnw`) | `-pl pinot-connectors/pinot-spark-4-connector` |

**Why JDK 21.** The Pinot Spark 4 connector is compiled with `--release 21` (class file 65).
Apache Spark 4's default Docker image (`apache/spark:4.0.0`) ships **JDK 17**, which cannot load
class-file-65 bytecode (`UnsupportedClassVersionError`). Either use `apache/spark:4.0.0-java21`
when available, or bake a custom image (shown below) that adds JDK 21.

---

## 2. Start a Pinot cluster

Pinot's `QuickStart -type batch` launches controller + broker + server + minion + zookeeper in a
single container and auto-loads a sample `baseballStats` table (97,889 rows) we can read from.

```bash
docker run -d --name pinot-quickstart \
  -p 9000:9000 \
  -p 8000:8000 \
  -p 8010:8010 \
  -p 7050:7050 \
  -p 7100:7100 \
  apachepinot/pinot:latest QuickStart -type batch
```

| Port | Role | Why exposed |
|---|---|---|
| 9000 | Controller HTTP | Table + schema + segment APIs |
| 8000 | Broker HTTP | SQL query endpoint |
| 8010 | Broker gRPC | Optional server-side streaming |
| 7050 | Server HTTP | Debug/admin |
| 7100 | Server gRPC | **Required** — the connector fetches segment data over this |

Wait for the cluster to be queryable:

```bash
until curl -sSf http://localhost:9000/brokers/tables/baseballStats > /dev/null; do sleep 2; done
curl -sS -X POST -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT COUNT(*) FROM baseballStats"}' \
  http://localhost:8000/query/sql
# => {"resultTable":{...,"rows":[[97889]]}, ...}
```

---

## 3. Build the connector shaded jar

```bash
./mvnw -pl pinot-connectors/pinot-spark-4-connector -am \
       -DskipTests -Pbuild-shaded-jar package
# => pinot-connectors/pinot-spark-4-connector/target/pinot-spark-4-connector-<ver>-shaded.jar
```

Stage it next to a fresh `commons-lang3` 3.20 in a scratch directory the Spark container can
mount:

```bash
mkdir -p /tmp/pinot-spark4-demo
cp pinot-connectors/pinot-spark-4-connector/target/pinot-spark-4-connector-*-shaded.jar /tmp/pinot-spark4-demo/
curl -fLSs -o /tmp/pinot-spark4-demo/commons-lang3-3.20.0.jar \
  https://repo1.maven.org/maven2/org/apache/commons/commons-lang3/3.20.0/commons-lang3-3.20.0.jar
```

**Why the extra `commons-lang3`.** Spark 4.0.0 bundles an older `commons-lang3` that lacks
`ObjectUtils.getIfNull(...)`, a method Pinot calls during connector startup. Without the newer
jar, the read path crashes inside `PinotServerDataFetcher` during the first executor task. The
mitigation below (`spark.{driver,executor}.extraClassPath`) is the simplest fix; see *Known
gotchas* at the bottom for the longer-term options.

---

## 4. Build a Spark 4 + JDK 21 image

Skip this section if you already have a `apache/spark:4.0.0-java21` image or an equivalent.
Otherwise, a 4-line Dockerfile does it:

```bash
cat > /tmp/pinot-spark4-demo/Dockerfile <<'EOF'
FROM apache/spark:4.0.0
USER root
RUN apt-get update && apt-get install -y openjdk-21-jdk-headless --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH
USER spark
EOF

docker build -t spark4-jdk21:local /tmp/pinot-spark4-demo/
```

(On x86_64 replace `java-21-openjdk-arm64` with `java-21-openjdk-amd64`.)

---

## 5. Read from Pinot

Save this to `/tmp/pinot-spark4-demo/read.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("pinot-read").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = (spark.read.format("pinot")
    .option("table", "baseballStats")
    .option("tableType", "offline")
    .option("controller", "localhost:9000")
    .option("broker", "localhost:8000")
    .load())

df.printSchema()
print("row count:", df.count())
df.select("playerName", "teamID", "yearID", "numberOfGames", "hits").show(5, False)
df.groupBy("teamID").count().orderBy("count", ascending=False).show(3, False)
spark.stop()
```

Run it. Note the `--network=container:pinot-quickstart` — this shares the Pinot container's
network namespace so the broker can return the server's internal address (e.g. `172.17.0.2:7100`)
and our Spark driver can still reach it. Without this, Spark would time out on gRPC because the
internal IP isn't routable from the host.

```bash
docker run --rm \
  --network=container:pinot-quickstart \
  -v /tmp/pinot-spark4-demo:/jars:ro \
  -e HOME=/tmp \
  spark4-jdk21:local \
  /opt/spark/bin/spark-submit \
    --jars /jars/pinot-spark-4-connector-1.6.0-SNAPSHOT-shaded.jar,/jars/commons-lang3-3.20.0.jar \
    --conf 'spark.driver.extraClassPath=/jars/commons-lang3-3.20.0.jar' \
    --conf 'spark.executor.extraClassPath=/jars/commons-lang3-3.20.0.jar' \
    --master 'local[2]' \
    --conf 'spark.log.level=WARN' \
    /jars/read.py
```

Expected output: 25-field schema, `row count: 97889`, 5 sample rows, and a teamID histogram
(top teams: `CHN` 4720, `PHI` 4621, `PIT` 4575).

### Read options reference

| Option | Required | Default | Notes |
|---|---|---|---|
| `table` | ✅ | — | Pinot raw table name (without `_OFFLINE` / `_REALTIME` suffix) |
| `tableType` | ✅ | — | `offline`, `realtime`, or `hybrid` |
| `controller` | ✅ | — | `host:port` of a Pinot controller |
| `broker` | ⚪️ | resolved via controller | `host:port`; skip to let the connector discover brokers |
| `useGrpcServer` | ⚪️ | `true` | set `false` to use the server HTTP (slower, legacy) |
| `queryOptions` | ⚪️ | empty | raw Pinot query options (e.g., `"timeoutMs=60000"`) |

Scroll through `pinot-connectors/pinot-spark-common/.../PinotDataSourceReadOptions.scala` for the
full list.

---

## 6. Write to Pinot

The write path is **two steps**:

1. Spark executors produce segment `.tar.gz` files at the configured `savePath`. This is all the
   connector does inside `df.write.format("pinot").save(...)`.
2. A separate step pushes those segment tars to the Pinot controller. Either hit `/v2/segments`
   directly (shown here, fine for tutorials), or use the `SparkSegmentUriPushJobRunner` /
   `SparkSegmentMetadataPushJobRunner` from the sibling
   [`pinot-batch-ingestion-spark-4`](../../../pinot-plugins/pinot-batch-ingestion/pinot-batch-ingestion-spark-4/README.md)
   module (recommended for production — handles consistent push, retry, parallelism, and
   metadata-only push for large segments already staged in deep storage).

### Step 6a. Create a target schema + table

```bash
cat > /tmp/pinot-spark4-demo/schema.json <<'EOF'
{
  "schemaName": "sparkWriteDemo",
  "dimensionFieldSpecs": [
    {"name": "id",       "dataType": "INT"},
    {"name": "name",     "dataType": "STRING"},
    {"name": "category", "dataType": "STRING"}
  ],
  "metricFieldSpecs": [
    {"name": "value", "dataType": "DOUBLE"}
  ],
  "dateTimeFieldSpecs": [
    {"name": "ts", "dataType": "LONG",
     "format": "1:MILLISECONDS:EPOCH", "granularity": "1:MILLISECONDS"}
  ]
}
EOF

cat > /tmp/pinot-spark4-demo/table.json <<'EOF'
{
  "tableName": "sparkWriteDemo",
  "tableType": "OFFLINE",
  "segmentsConfig": {
    "timeColumnName": "ts",
    "timeType": "MILLISECONDS",
    "replication": "1",
    "schemaName": "sparkWriteDemo"
  },
  "tableIndexConfig": {"loadMode": "MMAP"},
  "tenants": {"broker": "DefaultTenant", "server": "DefaultTenant"},
  "metadata": {}
}
EOF

curl -sS -X POST -H 'Content-Type: application/json' \
  -d @/tmp/pinot-spark4-demo/schema.json http://localhost:9000/schemas
curl -sS -X POST -H 'Content-Type: application/json' \
  -d @/tmp/pinot-spark4-demo/table.json http://localhost:9000/tables
```

### Step 6b. Write a DataFrame to segment tars

```python
# /tmp/pinot-spark4-demo/write.py
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType

spark = SparkSession.builder.appName("pinot-write").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

now_ms = int(time.time() * 1000)
schema = StructType([
    StructField("id",       IntegerType(), False),
    StructField("name",     StringType(),  False),
    StructField("category", StringType(),  False),
    StructField("value",    DoubleType(),  False),
    StructField("ts",       LongType(),    False),
])
rows = [
    (1,  "alpha", "cat-a",  1.5, now_ms +    0),
    (2,  "beta",  "cat-a",  2.5, now_ms + 1000),
    (3,  "gamma", "cat-b",  3.5, now_ms + 2000),
    (4,  "delta", "cat-b",  4.5, now_ms + 3000),
    (5,  "eps",   "cat-c",  5.5, now_ms + 4000),
    (6,  "zeta",  "cat-c",  6.5, now_ms + 5000),
    (7,  "eta",   "cat-a",  7.5, now_ms + 6000),
    (8,  "theta", "cat-b",  8.5, now_ms + 7000),
    (9,  "iota",  "cat-c",  9.5, now_ms + 8000),
    (10, "kappa", "cat-a", 10.5, now_ms + 9000),
]
df = spark.createDataFrame(rows, schema)

(df.write.format("pinot")
    .mode("append")
    .option("table", "sparkWriteDemo")
    .option("tableType", "OFFLINE")
    .option("segmentNameFormat", "{table}_{partitionId:03}")
    .option("invertedIndexColumns", "name,category")
    .option("timeColumnName", "ts")
    .option("timeFormat", "EPOCH|MILLISECONDS")
    .option("timeGranularity", "1:MILLISECONDS")
    .option("controller", "localhost:9000")
    .save("file:///segments"))
spark.stop()
```

Run it with an extra bind mount so the segment tars land on the host:

```bash
mkdir -p /tmp/pinot-spark4-demo/segments

docker run --rm \
  --network=container:pinot-quickstart \
  -v /tmp/pinot-spark4-demo:/jars:ro \
  -v /tmp/pinot-spark4-demo/segments:/segments \
  -e HOME=/tmp \
  spark4-jdk21:local \
  /opt/spark/bin/spark-submit \
    --jars /jars/pinot-spark-4-connector-1.6.0-SNAPSHOT-shaded.jar,/jars/commons-lang3-3.20.0.jar \
    --conf 'spark.driver.extraClassPath=/jars/commons-lang3-3.20.0.jar' \
    --conf 'spark.executor.extraClassPath=/jars/commons-lang3-3.20.0.jar' \
    --master 'local[2]' \
    --conf 'spark.log.level=WARN' \
    /jars/write.py

ls /tmp/pinot-spark4-demo/segments/
# sparkWriteDemo_000.tar.gz  sparkWriteDemo_001.tar.gz
```

### Step 6c. Push segments to the controller

```bash
for seg in /tmp/pinot-spark4-demo/segments/*.tar.gz; do
  curl -sSf -X POST -H 'UPLOAD_TYPE: SEGMENT' -F "file=@${seg}" \
    "http://localhost:9000/v2/segments?tableName=sparkWriteDemo&tableType=OFFLINE"
  echo
done
```

In production, **use a batch ingestion job instead of curl**:
`pinot-batch-ingestion-spark-4` ships `SparkSegmentTarPushJobRunner`,
`SparkSegmentUriPushJobRunner`, and `SparkSegmentMetadataPushJobRunner` that read a
`SegmentGenerationJobSpec` YAML and handle parallelism, retries, and consistent push lineage.

### Step 6d. Verify round-trip

```bash
curl -sS -X POST -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT id, name, category, value FROM sparkWriteDemo ORDER BY id"}' \
  http://localhost:8000/query/sql
```

Expect all 10 rows back with the exact ids, names, categories, and values.

You can also read them back through the connector itself (treat the write as canonical):

```bash
docker run --rm \
  --network=container:pinot-quickstart \
  -v /tmp/pinot-spark4-demo:/jars:ro \
  -e HOME=/tmp \
  spark4-jdk21:local \
  /opt/spark/bin/spark-submit \
    --jars /jars/pinot-spark-4-connector-1.6.0-SNAPSHOT-shaded.jar,/jars/commons-lang3-3.20.0.jar \
    --conf 'spark.driver.extraClassPath=/jars/commons-lang3-3.20.0.jar' \
    --conf 'spark.executor.extraClassPath=/jars/commons-lang3-3.20.0.jar' \
    --master 'local[2]' \
    --conf 'spark.log.level=WARN' \
    /jars/read.py  # adjusted to point at sparkWriteDemo
```

### Write options reference

| Option | Required | Notes |
|---|---|---|
| `table` | ✅ | Pinot raw table name |
| `tableType` | ✅ | `OFFLINE` (REALTIME is not supported on write) |
| `segmentNameFormat` | ✅ | Template; `{table}`, `{partitionId:NNN}`, `{timestamp}` placeholders |
| `timeColumnName` | ✅ if the table is time-partitioned | Must match the table's schema |
| `timeFormat` | ✅ if `timeColumnName` is set | e.g. `EPOCH|MILLISECONDS`, `SIMPLE_DATE_FORMAT|yyyyMMdd` |
| `timeGranularity` | ✅ if `timeColumnName` is set | e.g. `1:MILLISECONDS` |
| `invertedIndexColumns` / `noDictionaryColumns` / `bloomFilterColumns` / `rangeIndexColumns` | ⚪️ | Comma-separated |
| `controller` | ✅ | Controller `host:port`; used for schema lookup |
| Argument to `.save(path)` | ✅ | Destination for segment tars; `file://`, `hdfs://`, `s3a://`, `gs://` all work as long as the corresponding Hadoop FS jars are on the classpath |

The full list lives in
`pinot-connectors/pinot-spark-common/.../PinotDataSourceWriteOptions.scala`.

---

## 7. Clean up

```bash
docker rm -f pinot-quickstart
rm -rf /tmp/pinot-spark4-demo
```

---

## Known gotchas (things that tripped me up while validating this)

1. **`UnsupportedClassVersionError` on JDK 17 Spark images.** The Pinot shaded jar is class file
   65 (JDK 21). `apache/spark:4.0.0` uses JDK 17. Use `apache/spark:4.0.0-java21` or add JDK 21
   yourself as shown in §4.
2. **`NoSuchMethodError: ObjectUtils.getIfNull(...)` during the first executor task.** Spark 4's
   bundled `commons-lang3` is older than what Pinot expects. Prepend `commons-lang3:3.20.0` (or
   newer) to `spark.{driver,executor}.extraClassPath`. A follow-up PR can relocate
   `org.apache.commons.lang3.*` inside the connector's shade config to eliminate this step.
3. **gRPC connection timeout from a host-networked Spark driver.** The Pinot broker returns the
   server's **container-internal** address (`172.17.0.2:7100`). From the host that address isn't
   routable. Two fixes: (a) run Spark in a container with
   `--network=container:pinot-quickstart` as in this tutorial, or (b) configure the Pinot server
   with `pinot.server.instance.host=<externally-reachable-host>` so the broker returns an address
   the driver can actually reach.
4. **Spark 4 `DataFrameWriter.save()` requires a path.** `.save()` with no argument raises
   `IllegalStateException: Save path must be specified.`. Pass a dummy (`"unused"`) or a real
   `savePath` — the connector's `PinotDataWriter` pushes segment tars to this path via
   `hadoop.fs.FileSystem.copyFromLocalFile`, so make it a real `file://`, `hdfs://`, `s3a://`
   URI when you care about the output.
5. **`df.write.format("pinot").save(...)` does not upload segments to the controller.** It only
   produces the segment tars at `savePath`. You must separately run a push job (curl for
   demos, `SparkSegment{Tar,Uri,Metadata}PushJobRunner` in production). This is the same
   two-step flow as the Pinot Spark 3 connector.
6. **jline history `AccessDeniedException` in `spark-shell`.** Cosmetic only; set
   `-e HOME=/tmp` on `docker run` to silence it.
