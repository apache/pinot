/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.perf;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.function.FunctionUtils;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.intellij.lang.annotations.Language;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * JMH benchmark comparing multi-stage query latency between the legacy stats mode and the
 * {@code SubmitWithStream} stream-stats mode.
 *
 * <p>Three benchmark methods cover representative MSQE plan shapes:
 * <ul>
 *   <li>{@link #aggregation()} — single leaf stage feeding a root aggregate.</li>
 *   <li>{@link #tripleJoin()} — three-way self-join: 3 leaf stages + 2 join intermediates + 1 aggregate.</li>
 *   <li>{@link #sleep()} — single leaf scan with {@code sleep} applied per row; requires {@code -ea} to actually
 *       sleep (wired in {@link #main}). With assertions enabled each of the ~20 matching rows per server sleeps 1 ms,
 *       giving a stable ~20 ms query duration to isolate stats-path overhead.</li>
 * </ul>
 *
 * <p>Four concurrent client threads ({@link Threads}) share one embedded two-server cluster
 * ({@link State Scope.Benchmark}), exposing lock-contention and gRPC-stream overhead that only surfaces under
 * concurrency.
 *
 * <p>Example run:
 * <pre>
 *   mvn package -pl pinot-perf -DskipTests
 *   java -jar pinot-perf/target/benchmarks.jar BenchmarkStreamStatsMSQE
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Threads(4)
@State(Scope.Benchmark)
public class BenchmarkStreamStatsMSQE extends BaseClusterIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkStreamStatsMSQE.class);

  private static final String TABLE_NAME = "BenchTable";
  private static final String INT_COL = "intCol";
  private static final String STR_COL = "strCol";
  private static final String METRIC_COL = "metric";

  /// Four segments distributed round-robin across two servers → 2 segments per server.
  private static final int SEGMENTS = 4;

  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(TABLE_NAME)
      .setInvertedIndexColumns(List.of(INT_COL))
      .build();

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName(TABLE_NAME)
      .addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
      .addSingleValueDimension(STR_COL, FieldSpec.DataType.STRING)
      .addMetric(METRIC_COL, FieldSpec.DataType.LONG)
      .build();

  /**
   * Stats reporting mode:
   * <ul>
   *   <li>{@code "legacy"} — standard opchain-stats path (existing behaviour).</li>
   *   <li>{@code "stream"} — enables {@code streamStats=true} per-query, activating the
   *   {@code SubmitWithStream} bidi-RPC path.</li>
   * </ul>
   */
  @Param({"legacy", "stream"})
  private String _statsMode;

  @Param("10000")
  private int _numRows;

  @Override
  protected String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public boolean useMultiStageQueryEngine() {
    return true;
  }

  @Override
  protected long getCountStarResult() {
    return (long) _numRows * SEGMENTS;
  }

  @Setup
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    CompletableFuture<?> clusterReady = CompletableFuture.runAsync(() -> {
      try {
        startZk();
        startController();
        startBroker();
        startServers(2);
        addSchema(SCHEMA);
        addTableConfig(TABLE_CONFIG);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    if (!FunctionUtils.isAssertEnabled()) {
      LOGGER.warn("JVM assertions are disabled (-ea not set). The 'sleep' benchmark will measure near-zero "
          + "latency instead of the intended ~20 ms per query. Run via main() or append -ea to get meaningful "
          + "sleep results. The 'aggregation' and 'tripleJoin' benchmarks are unaffected.");
    }

    // Pre-create one independent supplier clone per segment so that parallel builds have no shared mutable state.
    // All segments use the same seed-42 origin (identical data), which is intentional for a latency benchmark.
    Distribution.DataSupplier baseSupplier = Distribution.UNIFORM.createSupplier(42, 0, _numRows);
    CompletableFuture<?>[] segFutures = new CompletableFuture<?>[SEGMENTS];
    for (int i = 0; i < SEGMENTS; i++) {
      segFutures[i] = buildSegment("seg" + i, baseSupplier.clone());
    }
    CompletableFuture.allOf(segFutures).join();

    clusterReady.join();
    uploadSegments(TABLE_NAME, _tarDir);
    waitForAllDocsLoaded(TABLE_NAME, 60_000);
  }

  @TearDown
  public void tearDown()
      throws IOException {
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteQuietly(_tempDir);
  }

  /**
   * Single leaf stage feeding a root aggregate — the minimal MSQE plan shape.
   */
  @Benchmark
  public JsonNode aggregation()
      throws Exception {
    return executeQuery("SELECT COUNT(*), SUM(" + METRIC_COL + ") FROM " + TABLE_NAME);
  }

  /**
   * Three-way self-join: 3 leaf scan stages + 2 join intermediates + 1 aggregate (6 stages total). Exercises the
   * stats fan-in across the full plan simultaneously.
   */
  @Benchmark
  public JsonNode tripleJoin()
      throws Exception {
    return executeQuery("SELECT count(*) "
        + "FROM " + TABLE_NAME + " t1 "
        + "JOIN " + TABLE_NAME + " t2 ON t1." + INT_COL + " = t2." + INT_COL + " "
        + "JOIN " + TABLE_NAME + " t3 ON t2." + INT_COL + " = t3." + INT_COL + " "
        + "WHERE t2." + INT_COL + " % 10 = 0");
  }

  /**
   * Single-stage scan with {@code sleep} applied per matching row. The filter {@code intCol % 1000 = 42} selects
   * roughly 10 values per segment → ~20 rows per server. The sleep argument references {@code intCol} to prevent
   * Calcite from constant-folding the literal and collapsing the call to a single broker-side evaluation.
   * With {@code -ea} each row sleeps 1 ms giving a stable ~20 ms query duration.
   */
  @Benchmark
  public JsonNode sleep()
      throws Exception {
    // CASE references intCol to block constant-folding; all matching rows have intCol > 0 so always sleeps 1 ms.
    return executeQuery("SELECT sleep(CASE WHEN " + INT_COL + " > 0 THEN 1 ELSE 0 END) "
        + "FROM " + TABLE_NAME + " WHERE " + INT_COL + " % 1000 = 42");
  }

  private JsonNode executeQuery(@Language("sql") String sql)
      throws Exception {
    String queryOptions = "maxRowsInJoin=100000000";
    if ("stream".equals(_statsMode)) {
      queryOptions += ";streamStats=true";
    }
    JsonNode result = postQuery(sql,
        ClusterIntegrationTestUtils.getBrokerQueryApiUrl(getBrokerBaseApiUrl(), true),
        null,
        Map.of("queryOptions", queryOptions));
    JsonNode exceptions = result.get("exceptions");
    if (exceptions != null && exceptions.size() > 0) {
      JsonNode first = exceptions.get(0);
      if (first != null && !first.isNull()) {
        throw new RuntimeException(first.path("message").asText("unknown error"));
      }
    }
    return result;
  }

  /// Each call receives its own pre-cloned supplier so there is no shared mutable state between parallel builds.
  private CompletableFuture<?> buildSegment(String segmentName, Distribution.DataSupplier segmentSupplier) {
    return CompletableFuture.runAsync(() -> {
      try {
        LazyDataGenerator rows = createDataGenerator(segmentSupplier);
        SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
        config.setOutDir(_segmentDir.getPath());
        config.setTableName(TABLE_NAME);
        config.setSegmentName(segmentName);

        SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
        try (RecordReader reader = new GeneratedDataRecordReader(rows)) {
          driver.init(config, reader);
          driver.build();
        }

        File indexDir = new File(_segmentDir, segmentName);
        File tarFile = new File(_tarDir, segmentName + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
        TarCompressionUtils.createCompressedTarFile(indexDir, tarFile);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private LazyDataGenerator createDataGenerator(Distribution.DataSupplier supplier) {
    return new LazyDataGenerator() {
      Distribution.DataSupplier _supplier = supplier;

      @Override
      public int size() {
        return _numRows;
      }

      @Override
      public GenericRow next(GenericRow row, int index) {
        int value = (int) _supplier.getAsLong();
        row.putValue(INT_COL, value);
        row.putValue(STR_COL, "str" + value);
        row.putValue(METRIC_COL, (long) value);
        return row;
      }

      @Override
      public void rewind() {
        _supplier.reset();
      }
    };
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder()
        .include(BenchmarkStreamStatsMSQE.class.getSimpleName())
        // -ea activates FunctionUtils.isAssertEnabled() so that sleep(1) actually sleeps.
        .jvmArgsAppend("-ea");
    new Runner(opt.build()).run();
  }
}
