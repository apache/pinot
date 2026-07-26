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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * Benchmarks the additive INNER-join probe-side runtime filter ({@code PinotJoinToInnerRuntimeFilterRule})
 * on the canonical large-fact &#8904; small/selective-dim shape. It is a self-join of a 1M-row table with
 * <b>unique</b> keys (so the join is 1:1) where a {@code WHERE} on the build (right) side controls how
 * selective — i.e. how small — the build is, via {@code t2.intCol % buildMod = 0}:
 * <ul>
 *   <li>{@code buildMod = 10000} -&gt; 0.01% of the build survives (100 rows): the runtime filter reduces
 *       the probe to those 100 keys before it is shuffled into the join — the intended win. AUTO uses the
 *       exact-IN tier here.</li>
 *   <li>{@code buildMod = 1} -&gt; 100% build (every row): the filter cannot reduce the probe (every key
 *       matches) and only adds the build-key broadcast + reducer overhead — the regression/cost case the
 *       feature must be left off for. AUTO uses the bloom tier here (build &gt; maxInSize).</li>
 * </ul>
 * {@code filter = off} is the baseline (no hint); {@code filter = auto} enables the runtime filter via the
 * join hint (the cluster default stays off).
 *
 * <p>The {@code select*} methods <b>project a realistically wide probe row</b> (two numerics plus three
 * strings) through the join, so the probe-side shuffle — the thing the feature reduces, and the stated
 * motivation of a wide fact table being shuffled needlessly — dominates. The {@code countJoinInt} method
 * (only the join key crosses the network) is included as a reference for the overhead floor when there is
 * almost nothing to shuffle: it isolates the case where the reducer cannot pay for itself.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkRuntimeFilterJoin extends BaseClusterIntegrationTest {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder()
        .include(BenchmarkRuntimeFilterJoin.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  /// Number of rows on each segment.
  private final int _rowsPerSegment = 100_000;
  @Param({"10"})
  private int _segments;

  /// Reducer mode: "off" (baseline, no hint) or a runtime_filter hint value ("auto", "in", "bloom").
  @Param({"off", "auto"})
  private String _filter;

  /// Build selectivity: the build keeps rows where {@code intCol % buildMod == 0}. The two values are the
  /// extremes — 10000 (~0.01% build, the win, AUTO uses exact IN) and 1 (100% build, the cost, AUTO uses
  /// bloom). The ~1% mid-point (buildMod=100) is omitted because it straddles the AUTO IN/bloom threshold.
  @Param({"10000", "1"})
  private int _buildMod;

  private JsonNode query(String query)
      throws Exception {
    JsonNode result = postQuery(query,
        ClusterIntegrationTestUtils.getBrokerQueryApiUrl(getBrokerBaseApiUrl(), true),
        null,
        getExtraQueryProperties());
    JsonNode exceptions = result.get("exceptions").get(0);
    if (exceptions != null) {
      throw new RuntimeException(exceptions.get("message").asText());
    }
    return result.get("resultTable").get("rows");
  }

  @Override
  protected Map<String, String> getExtraQueryProperties() {
    return Map.of("useMultistageEngine", "true", "dropResults", "true");
  }

  private String hint() {
    return _filter.equals("off") ? "" : "/*+ joinOptions(runtime_filter='" + _filter + "') */ ";
  }

  /// Projects the probe rows through an INT-key join, so the probe shuffle dominates: the runtime filter's
  /// probe reduction is the main lever here.
  @Benchmark
  public JsonNode selectJoinInt()
      throws Exception {
    @Language("sql")
    String query = "SET useMultistageEngine=true;"
        + "SET maxRowsInJoin=1000000000;"
        + "SELECT " + hint() + PROBE_PROJECTION + " "
        + "FROM MyTable t1 "
        + "JOIN MyTable t2 "
        + "ON t1.intCol = t2.intCol "
        + "WHERE t2.intCol % " + _buildMod + " = 0";
    return query(query);
  }

  /// Same projected shape on a STRING key, which exercises the bloom-on-string reducer (no range predicate).
  @Benchmark
  public JsonNode selectJoinStr()
      throws Exception {
    @Language("sql")
    String query = "SET useMultistageEngine=true;"
        + "SET maxRowsInJoin=1000000000;"
        + "SELECT " + hint() + PROBE_PROJECTION + " "
        + "FROM MyTable t1 "
        + "JOIN MyTable t2 "
        + "ON t1.strCol = t2.strCol "
        + "WHERE t2.intCol % " + _buildMod + " = 0";
    return query(query);
  }

  /// Reference: only the join key crosses the network (count, no projection), so there is little to
  /// shuffle — this isolates the reducer's fixed overhead (the floor below which the feature cannot win).
  @Benchmark
  public JsonNode countJoinInt()
      throws Exception {
    @Language("sql")
    String query = "SET useMultistageEngine=true;"
        + "SET maxRowsInJoin=1000000000;"
        + "SELECT " + hint() + "count(*) "
        + "FROM MyTable t1 "
        + "JOIN MyTable t2 "
        + "ON t1.intCol = t2.intCol "
        + "WHERE t2.intCol % " + _buildMod + " = 0";
    return query(query);
  }

  @Setup
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    CompletableFuture<?> clusterStarted = CompletableFuture.runAsync(() -> {
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

    CompletableFuture[] futures = new CompletableFuture[_segments];
    for (int i = 0; i < _segments; i++) {
      futures[i] = buildSegment("segment" + i, (long) i * _rowsPerSegment);
    }
    CompletableFuture.allOf(futures).join();

    clusterStarted.join();
    uploadSegments(TABLE_NAME, _tarDir);
    waitForAllDocsLoaded(60000);
  }

  @Override
  protected long getCountStarResult() {
    return _rowsPerSegment * (long) _segments;
  }

  private CompletableFuture<?> buildSegment(String segmentName, long keyBase) {
    return CompletableFuture.runAsync(() -> {
      try {
        LazyDataGenerator rows = createDataGenerator(keyBase);
        SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
        config.setOutDir(_segmentDir.getPath());
        config.setTableName(TABLE_NAME);
        config.setSegmentName(segmentName);

        SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
        try (RecordReader recordReader = new GeneratedDataRecordReader(rows)) {
          driver.init(config, recordReader);
          driver.build();
        }

        File indexDir = new File(_segmentDir, segmentName);
        File segmentTarFile = new File(_tarDir, segmentName + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
        TarCompressionUtils.createCompressedTarFile(indexDir, segmentTarFile);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  /// Deterministic, globally-unique keys ({@code keyBase + index}) so the self-join is 1:1 and the build
  /// selectivity (and thus the probe reduction) is exact rather than statistical.
  private LazyDataGenerator createDataGenerator(long keyBase) {
    return new LazyDataGenerator() {
      @Override
      public int size() {
        return _rowsPerSegment;
      }

      @Override
      public GenericRow next(GenericRow row, int index) {
        long value = keyBase + index;
        row.putValue(INT_COL_NAME, (int) value);
        row.putValue(LONG_COL_NAME, value);
        row.putValue(DOUBLE_COL_NAME, (double) value);
        row.putValue(STRING_COL_NAME, "value" + value);
        row.putValue(STR2_COL_NAME, "the_quick_brown_fox_jumps_over_" + value);
        row.putValue(STR3_COL_NAME, "lorem_ipsum_dolor_sit_amet_consectetur_" + value);
        return row;
      }

      @Override
      public void rewind() {
      }
    };
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

  private static final String TABLE_NAME = "MyTable";
  private static final String INT_COL_NAME = "intCol";
  private static final String LONG_COL_NAME = "longCol";
  private static final String DOUBLE_COL_NAME = "doubleCol";
  private static final String STRING_COL_NAME = "strCol";
  private static final String STR2_COL_NAME = "str2Col";
  private static final String STR3_COL_NAME = "str3Col";

  /// The wide probe-row projection that crosses the network on the probe side of the join.
  private static final String PROBE_PROJECTION =
      "t1.intCol, t1.longCol, t1.doubleCol, t1.strCol, t1.str2Col, t1.str3Col";

  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
      .setInvertedIndexColumns(List.of(INT_COL_NAME, STRING_COL_NAME))
      .build();

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
      .addSingleValueDimension(INT_COL_NAME, FieldSpec.DataType.INT)
      .addSingleValueDimension(LONG_COL_NAME, FieldSpec.DataType.LONG)
      .addSingleValueDimension(DOUBLE_COL_NAME, FieldSpec.DataType.DOUBLE)
      .addSingleValueDimension(STRING_COL_NAME, FieldSpec.DataType.STRING)
      .addSingleValueDimension(STR2_COL_NAME, FieldSpec.DataType.STRING)
      .addSingleValueDimension(STR3_COL_NAME, FieldSpec.DataType.STRING)
      .build();
}
