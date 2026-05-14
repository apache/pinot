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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
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


/**
 * JMH benchmark comparing multi-stage query throughput between legacy stats mode and stream-stats mode.
 *
 * <p>Two concurrent client threads simulate parallel in-flight queries to expose any lock contention or
 * connection-multiplexing overhead that only surfaces under concurrency. Two benchmark methods cover representative MSE
 * query shapes: a single-stage aggregation and a two-stage self-join.
 *
 * <p>Example run:
 * <pre>
 *   mvn package -pl pinot-perf -DskipTests
 *   java -jar pinot-perf/target/benchmarks.jar BenchmarkStreamStatsMSQE
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 10)
@Threads(2)
@State(Scope.Benchmark)
public class BenchmarkStreamStatsMSQE extends BaseClusterIntegrationTest {

  private static final String TABLE_NAME = "BenchTable";
  private static final String INT_COL = "intCol";
  private static final String STR_COL = "strCol";

  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName(TABLE_NAME)
      .addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
      .addSingleValueDimension(STR_COL, FieldSpec.DataType.STRING)
      .addMetric("metric", FieldSpec.DataType.LONG)
      .build();

  /**
   * Stats reporting mode:
   * <ul>
   *   <li>{@code "legacy"} — standard opchain-stats path (existing behavior).</li>
   *   <li>{@code "stream"} — enables {@code streamStats=true} per-query option, activating the
   *   {@code SubmitWithStream} bidi-RPC path.</li>
   * </ul>
   */
  @Param({"legacy", "stream"})
  private String _statsMode;

  @Param("500000")
  private int _numRows;

  @Override
  public boolean useMultiStageQueryEngine() {
    return true;
  }

  @Override
  protected Map<String, String> getExtraQueryProperties() {
    if ("stream".equals(_statsMode)) {
      return Map.of(CommonConstants.Broker.Request.QueryOptionKey.STREAM_STATS, "true");
    }
    return Map.of();
  }

  @Setup
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();
    startController();
    startBroker();
    startServer();

    addSchema(SCHEMA);
    addTableConfig(TABLE_CONFIG);

    buildAndUploadSegment("seg0");
    buildAndUploadSegment("seg1");

    waitForAllDocsLoaded(60_000L);
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
   * Single-stage aggregation — exercises the leaf→root stats path through the broker accumulator.
   */
  @Benchmark
  public JsonNode aggregation()
      throws Exception {
    return postQuery("SELECT COUNT(*), SUM(metric) FROM " + TABLE_NAME);
  }

  /**
   * Two-stage self-join — exercises the multi-stage stats path (leaf stage + join stage + root stage), which
   * involves multiple opchain completions per query and a richer stats tree at the broker.
   */
  @Benchmark
  public JsonNode join()
      throws Exception {
    return postQuery(
        "SELECT a." + INT_COL + ", COUNT(*) FROM " + TABLE_NAME + " a "
            + "JOIN " + TABLE_NAME + " b ON a." + INT_COL + " = b." + INT_COL
            + " GROUP BY a." + INT_COL + " LIMIT 100");
  }

  @Override
  protected long getCountStarResult() {
    return _numRows * 2L;
  }

  // ---- helpers ----

  private void buildAndUploadSegment(String segmentName)
      throws Exception {
    final int rows = _numRows;
    LazyDataGenerator data = new LazyDataGenerator() {
      @Override
      public int size() {
        return rows;
      }

      @Override
      public GenericRow next(GenericRow row, int i) {
        row.putValue(INT_COL, i % 1000);
        row.putValue(STR_COL, "str" + (i % 100));
        row.putValue("metric", (long) i);
        return null;
      }

      @Override
      public void rewind() {
      }
    };

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setOutDir(_segmentDir.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader reader = new GeneratedDataRecordReader(data)) {
      driver.init(config, reader);
      driver.build();
    }

    File indexDir = new File(_segmentDir, segmentName);
    File tarFile = new File(_tarDir, segmentName + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    TarCompressionUtils.createCompressedTarFile(indexDir, tarFile);
    uploadSegments(TABLE_NAME, _tarDir);
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkStreamStatsMSQE.class.getSimpleName());
    new Runner(opt.build()).run();
  }
}
