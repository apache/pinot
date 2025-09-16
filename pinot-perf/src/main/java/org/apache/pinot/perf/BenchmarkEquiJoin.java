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
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
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
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkEquiJoin extends BaseClusterIntegrationTest {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder()
        .include(BenchmarkEquiJoin.class.getSimpleName())
        .addProfiler(GCProfiler.class);
    new Runner(opt.build()).run();
  }

  /// Number of rows on each segment
  private int _rowsPerSegment = 100_000;
  @Param({"10"})
  private int _segments;

  /// The ratio (in the range of `(0, 1]`) of unique rows in the table.
  ///
  /// The lower the [#_uRatio], the more rows will match in the join.
  /// The requested uniqueness will not be guaranteed. Instead it will be statistically approached by using a
  /// random number generator to create the data.
  //@Param({"1", "0.1", "0.01"})
  @Param({"1"})
  private double _uRatio;

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

  /// Executes a query where [#_rowsPerSegment] left rows are joined by int with [#_rowsPerSegment]/100 right rows
  /// and then counted
  @Benchmark
  public JsonNode countJoinOneHundredthInt()
      throws Exception {
    @Language("sql")
    String query = "SET useMultistageEngine=true;"
        + "SET maxRowsInJoin=1000000000;"
        + "SELECT count(*) "
        + "FROM MyTable t1 "
        + "JOIN MyTable t2 "
        + "ON t1.intCol = t2.intCol "
        + "WHERE t2.intCol % 10 = 0";
    ;
    return query(query);
  }

  /// Executes a query where [#_rowsPerSegment] left rows are joined by str with [#_rowsPerSegment]/100 right rows
  /// and then counted
  @Benchmark
  public JsonNode countJoinOneHundredthStr()
      throws Exception {
    @Language("sql")
    String query = "SET useMultistageEngine=true;"
        + "SELECT COUNT(*) "
        + "FROM MyTable t1 "
        + "JOIN MyTable t2 "
        + "ON t1.strCol = t2.strCol "
        + "WHERE t2.intCol % 100 = 0";
    return query(query);
  }

  /// Executes a query where [#_rowsPerSegment] left rows are joined by int with [#_rowsPerSegment]/100 right rows
  /// and then all rows projected
  @Benchmark
  public JsonNode projectJoinOneHundredthInt()
      throws Exception {
    @Language("sql")
    String query = "SET useMultistageEngine=true;"
        + "SELECT * "
        + "FROM MyTable t1 "
        + "JOIN MyTable t2 "
        + "ON t1.intCol = t2.intCol "
        + "WHERE t2.intCol % 100 = 0";
    return query(query);
  }

  /// Executes a query where [#_rowsPerSegment] left rows are joined by str with [#_rowsPerSegment]/100 right rows
  /// and then all rows projected
  @Benchmark
  public JsonNode projectJoinOneHundredthStr()
      throws Exception {
    @Language("sql")
    String query = "SET useMultistageEngine=true;"
        + "SELECT * "
        + "FROM MyTable t1 "
        + "JOIN MyTable t2 "
        + "ON t1.strCol = t2.strCol "
        + "WHERE t2.intCol % 100 = 0";
    return query(query);
  }

  @Setup
  public void setUp()
      throws Exception {

    Distribution.DataSupplier supplier = createSupplier();

    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    CompletableFuture<?> clusterStarted = CompletableFuture.runAsync(() -> {
      try {
        // Start the Pinot cluster
        startZk();
        startController();
        startBroker();
        startServers(2);

        // upload test data
        addSchema(SCHEMA);
        addTableConfig(TABLE_CONFIG);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    CompletableFuture[] futures = new CompletableFuture[_segments];
    for (int i = 0; i < _segments; i++) {
      futures[i] = buildSegment("segment" + i, supplier);
    }
    CompletableFuture.allOf(futures).join();

    clusterStarted.join();
    uploadSegments(TABLE_NAME, _tarDir);

    //check for data to arrive
    waitForAllDocsLoaded(60000);
  }

  @Override
  protected long getCountStarResult() {
    return _rowsPerSegment * (long) _segments;
  }

  private CompletableFuture<?> buildSegment(String segmentName, Distribution.DataSupplier supplier) {
    return CompletableFuture.runAsync(() -> {
      try {
        LazyDataGenerator rows = createDataGenerator(supplier);
        SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
        config.setOutDir(_segmentDir.getPath());
        config.setTableName(TABLE_NAME);
        config.setSegmentName(segmentName);

        SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
        try (RecordReader recordReader = new GeneratedDataRecordReader(rows)) {
          driver.init(config, recordReader);
          driver.build();
        }
        //save generator state so that other segments are not identical to this one
        supplier.snapshot();

        // Tar the segment
        File indexDir = new File(_segmentDir, segmentName);
        File segmentTarFile = new File(_tarDir, segmentName + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
        TarCompressionUtils.createCompressedTarFile(indexDir, segmentTarFile);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private LazyDataGenerator createDataGenerator(Distribution.DataSupplier supplier) {
    return new LazyDataGenerator() {
      Distribution.DataSupplier _supplier = supplier.clone();

      @Override
      public int size() {
        return _rowsPerSegment;
      }

      @Override
      public GenericRow next(GenericRow row, int index) {
        long randomValue = _supplier.getAsLong();
        row.putValue(INT_COL_NAME, (int) randomValue);
        row.putValue(LONG_COL_NAME, randomValue);
        row.putValue(STRING_COL_NAME, "value" + randomValue);
        return row;
      }

      @Override
      public void rewind() {
        _supplier = supplier.clone();
      }
    };
  }

  private Distribution.DataSupplier createSupplier() {
    int actualTableRows = _rowsPerSegment * _segments;
    Preconditions.checkState(_uRatio > 0 && _uRatio <= 1.0, "_uRatio must be in the range of (0, 1]");
    int uniqueRows = _uRatio == 1 ? actualTableRows : (int) (actualTableRows * _uRatio);

    return Distribution.UNIFORM.createSupplier(42, 0, uniqueRows);
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

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    super.overrideBrokerConf(brokerConf);
    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    super.overrideServerConf(serverConf);
    serverConf.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);
  }

  private static final String TABLE_NAME = "MyTable";
  private static final String INT_COL_NAME = "intCol";
  private static final String LONG_COL_NAME = "longCol";
  private static final String STRING_COL_NAME = "strCol";
  private static final List<FieldConfig> FIELD_CONFIGS = new ArrayList<>();

  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
      .setInvertedIndexColumns(List.of(INT_COL_NAME, LONG_COL_NAME, STRING_COL_NAME))
      .setFieldConfigList(FIELD_CONFIGS)
      .build();

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
      .addSingleValueDimension(INT_COL_NAME, FieldSpec.DataType.INT)
      .addSingleValueDimension(LONG_COL_NAME, FieldSpec.DataType.LONG)
      .addSingleValueDimension(STRING_COL_NAME, FieldSpec.DataType.STRING)
      .build();
}
