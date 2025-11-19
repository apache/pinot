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
import io.grpc.NameResolverRegistry;
import io.grpc.internal.DnsNameResolverProvider;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.NetUtils;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * Benchmark distinct queries using multi-stage query engine.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkDistinctQueriesMSQE extends BaseClusterIntegrationTest {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkDistinctQueriesMSQE.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  private static final String TABLE_NAME = "MyTable";
  private static final String FIRST_SEGMENT_NAME = "firstTestSegment";
  private static final String SECOND_SEGMENT_NAME = "secondTestSegment";
  private static final String INT_COL_NAME = "INT_COL";
  private static final String SORTED_COL_NAME = "SORTED_COL";
  private static final String RAW_INT_COL_NAME = "RAW_INT_COL";
  private static final String RAW_STRING_COL_NAME = "RAW_STRING_COL";
  private static final String NO_INDEX_INT_COL_NAME = "NO_INDEX_INT_COL";
  private static final String NO_INDEX_STRING_COL = "NO_INDEX_STRING_COL";
  private static final String LOW_CARDINALITY_STRING_COL = "LOW_CARDINALITY_STRING_COL";
  private static final String TIMESTAMP_COL = "TSTMP_COL";
  private static final List<FieldConfig> FIELD_CONFIGS = new ArrayList<>();
  private static final String TRACKING_QUERY_OPTIONS =
      "maxRowsInDistinct=2000000000;numRowsWithoutChangeInDistinct=2000000000;maxExecutionTimeMsInDistinct=600000";

  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(TABLE_NAME)
      .setInvertedIndexColumns(List.of(INT_COL_NAME, LOW_CARDINALITY_STRING_COL))
      .setFieldConfigList(FIELD_CONFIGS)
      .setNoDictionaryColumns(List.of(RAW_INT_COL_NAME, RAW_STRING_COL_NAME, TIMESTAMP_COL))
      .setSortedColumn(SORTED_COL_NAME)
      .setRangeIndexColumns(List.of(INT_COL_NAME, LOW_CARDINALITY_STRING_COL))
      .setStarTreeIndexConfigs(
          Collections.singletonList(
              new StarTreeIndexConfig(List.of(SORTED_COL_NAME, INT_COL_NAME), null,
                  Collections.singletonList(
                      new AggregationFunctionColumnPair(AggregationFunctionType.SUM, RAW_INT_COL_NAME).toColumnName()),
                  null, Integer.MAX_VALUE))).build();

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName(TABLE_NAME)
      .addSingleValueDimension(SORTED_COL_NAME, FieldSpec.DataType.INT)
      .addSingleValueDimension(NO_INDEX_INT_COL_NAME, FieldSpec.DataType.INT)
      .addSingleValueDimension(RAW_INT_COL_NAME, FieldSpec.DataType.INT)
      .addSingleValueDimension(INT_COL_NAME, FieldSpec.DataType.INT)
      .addSingleValueDimension(RAW_STRING_COL_NAME, FieldSpec.DataType.STRING)
      .addSingleValueDimension(NO_INDEX_STRING_COL, FieldSpec.DataType.STRING)
      .addSingleValueDimension(LOW_CARDINALITY_STRING_COL, FieldSpec.DataType.STRING)
      .addSingleValueDimension(TIMESTAMP_COL, FieldSpec.DataType.TIMESTAMP)
      .build();

  public static final String DISTINCT_LOW_CARDINALITY_QUERY =
      "SELECT DISTINCT LOW_CARDINALITY_STRING_COL FROM MyTable LIMIT 1000";
  public static final String DISTINCT_INT_QUERY =
      "SELECT DISTINCT INT_COL FROM MyTable LIMIT 100000";
  public static final String DISTINCT_RAW_STRING_QUERY =
      "SELECT DISTINCT RAW_STRING_COL FROM MyTable LIMIT 100000";
  public static final String DISTINCT_MULTI_COL_QUERY =
      "SELECT DISTINCT INT_COL, LOW_CARDINALITY_STRING_COL FROM MyTable LIMIT 100000";
  public static final String DISTINCT_ORDER_BY_QUERY =
      "SELECT DISTINCT INT_COL FROM MyTable ORDER BY INT_COL DESC LIMIT 100000";
  public static final String DISTINCT_FILTERED_RAW_STRING_QUERY =
      "SELECT DISTINCT RAW_STRING_COL FROM MyTable WHERE LOW_CARDINALITY_STRING_COL = 'value1' LIMIT 100000";

  private Distribution.DataSupplier _supplier;
  private int _brokerQueryRunnerPort;
  private int _serverQueryServicePort;
  private int _serverQueryRunnerPort;

  @Param("1500000")
  private int _numRows;

  @Param({"EXP(0.001)", "EXP(0.5)", "EXP(0.999)"})
  String _scenario;

  @Param({"enabled", "disabled"})
  String _trackingMode;

  @Param({
      DISTINCT_LOW_CARDINALITY_QUERY, DISTINCT_INT_QUERY, DISTINCT_RAW_STRING_QUERY, DISTINCT_MULTI_COL_QUERY,
      DISTINCT_ORDER_BY_QUERY, DISTINCT_FILTERED_RAW_STRING_QUERY
  })
  String _query;

  @Setup
  public void setUp()
      throws Exception {
    // Ensure DNS resolver is registered so gRPC can resolve host:port targets in the shaded benchmark jar.
    NameResolverRegistry.getDefaultRegistry().register(new DnsNameResolverProvider());
    int basePort = NetUtils.findOpenPort(24000);
    _brokerQueryRunnerPort = basePort;
    _serverQueryServicePort = NetUtils.findOpenPort(basePort + 1);
    _serverQueryRunnerPort = NetUtils.findOpenPort(_serverQueryServicePort + 1);
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    _supplier = Distribution.createSupplier(42, _scenario);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // upload test data
    addSchema(SCHEMA);
    addTableConfig(TABLE_CONFIG);

    buildSegment(FIRST_SEGMENT_NAME);
    buildSegment(SECOND_SEGMENT_NAME);

    uploadSegments(TABLE_NAME, _tarDir);

    //check for data to arrive
    waitForAllDocsLoaded(60000);
  }

  @Override
  protected long getCountStarResult() {
    return _numRows * 2;
  }

  @Override
  protected Map<String, String> getExtraQueryProperties() {
    if ("enabled".equals(_trackingMode)) {
      return Map.of("queryOptions", TRACKING_QUERY_OPTIONS);
    }
    return Map.of();
  }

  @Override
  protected void overrideBrokerConf(org.apache.pinot.spi.env.PinotConfiguration brokerConf) {
    brokerConf.setProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, _brokerQueryRunnerPort);
  }

  @Override
  protected void overrideServerConf(org.apache.pinot.spi.env.PinotConfiguration serverConf) {
    serverConf.setProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_SERVER_PORT, _serverQueryServicePort);
    serverConf.setProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, _serverQueryRunnerPort);
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

  @Benchmark
  public JsonNode query()
      throws Exception {
    JsonNode result =
        postQuery(_query, ClusterIntegrationTestUtils.getBrokerQueryApiUrl(getBrokerBaseApiUrl(), true), null,
            getExtraQueryProperties());
    JsonNode exceptions = result.get("exceptions").get(0);
    if (exceptions != null) {
      throw new RuntimeException(exceptions.get("message").asText());
    }
    return result.get("resultTable").get("rows");
  }

  private void buildSegment(String segmentName)
      throws Exception {
    LazyDataGenerator rows = BenchmarkQueriesSSQE.createTestData(_numRows, _supplier);
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
    _supplier.snapshot();

    // Tar the segment
    File indexDir = new File(_segmentDir, segmentName);
    File segmentTarFile = new File(_tarDir, segmentName + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    TarCompressionUtils.createCompressedTarFile(indexDir, segmentTarFile);
  }
}
