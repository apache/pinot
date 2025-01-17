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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
 * Benchmark similar to BenchmarkQueries, but using multi-stage query engine.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkQueriesMSQE extends BaseClusterIntegrationTest {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkQueriesMSQE.class.getSimpleName());
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

  public static final String REGEXP_LIKE_CONST_QUERY = "select * from \n"
      + "(\n"
      + "  select RAW_STRING_COL\n"
      + "  from MyTable \n"
      + "  limit 100000\n"
      + ") \n"
      + "where regexp_like_const('.*a.*', RAW_STRING_COL )";

  public static final String REGEXP_LIKE_VAR_QUERY = "select * from \n"
      + "(\n"
      + "  select RAW_STRING_COL\n"
      + "  from MyTable \n"
      + "  limit 100000\n"
      + ") \n"
      + "where regexp_like('.*a.*', RAW_STRING_COL )";

  private Distribution.DataSupplier _supplier;

  @Param("1500000")
  private int _numRows;

  @Param({"EXP(0.001)", "EXP(0.5)", "EXP(0.999)"})
  String _scenario;

  @Param({
      REGEXP_LIKE_CONST_QUERY, REGEXP_LIKE_VAR_QUERY
  })
  String _query;

  @Setup
  public void setUp()
      throws Exception {
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
    LazyDataGenerator rows = BenchmarkQueries.createTestData(_numRows, _supplier);
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
