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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.queries.BaseQueriesTest;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.UuidUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
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
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * Compares query execution performance for UUID values stored in three representations:
 * <ul>
 *   <li><b>STRING</b> — canonical RFC 4122 form, e.g. {@code "550e8400-e29b-41d4-a716-446655440000"} (36-char string
 *   key in group-by)</li>
 *   <li><b>BYTES</b> — raw 16-byte binary; group-by key is a {@code ByteArray} (heap allocation per row)</li>
 *   <li><b>UUID</b> — native {@code DataType.UUID}; group-by key is a {@code UuidKey} (two {@code long} fields,
 *   no heap allocation)</li>
 * </ul>
 * Each representation is tested with both raw (no-dictionary) and dictionary-encoded columns. The benchmarked
 * operations are GROUP BY, COUNT(DISTINCT), and equality-filter COUNT(*).
 *
 * <p>Run with:
 * <pre>
 *   ./mvnw package -DskipTests -pl pinot-perf -am -Ppinot-fastdev
 *   java -jar pinot-perf/target/benchmarks.jar BenchmarkUuidQueryExecution
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@State(Scope.Benchmark)
public class BenchmarkUuidQueryExecution extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "BenchmarkUuidQueryExecution");
  private static final String TABLE_NAME = "uuidBenchTable";
  private static final String SEGMENT_NAME = "uuidBenchSegment";

  // STRING columns: UUID in canonical "8-4-4-4-12" dash-separated form
  private static final String STR_UUID_RAW = "str_uuid_raw";
  private static final String STR_UUID_DICT = "str_uuid_dict";
  // BYTES columns: UUID as raw 16-byte binary; group-by uses ByteArray
  private static final String BYTES_UUID_RAW = "bytes_uuid_raw";
  private static final String BYTES_UUID_DICT = "bytes_uuid_dict";
  // UUID columns: native DataType.UUID; group-by uses UuidKey (two longs)
  private static final String UUID_RAW = "uuid_raw";
  private static final String UUID_DICT = "uuid_dict";
  // Two-LONG columns: UUID split into MSB + LSB; GROUP BY both simultaneously
  private static final String LONG_MSB_RAW = "long_msb_raw";
  private static final String LONG_LSB_RAW = "long_lsb_raw";
  private static final String LONG_MSB_DICT = "long_msb_dict";
  private static final String LONG_LSB_DICT = "long_lsb_dict";

  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(TABLE_NAME)
      .setNoDictionaryColumns(List.of(STR_UUID_RAW, BYTES_UUID_RAW, UUID_RAW, LONG_MSB_RAW, LONG_LSB_RAW))
      .build();

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(STR_UUID_RAW, FieldSpec.DataType.STRING)
      .addSingleValueDimension(STR_UUID_DICT, FieldSpec.DataType.STRING)
      .addSingleValueDimension(BYTES_UUID_RAW, FieldSpec.DataType.BYTES)
      .addSingleValueDimension(BYTES_UUID_DICT, FieldSpec.DataType.BYTES)
      .addSingleValueDimension(UUID_RAW, FieldSpec.DataType.UUID)
      .addSingleValueDimension(UUID_DICT, FieldSpec.DataType.UUID)
      .addSingleValueDimension(LONG_MSB_RAW, FieldSpec.DataType.LONG)
      .addSingleValueDimension(LONG_LSB_RAW, FieldSpec.DataType.LONG)
      .addSingleValueDimension(LONG_MSB_DICT, FieldSpec.DataType.LONG)
      .addSingleValueDimension(LONG_LSB_DICT, FieldSpec.DataType.LONG)
      .build();

  /** Total rows in the segment. */
  @Param("500000")
  private int _numRows;

  /** Number of distinct UUID values. Controls group-by cardinality. */
  @Param("1000")
  private int _numUniqueUuids;

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  /** UUID string used in equality-filter benchmarks (RFC 4122 form). */
  private String _filterUuidString;
  /** Same UUID as hex (no dashes) for BYTES column equality filter. */
  private String _filterBytesHex;
  /** MSB of the filter UUID, for two-long filter benchmarks. */
  private long _filterMsb;
  /** LSB of the filter UUID, for two-long filter benchmarks. */
  private long _filterLsb;

  @Setup
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    INDEX_DIR.mkdirs();

    Random random = new Random(42L);

    // Pre-generate the UUID value pool
    String[] uuidStrings = new String[_numUniqueUuids];
    byte[][] uuidBytesArr = new byte[_numUniqueUuids][];
    long[] msbArr = new long[_numUniqueUuids];
    long[] lsbArr = new long[_numUniqueUuids];
    for (int i = 0; i < _numUniqueUuids; i++) {
      UUID uuid = new UUID(random.nextLong(), random.nextLong());
      uuidStrings[i] = uuid.toString();
      uuidBytesArr[i] = UuidUtils.toBytes(uuid);
      msbArr[i] = uuid.getMostSignificantBits();
      lsbArr[i] = uuid.getLeastSignificantBits();
    }
    _filterUuidString = uuidStrings[0];
    _filterBytesHex = BytesUtils.toHexString(uuidBytesArr[0]);
    _filterMsb = msbArr[0];
    _filterLsb = lsbArr[0];

    // Build rows with uniform distribution over the UUID pool
    List<GenericRow> rows = new ArrayList<>(_numRows);
    for (int i = 0; i < _numRows; i++) {
      int idx = i % _numUniqueUuids;
      GenericRow row = new GenericRow();
      row.putValue(STR_UUID_RAW, uuidStrings[idx]);
      row.putValue(STR_UUID_DICT, uuidStrings[idx]);
      row.putValue(BYTES_UUID_RAW, uuidBytesArr[idx]);
      row.putValue(BYTES_UUID_DICT, uuidBytesArr[idx]);
      row.putValue(UUID_RAW, uuidBytesArr[idx]);
      row.putValue(UUID_DICT, uuidBytesArr[idx]);
      row.putValue(LONG_MSB_RAW, msbArr[idx]);
      row.putValue(LONG_LSB_RAW, lsbArr[idx]);
      row.putValue(LONG_MSB_DICT, msbArr[idx]);
      row.putValue(LONG_LSB_DICT, lsbArr[idx]);
      rows.add(row);
    }

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    IndexLoadingConfig loadingConfig = new IndexLoadingConfig(TABLE_CONFIG, SCHEMA);
    _indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), loadingConfig);
    _indexSegments = List.of(_indexSegment);
  }

  @TearDown
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  // ---- GROUP BY --------------------------------------------------------

  @Benchmark
  public BrokerResponseNative groupByStrUuidRaw() {
    return getBrokerResponse(
        "SELECT " + STR_UUID_RAW + ", COUNT(*) FROM " + TABLE_NAME + " GROUP BY " + STR_UUID_RAW);
  }

  @Benchmark
  public BrokerResponseNative groupByStrUuidDict() {
    return getBrokerResponse(
        "SELECT " + STR_UUID_DICT + ", COUNT(*) FROM " + TABLE_NAME + " GROUP BY " + STR_UUID_DICT);
  }

  @Benchmark
  public BrokerResponseNative groupByBytesUuidRaw() {
    return getBrokerResponse(
        "SELECT " + BYTES_UUID_RAW + ", COUNT(*) FROM " + TABLE_NAME + " GROUP BY " + BYTES_UUID_RAW);
  }

  @Benchmark
  public BrokerResponseNative groupByBytesUuidDict() {
    return getBrokerResponse(
        "SELECT " + BYTES_UUID_DICT + ", COUNT(*) FROM " + TABLE_NAME + " GROUP BY " + BYTES_UUID_DICT);
  }

  @Benchmark
  public BrokerResponseNative groupByNativeUuidRaw() {
    return getBrokerResponse(
        "SELECT " + UUID_RAW + ", COUNT(*) FROM " + TABLE_NAME + " GROUP BY " + UUID_RAW);
  }

  @Benchmark
  public BrokerResponseNative groupByNativeUuidDict() {
    return getBrokerResponse(
        "SELECT " + UUID_DICT + ", COUNT(*) FROM " + TABLE_NAME + " GROUP BY " + UUID_DICT);
  }

  @Benchmark
  public BrokerResponseNative groupByTwoLongRaw() {
    return getBrokerResponse(
        "SELECT " + LONG_MSB_RAW + ", " + LONG_LSB_RAW + ", COUNT(*) FROM " + TABLE_NAME
            + " GROUP BY " + LONG_MSB_RAW + ", " + LONG_LSB_RAW);
  }

  @Benchmark
  public BrokerResponseNative groupByTwoLongDict() {
    return getBrokerResponse(
        "SELECT " + LONG_MSB_DICT + ", " + LONG_LSB_DICT + ", COUNT(*) FROM " + TABLE_NAME
            + " GROUP BY " + LONG_MSB_DICT + ", " + LONG_LSB_DICT);
  }

  // ---- COUNT(DISTINCT) ------------------------------------------------

  @Benchmark
  public BrokerResponseNative countDistinctStrUuidRaw() {
    return getBrokerResponse("SELECT COUNT(DISTINCT " + STR_UUID_RAW + ") FROM " + TABLE_NAME);
  }

  @Benchmark
  public BrokerResponseNative countDistinctStrUuidDict() {
    return getBrokerResponse("SELECT COUNT(DISTINCT " + STR_UUID_DICT + ") FROM " + TABLE_NAME);
  }

  @Benchmark
  public BrokerResponseNative countDistinctBytesUuidRaw() {
    return getBrokerResponse("SELECT COUNT(DISTINCT " + BYTES_UUID_RAW + ") FROM " + TABLE_NAME);
  }

  @Benchmark
  public BrokerResponseNative countDistinctBytesUuidDict() {
    return getBrokerResponse("SELECT COUNT(DISTINCT " + BYTES_UUID_DICT + ") FROM " + TABLE_NAME);
  }

  @Benchmark
  public BrokerResponseNative countDistinctNativeUuidRaw() {
    return getBrokerResponse("SELECT COUNT(DISTINCT " + UUID_RAW + ") FROM " + TABLE_NAME);
  }

  @Benchmark
  public BrokerResponseNative countDistinctNativeUuidDict() {
    return getBrokerResponse("SELECT COUNT(DISTINCT " + UUID_DICT + ") FROM " + TABLE_NAME);
  }

  // ---- Equality filter (full scan) ------------------------------------
  // For BYTES, the literal is a lowercase hex string (no dashes).
  // For STRING and UUID, the literal is the RFC 4122 dash-separated string.

  @Benchmark
  public BrokerResponseNative filterStrUuidRaw() {
    return getBrokerResponse(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE " + STR_UUID_RAW + " = '" + _filterUuidString + "'");
  }

  @Benchmark
  public BrokerResponseNative filterStrUuidDict() {
    return getBrokerResponse(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE " + STR_UUID_DICT + " = '" + _filterUuidString + "'");
  }

  @Benchmark
  public BrokerResponseNative filterBytesUuidRaw() {
    return getBrokerResponse(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE " + BYTES_UUID_RAW + " = '" + _filterBytesHex + "'");
  }

  @Benchmark
  public BrokerResponseNative filterBytesUuidDict() {
    return getBrokerResponse(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE " + BYTES_UUID_DICT + " = '" + _filterBytesHex + "'");
  }

  @Benchmark
  public BrokerResponseNative filterNativeUuidRaw() {
    return getBrokerResponse(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE " + UUID_RAW + " = '" + _filterUuidString + "'");
  }

  @Benchmark
  public BrokerResponseNative filterNativeUuidDict() {
    return getBrokerResponse(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE " + UUID_DICT + " = '" + _filterUuidString + "'");
  }

  @Benchmark
  public BrokerResponseNative filterTwoLongRaw() {
    return getBrokerResponse(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE " + LONG_MSB_RAW + " = " + _filterMsb
            + " AND " + LONG_LSB_RAW + " = " + _filterLsb);
  }

  @Benchmark
  public BrokerResponseNative filterTwoLongDict() {
    return getBrokerResponse(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE " + LONG_MSB_DICT + " = " + _filterMsb
            + " AND " + LONG_LSB_DICT + " = " + _filterLsb);
  }

  @Override
  protected String getFilter() {
    return null;
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkUuidQueryExecution.class.getSimpleName()).build()).run();
  }
}
