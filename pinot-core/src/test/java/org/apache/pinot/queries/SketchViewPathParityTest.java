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
package org.apache.pinot.queries;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.datasketches.tuple.aninteger.IntegerSketch;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Proves the zero-copy {@code ByteBuffer} view read path produces identical sketch-aggregation
 * results to the {@code byte[]} read path. The two paths are selected by the forward-index
 * compression codec: a PASS_THROUGH BYTES column reports
 * {@link org.apache.pinot.segment.spi.index.reader.ForwardIndexReader#isBufferViewStableAcrossReads()}
 * true (view path), while an LZ4 column reports false (byte[] fallback). The same serialized
 * theta / CPC / Integer-Tuple sketches are written into both a PASS_THROUGH segment and an LZ4
 * segment; the per-segment aggregation result must match exactly.
 */
public class SketchViewPathParityTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "SketchViewPathParityTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final int NUM_RECORDS = 1000;
  private static final int DISTINCTS_PER_ROW = 3;

  private static final String THETA_COLUMN = "thetaColumn";
  private static final String CPC_COLUMN = "cpcColumn";
  private static final String TUPLE_COLUMN = "tupleColumn";

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addMetric(THETA_COLUMN, DataType.BYTES)
      .addMetric(CPC_COLUMN, DataType.BYTES)
      .addMetric(TUPLE_COLUMN, DataType.BYTES)
      .build();

  private ImmutableSegment _passThroughSegment;
  private ImmutableSegment _lz4Segment;

  // The segment the BaseQueriesTest helpers run against; swapped per assertion.
  private IndexSegment _currentSegment;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _currentSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return List.of(_currentSegment);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    UpdateSketchBuilder thetaBuilder = new UpdateSketchBuilder();

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      int base = i * DISTINCTS_PER_ROW;

      UpdateSketch theta = thetaBuilder.build();
      CpcSketch cpc = new CpcSketch();
      IntegerSketch tuple = new IntegerSketch(12, IntegerSummary.Mode.Sum);
      for (int d = 0; d < DISTINCTS_PER_ROW; d++) {
        theta.update(base + d);
        cpc.update(base + d);
        tuple.update(Integer.toString(base + d), 1);
      }
      record.putValue(THETA_COLUMN, theta.compact().toByteArray());
      record.putValue(CPC_COLUMN, cpc.toByteArray());
      record.putValue(TUPLE_COLUMN, tuple.compact().toByteArray());
      records.add(record);
    }

    _passThroughSegment = buildSegment("passThrough", FieldConfig.CompressionCodec.PASS_THROUGH, records);
    _lz4Segment = buildSegment("lz4", FieldConfig.CompressionCodec.LZ4, records);
  }

  private ImmutableSegment buildSegment(String segmentName, FieldConfig.CompressionCodec codec,
      List<GenericRow> records)
      throws Exception {
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    for (String column : List.of(THETA_COLUMN, CPC_COLUMN, TUPLE_COLUMN)) {
      fieldConfigs.add(new FieldConfig(column, FieldConfig.EncodingType.RAW, List.of(), codec, null));
    }
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(List.of(THETA_COLUMN, CPC_COLUMN, TUPLE_COLUMN))
        .setFieldConfigList(fieldConfigs)
        .build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setTableName(RAW_TABLE_NAME);
    config.setSegmentName(segmentName);
    config.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(records));
    driver.build();
    return ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName), ReadMode.mmap);
  }

  private long distinctCount(IndexSegment segment, String query) {
    _currentSegment = segment;
    BrokerResponseNative response = getBrokerResponse(query);
    Object value = response.getResultTable().getRows().get(0)[0];
    return ((Number) value).longValue();
  }

  @Test
  public void thetaViewPathMatchesByteArrayPath() {
    String query = "SELECT DISTINCT_COUNT_THETA_SKETCH(" + THETA_COLUMN + ") FROM " + RAW_TABLE_NAME;
    long viaView = distinctCount(_passThroughSegment, query);
    long viaArray = distinctCount(_lz4Segment, query);
    assertEquals(viaView, viaArray);
    assertTrue(viaView > 0, "expected a non-trivial distinct count");
  }

  @Test
  public void cpcViewPathMatchesByteArrayPath() {
    String query = "SELECT DISTINCT_COUNT_CPC_SKETCH(" + CPC_COLUMN + ") FROM " + RAW_TABLE_NAME;
    long viaView = distinctCount(_passThroughSegment, query);
    long viaArray = distinctCount(_lz4Segment, query);
    assertEquals(viaView, viaArray);
    assertTrue(viaView > 0, "expected a non-trivial distinct count");
  }

  @Test
  public void tupleViewPathMatchesByteArrayPath() {
    String query = "SELECT DISTINCT_COUNT_TUPLE_SKETCH(" + TUPLE_COLUMN + ") FROM " + RAW_TABLE_NAME;
    long viaView = distinctCount(_passThroughSegment, query);
    long viaArray = distinctCount(_lz4Segment, query);
    assertEquals(viaView, viaArray);
    assertTrue(viaView > 0, "expected a non-trivial distinct count");
  }

  @Test
  public void passThroughReportsStableViewsAndLz4DoesNot() {
    assertTrue(_passThroughSegment.getDataSource(THETA_COLUMN).getForwardIndex().isBufferViewStableAcrossReads(),
        "PASS_THROUGH forward index should report stable views (view path)");
    assertTrue(!_lz4Segment.getDataSource(THETA_COLUMN).getForwardIndex().isBufferViewStableAcrossReads(),
        "LZ4 forward index should report unstable views (byte[] fallback)");
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    if (_passThroughSegment != null) {
      _passThroughSegment.destroy();
    }
    if (_lz4Segment != null) {
      _lz4Segment.destroy();
    }
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
