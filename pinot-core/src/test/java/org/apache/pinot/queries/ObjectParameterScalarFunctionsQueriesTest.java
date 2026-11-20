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
import java.math.BigDecimal;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.function.scalar.StringFunctions;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.function.scalar.SketchFunctions;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
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


/// Queries test for scalar functions with an `Object` parameter (e.g. the sketch functions and `jsonFormat`) on column
/// arguments. The function should get the external Java value of the column type.
public class ObjectParameterScalarFunctionsQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "ObjectParameterScalarFunctionsQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String INT_COLUMN = "intCol";
  private static final String LONG_COLUMN = "longCol";
  private static final String FLOAT_COLUMN = "floatCol";
  private static final String DOUBLE_COLUMN = "doubleCol";
  private static final String BIG_DECIMAL_COLUMN = "bigDecimalCol";
  private static final String BOOLEAN_COLUMN = "booleanCol";
  private static final String TIMESTAMP_COLUMN = "timestampCol";
  private static final String STRING_COLUMN = "stringCol";
  private static final String BYTES_COLUMN = "bytesCol";
  private static final String UUID_COLUMN = "uuidCol";
  private static final String INT_MV_COLUMN = "intMVCol";
  private static final String BOOLEAN_MV_COLUMN = "booleanMVCol";
  private static final String STRING_MV_COLUMN = "stringMVCol";
  private static final String BYTES_MV_COLUMN = "bytesMVCol";
  private static final String UUID_MV_COLUMN = "uuidMVCol";
  private static final String UUID_1 = "550e8400-e29b-41d4-a716-446655440000";
  private static final String UUID_2 = "123e4567-e89b-12d3-a456-426614174000";
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(INT_COLUMN, DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, DataType.LONG)
      .addSingleValueDimension(FLOAT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_COLUMN, DataType.DOUBLE)
      .addMetric(BIG_DECIMAL_COLUMN, DataType.BIG_DECIMAL)
      .addSingleValueDimension(BOOLEAN_COLUMN, DataType.BOOLEAN)
      .addSingleValueDimension(TIMESTAMP_COLUMN, DataType.TIMESTAMP)
      .addSingleValueDimension(STRING_COLUMN, DataType.STRING)
      .addSingleValueDimension(BYTES_COLUMN, DataType.BYTES)
      .addSingleValueDimension(UUID_COLUMN, DataType.UUID)
      .addMultiValueDimension(INT_MV_COLUMN, DataType.INT)
      .addMultiValueDimension(BOOLEAN_MV_COLUMN, DataType.BOOLEAN)
      .addMultiValueDimension(STRING_MV_COLUMN, DataType.STRING)
      .addMultiValueDimension(BYTES_MV_COLUMN, DataType.BYTES)
      .addMultiValueDimension(UUID_MV_COLUMN, DataType.UUID)
      .build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    GenericRow record = new GenericRow();
    record.putValue(INT_COLUMN, 1);
    record.putValue(LONG_COLUMN, 2L);
    record.putValue(FLOAT_COLUMN, 1.5f);
    record.putValue(DOUBLE_COLUMN, 2.5);
    record.putValue(BIG_DECIMAL_COLUMN, new BigDecimal("3.25"));
    record.putValue(BOOLEAN_COLUMN, true);
    record.putValue(TIMESTAMP_COLUMN, 1700000000000L);
    record.putValue(STRING_COLUMN, "abc");
    record.putValue(BYTES_COLUMN, new byte[]{1, 2, 3});
    record.putValue(UUID_COLUMN, UUID_1);
    record.putValue(INT_MV_COLUMN, new Object[]{1, 2});
    record.putValue(BOOLEAN_MV_COLUMN, new Object[]{true, false});
    record.putValue(STRING_MV_COLUMN, new Object[]{"a", "b"});
    record.putValue(BYTES_MV_COLUMN, new Object[]{new byte[]{1, 2}, new byte[]{3}});
    record.putValue(UUID_MV_COLUMN, new Object[]{UUID_1, UUID_2});

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(List.of(record)));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = List.of(immutableSegment, immutableSegment);
  }

  @Test
  public void testSketchFunctions() {
    String query = "SELECT toBase64(toThetaSketch(intCol)), toBase64(toHLL(longCol)), "
        + "toBase64(toCpcSketch(doubleCol)), toBase64(toULL(stringCol)), "
        + "toBase64(toIntegerSumTupleSketch(bigDecimalCol, intCol)), toBase64(toThetaSketch(bytesCol)), "
        + "getThetaSketchEstimate(toThetaSketch(floatCol)) FROM testTable LIMIT 1";
    Object[] expectedRow = new Object[]{
        StringFunctions.toBase64(SketchFunctions.toThetaSketch(1)),
        StringFunctions.toBase64(SketchFunctions.toHLL(2L)),
        StringFunctions.toBase64(SketchFunctions.toCpcSketch(2.5)),
        StringFunctions.toBase64(SketchFunctions.toULL("abc")),
        StringFunctions.toBase64(SketchFunctions.toIntegerSumTupleSketch(new BigDecimal("3.25"), 1)),
        StringFunctions.toBase64(SketchFunctions.toThetaSketch(new byte[]{1, 2, 3})),
        1L
    };
    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), expectedRow);
  }

  @Test
  public void testJsonFormat() {
    String query = "SELECT jsonFormat(intCol), jsonFormat(longCol), jsonFormat(floatCol), jsonFormat(doubleCol), "
        + "jsonFormat(bigDecimalCol), jsonFormat(booleanCol), jsonFormat(timestampCol), jsonFormat(stringCol), "
        + "jsonFormat(bytesCol), jsonFormat(uuidCol), jsonFormat(intMVCol), jsonFormat(booleanMVCol), "
        + "jsonFormat(stringMVCol), jsonFormat(bytesMVCol), jsonFormat(uuidMVCol) FROM testTable LIMIT 1";
    Object[] expectedRow = new Object[]{
        "1", "2", "1.5", "2.5", "3.25", "true", "1700000000000", "\"abc\"", "\"AQID\"", "\"" + UUID_1 + "\"",
        "[1,2]", "[true,false]", "[\"a\",\"b\"]", "[\"AQI=\",\"Aw==\"]", "[\"" + UUID_1 + "\",\"" + UUID_2 + "\"]"
    };
    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), expectedRow);
  }

  private List<Object[]> getRows(String query) {
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    assertTrue(brokerResponse.getExceptions().isEmpty(), brokerResponse.getExceptions().toString());
    return brokerResponse.getResultTable().getRows();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
