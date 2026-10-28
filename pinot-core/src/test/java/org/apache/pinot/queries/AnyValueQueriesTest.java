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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
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


/// Queries test for ANY_VALUE, whose data type argument the optimizer supplies from the schema when the
/// `inferAnyValueType` query option is set. All the rows with the same key hold the same values, so that any value is
/// the expected one.
public class AnyValueQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "AnyValueQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final int NUM_RECORDS = 10;

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension("keyCol", DataType.INT)
      .addSingleValueDimension("intCol", DataType.INT)
      .addSingleValueDimension("longCol", DataType.LONG)
      .addSingleValueDimension("floatCol", DataType.FLOAT)
      .addSingleValueDimension("doubleCol", DataType.DOUBLE)
      .addMetric("bigDecimalCol", DataType.BIG_DECIMAL)
      .addSingleValueDimension("boolCol", DataType.BOOLEAN)
      .addSingleValueDimension("timestampCol", DataType.TIMESTAMP)
      .addSingleValueDimension("stringCol", DataType.STRING)
      .addSingleValueDimension("jsonCol", DataType.JSON)
      .addSingleValueDimension("bytesCol", DataType.BYTES)
      .addSingleValueDimension("uuidCol", DataType.UUID)
      .build();
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
      .setNoDictionaryColumns(List.of("longCol", "doubleCol", "bytesCol")).build();
  private static final String INFER_ANY_VALUE_TYPE = "SET inferAnyValueType=true; ";
  private static final String ALL_COLUMNS = "ANY_VALUE(intCol), ANY_VALUE(longCol), ANY_VALUE(floatCol), "
      + "ANY_VALUE(doubleCol), ANY_VALUE(bigDecimalCol), ANY_VALUE(boolCol), ANY_VALUE(timestampCol), "
      + "ANY_VALUE(stringCol), ANY_VALUE(jsonCol), ANY_VALUE(bytesCol), ANY_VALUE(uuidCol)";

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

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      int key = i % 2;
      GenericRow record = new GenericRow();
      record.putValue("keyCol", key);
      record.putValue("intCol", 10 + key);
      record.putValue("longCol", 10_000_000_000L + key);
      record.putValue("floatCol", 0.5f + key);
      record.putValue("doubleCol", 0.25 + key);
      record.putValue("bigDecimalCol", new BigDecimal("12345678901234567890.5").add(BigDecimal.valueOf(key)));
      record.putValue("boolCol", key == 0);
      record.putValue("timestampCol", 1_600_000_000_000L + key);
      record.putValue("stringCol", "s" + key);
      record.putValue("jsonCol", "{\"k\":" + key + "}");
      record.putValue("bytesCol", new byte[]{(byte) key, 1});
      record.putValue("uuidCol", "123e4567-e89b-12d3-a456-42661417400" + key);
      records.add(record);
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = List.of(immutableSegment, immutableSegment);
  }

  @Test
  public void testAggregationOnly() {
    BrokerResponseNative brokerResponse = getBrokerResponseForOptimizedQuery(
        INFER_ANY_VALUE_TYPE + "SELECT " + ALL_COLUMNS + " FROM testTable WHERE keyCol = 0", SCHEMA);
    DataSchema expectedDataSchema = new DataSchema(new String[]{
        "anyvalue(intCol,'INT')", "anyvalue(longCol,'LONG')", "anyvalue(floatCol,'FLOAT')",
        "anyvalue(doubleCol,'DOUBLE')", "anyvalue(bigDecimalCol,'BIG_DECIMAL')", "anyvalue(boolCol,'BOOLEAN')",
        "anyvalue(timestampCol,'TIMESTAMP')", "anyvalue(stringCol,'STRING')", "anyvalue(jsonCol,'STRING')",
        "anyvalue(bytesCol,'BYTES')", "anyvalue(uuidCol,'UUID')"
    }, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE,
        ColumnDataType.BIG_DECIMAL, ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP, ColumnDataType.STRING,
        ColumnDataType.STRING, ColumnDataType.BYTES, ColumnDataType.UUID
    });
    Object[] expectedRow = new Object[]{
        10, 10_000_000_000L, 0.5f, 0.25, "12345678901234567890.5", true,
        new Timestamp(1_600_000_000_000L).toString(), "s0", "{\"k\":0}", "0001", "123e4567-e89b-12d3-a456-426614174000"
    };
    QueriesTestUtils.testInterSegmentsResult(brokerResponse,
        new ResultTable(expectedDataSchema, List.<Object[]>of(expectedRow)));
  }

  @Test
  public void testGroupBy() {
    ResultTable resultTable = getBrokerResponseForOptimizedQuery(
        INFER_ANY_VALUE_TYPE + "SELECT keyCol, ANY_VALUE(intCol) AS v1, ANY_VALUE(boolCol) AS v2, "
            + "ANY_VALUE(timestampCol) AS v3, "
            + "ANY_VALUE(uuidCol) AS v4 FROM testTable GROUP BY keyCol ORDER BY ANY_VALUE(longCol) DESC",
        SCHEMA).getResultTable();
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"keyCol", "v1", "v2", "v3", "v4"},
        new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP,
            ColumnDataType.UUID
        }));
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0), new Object[]{
        1, 11, false, new Timestamp(1_600_000_000_001L).toString(), "123e4567-e89b-12d3-a456-426614174001"
    });
    assertEquals(rows.get(1), new Object[]{
        0, 10, true, new Timestamp(1_600_000_000_000L).toString(), "123e4567-e89b-12d3-a456-426614174000"
    });
  }

  @Test
  public void testEmptyInput() {
    ResultTable resultTable = getBrokerResponseForOptimizedQuery(
        INFER_ANY_VALUE_TYPE + "SELECT " + ALL_COLUMNS + " FROM testTable WHERE keyCol < 0", SCHEMA).getResultTable();
    assertEquals(resultTable.getDataSchema().getColumnDataTypes(), new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE,
        ColumnDataType.BIG_DECIMAL, ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP, ColumnDataType.STRING,
        ColumnDataType.STRING, ColumnDataType.BYTES, ColumnDataType.UUID
    });
    assertEquals(resultTable.getRows().size(), 1);
    assertEquals(resultTable.getRows().get(0), new Object[11]);

    resultTable = getBrokerResponseForOptimizedQuery(
        INFER_ANY_VALUE_TYPE + "SELECT keyCol, ANY_VALUE(boolCol), ANY_VALUE(timestampCol) FROM testTable "
            + "WHERE keyCol < 0 GROUP BY keyCol",
        SCHEMA).getResultTable();
    assertEquals(resultTable.getDataSchema().getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP});
    assertTrue(resultTable.getRows().isEmpty());
  }

  @Test
  public void testExplicitDataType() {
    // SSE accepts convertible explicit types; MSE requires the inferred type.
    ResultTable resultTable = getBrokerResponseForOptimizedQuery(
        "SELECT ANY_VALUE(intCol, 'LONG'), ANY_VALUE(intCol, 'STRING'), ANY_VALUE(timestampCol, 'LONG') "
            + "FROM testTable WHERE keyCol = 0", SCHEMA).getResultTable();
    assertEquals(resultTable.getDataSchema().getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.STRING, ColumnDataType.LONG});
    assertEquals(resultTable.getRows().get(0), new Object[]{10L, "10", 1_600_000_000_000L});
  }

  @Test
  public void testWithoutInferredDataType() {
    ResultTable resultTable = getBrokerResponseForOptimizedQuery(
        "SELECT ANY_VALUE(intCol), ANY_VALUE(boolCol), ANY_VALUE(timestampCol) "
            + "FROM testTable WHERE keyCol = 0", SCHEMA).getResultTable();
    assertEquals(resultTable.getDataSchema(),
        new DataSchema(new String[]{"anyvalue(intCol)", "anyvalue(boolCol)", "anyvalue(timestampCol)"},
            new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.STRING}));
    assertEquals(resultTable.getRows().get(0), new Object[]{"10", "1", "1600000000000"});
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
