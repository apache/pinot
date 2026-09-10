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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
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


/// Exercises schema-bound ANY_VALUE through raw and dictionary segments, server merges and broker reduction.
public class AnyValueInferredQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "AnyValueInferredQueriesTest");
  private static final long TIMESTAMP = 1_700_000_000_123L;
  private static final long LARGE_LONG = 9_007_199_254_740_993L;
  private static final String UUID = "00112233-4455-6677-8899-aabbccddeeff";
  private static final String SELECT = "ANY_VALUE(label), ANY_VALUE(stamp), ANY_VALUE(flag), ANY_VALUE(number), "
      + "ANY_VALUE(fraction), ANY_VALUE(payload), ANY_VALUE(binaryValue), ANY_VALUE(token)";
  private static final ColumnDataType[] RESULT_TYPES = {
      ColumnDataType.STRING, ColumnDataType.TIMESTAMP, ColumnDataType.BOOLEAN, ColumnDataType.LONG,
      ColumnDataType.DOUBLE, ColumnDataType.STRING, ColumnDataType.BYTES, ColumnDataType.UUID
  };
  private static final Object[] RESULT = {
      "alpha", new Timestamp(TIMESTAMP).toString(), true, LARGE_LONG, 1.25d, "{\"key\":42}", "00ff", UUID
  };
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName("testTable")
      .setEnableColumnBasedNullHandling(true)
      .addSingleValueDimension("grp", DataType.STRING)
      .addDimensionField("label", DataType.STRING, field -> field.setNullable(true))
      .addDimensionField("stamp", DataType.TIMESTAMP, field -> field.setNullable(true))
      .addDimensionField("flag", DataType.BOOLEAN, field -> field.setNullable(true))
      .addDimensionField("number", DataType.LONG, field -> field.setNullable(true))
      .addDimensionField("fraction", DataType.DOUBLE, field -> field.setNullable(true))
      .addDimensionField("payload", DataType.JSON, field -> field.setNullable(true))
      .addDimensionField("binaryValue", DataType.BYTES, field -> field.setNullable(true))
      .addDimensionField("token", DataType.UUID, field -> field.setNullable(true))
      .build();
  private final List<IndexSegment> _segments = new ArrayList<>();

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _segments.get(0);
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _segments;
  }

  @Override
  protected List<List<IndexSegment>> getDistinctInstances() {
    return List.of(_segments.subList(0, 2), _segments.subList(2, 4));
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    buildSegment(0, false, List.of(row("a", true), row("a", false), row("n", true)));
    buildSegment(1, true, List.of(row("b", false), row("n", true)));
    buildSegment(2, true, List.of(row("a", false), row("b", false), row("n", true)));
    buildSegment(3, false, List.of(row("b", true), row("b", false), row("n", true)));
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    for (IndexSegment segment : _segments) {
      segment.destroy();
    }
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Test
  public void testDistributedLogicalTypes() {
    ResultTable result = query("SELECT " + SELECT + " FROM testTable");
    assertTypes(result, RESULT_TYPES);
    assertEquals(result.getRows().size(), 1);
    assertEquals(result.getRows().get(0), RESULT);
    assertEquals(result.getDataSchema().getColumnName(0), "anyvalue(label)");

    result = query("SELECT " + SELECT + " FROM testTable WHERE grp = 'a'");
    assertTypes(result, RESULT_TYPES);
    assertEquals(result.getRows().get(0), RESULT);
  }

  @Test
  public void testGroupedAndPostAggregation() {
    ResultTable result = query("SELECT grp, ANY_VALUE(label), ANY_VALUE(stamp), ANY_VALUE(flag), ANY_VALUE(token) "
        + "FROM testTable GROUP BY grp ORDER BY grp");
    assertTypes(result, ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.TIMESTAMP,
        ColumnDataType.BOOLEAN, ColumnDataType.UUID);
    assertEquals(result.getRows().size(), 3);
    assertEquals(result.getRows().get(0), new Object[]{"a", RESULT[0], RESULT[1], true, UUID});
    assertEquals(result.getRows().get(1), new Object[]{"b", RESULT[0], RESULT[1], true, UUID});
    assertEquals(result.getRows().get(2), new Object[]{"n", null, null, null, null});

    result = query("SELECT grp, UPPER(ANY_VALUE(label)), fromTimestamp(ANY_VALUE(stamp)), ANY_VALUE(number) + 1 "
        + "FROM testTable GROUP BY grp HAVING ANY_VALUE(label) = 'alpha' ORDER BY ANY_VALUE(stamp), grp");
    assertTypes(result, ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.LONG, ColumnDataType.LONG);
    assertEquals(result.getRows().size(), 2);
    assertEquals(result.getRows().get(0), new Object[]{"a", "ALPHA", TIMESTAMP, LARGE_LONG + 1});
    assertEquals(result.getRows().get(1), new Object[]{"b", "ALPHA", TIMESTAMP, LARGE_LONG + 1});
  }

  @Test
  public void testEmptyAndNullTypes() {
    for (String predicate : List.of("grp = 'missing'", "grp = 'n'")) {
      ResultTable result = query("SELECT " + SELECT + " FROM testTable WHERE " + predicate);
      assertTypes(result, RESULT_TYPES);
      assertEquals(result.getRows().size(), 1);
      assertEquals(result.getRows().get(0), new Object[RESULT_TYPES.length]);
    }
    ResultTable result = query("SELECT " + SELECT + " FROM testTable LIMIT 0");
    assertTypes(result, RESULT_TYPES);
    assertTrue(result.getRows().isEmpty());

    result = query("SELECT ANY_VALUE(stamp), ANY_VALUE(flag), ANY_VALUE(number), ANY_VALUE(token) "
        + "FROM testTable WHERE grp = 'missing' GROUP BY grp");
    assertTypes(result, ColumnDataType.TIMESTAMP, ColumnDataType.BOOLEAN, ColumnDataType.LONG, ColumnDataType.UUID);
    assertTrue(result.getRows().isEmpty());
  }

  @Test
  public void testFilteredAggregate() {
    ResultTable result = query("SELECT ANY_VALUE(stamp) FILTER (WHERE grp = 'a'), "
        + "ANY_VALUE(flag) FILTER (WHERE grp = 'b'), ANY_VALUE(number) FILTER (WHERE grp = 'missing') FROM testTable");
    assertTypes(result, ColumnDataType.TIMESTAMP, ColumnDataType.BOOLEAN, ColumnDataType.LONG);
    assertEquals(result.getRows().get(0), new Object[]{RESULT[1], true, null});
  }

  private ResultTable query(String sql) {
    BrokerResponseNative response = getBrokerResponseForOptimizedQuery("SET enableNullHandling = true; " + sql, SCHEMA);
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    return response.getResultTable();
  }

  private static void assertTypes(ResultTable result, ColumnDataType... types) {
    assertEquals(result.getDataSchema().getColumnDataTypes(), types);
  }

  private static GenericRow row(String group, boolean nulls) {
    GenericRow row = new GenericRow();
    row.putValue("grp", group);
    row.putValue("label", nulls ? null : "alpha");
    row.putValue("stamp", nulls ? null : TIMESTAMP);
    row.putValue("flag", nulls ? null : true);
    row.putValue("number", nulls ? null : LARGE_LONG);
    row.putValue("fraction", nulls ? null : 1.25d);
    row.putValue("payload", nulls ? null : "{\"key\":42}");
    row.putValue("binaryValue", nulls ? null : new byte[]{0, (byte) 0xff});
    row.putValue("token", nulls ? null : UUID);
    return row;
  }

  private void buildSegment(int id, boolean raw, List<GenericRow> rows)
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setNoDictionaryColumns(raw
            ? List.of("label", "stamp", "flag", "number", "fraction", "payload", "binaryValue", "token")
            : List.of())
        .build();
    String name = "segment" + id;
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setSegmentName(name);
    config.setOutDir(INDEX_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    _segments.add(ImmutableSegmentLoader.load(new File(INDEX_DIR, name), ReadMode.mmap));
  }
}
