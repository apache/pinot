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
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.utils.rewriter.ParentAggregationResultRewriter;
import org.apache.pinot.core.query.utils.rewriter.ResultRewriterFactory;
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
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.rewriter.ExprMinMaxRewriter;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriterFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Verifies ExprMin/Max logical result schemas through raw/dictionary segments and broker parent-result rewriting.
public class PolymorphicExprMinMaxQueriesTest extends BaseQueriesTest {
  private static final long BASE_TIMESTAMP = 1_700_000_000_000L;
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName("testTable")
      .setEnableColumnBasedNullHandling(true)
      .addSingleValueDimension("grp", DataType.STRING)
      .addDimensionField("id", DataType.LONG, field -> field.setNullable(true))
      .addSingleValueDimension("stamp", DataType.TIMESTAMP)
      .addDimensionField("nullableStamp", DataType.TIMESTAMP, field -> field.setNullable(true))
      .addSingleValueDimension("enabled", DataType.BOOLEAN)
      .addSingleValueDimension("value", DataType.STRING)
      .addSingleValueDimension("jsonValue", DataType.JSON)
      .addMultiValueDimension("mvValues", DataType.LONG)
      .build();
  private final List<IndexSegment> _segments = new ArrayList<>();
  private File _indexDir;
  private String _previousQueryRewriters;
  private String _previousResultRewriters;

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
    _previousQueryRewriters = CalciteSqlParser.QUERY_REWRITERS.stream().map(r -> r.getClass().getName())
        .collect(Collectors.joining(","));
    _previousResultRewriters = ResultRewriterFactory.getResultRewriter().stream().map(r -> r.getClass().getName())
        .collect(Collectors.joining(","));
    QueryRewriterFactory.init(String.join(",", QueryRewriterFactory.DEFAULT_QUERY_REWRITERS_CLASS_NAMES)
        + "," + ExprMinMaxRewriter.class.getName());
    ResultRewriterFactory.init(ParentAggregationResultRewriter.class.getName());
    _indexDir = Files.createTempDirectory("polymorphic-expr-min-max").toFile();
    buildSegment(0, false, List.of(row("a", 1L), row("a", 4L)));
    buildSegment(1, true, List.of(row("a", 2L), row("b", 5L)));
    buildSegment(2, true, List.of(row("a", 3L), row("b", 6L)));
    buildSegment(3, false, List.of(row("n", null)));
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    QueryRewriterFactory.init(_previousQueryRewriters);
    ResultRewriterFactory.init(_previousResultRewriters == null || _previousResultRewriters.isEmpty()
        ? null
        : _previousResultRewriters);
    for (IndexSegment segment : _segments) {
      segment.destroy();
    }
    if (_indexDir != null) {
      FileUtils.deleteDirectory(_indexDir);
    }
  }

  @Test
  public void testLogicalProjectionTypes() {
    ResultTable result = query("SELECT EXPR_MIN(enabled, id), EXPR_MAX(stamp, id), "
        + "EXPR_MAX(mvValues, id), EXPR_MIN(value, id) FROM testTable");
    assertTypes(result, ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP, ColumnDataType.LONG_ARRAY,
        ColumnDataType.STRING);
    assertEquals(result.getRows().size(), 1);
    assertEquals(result.getRows().get(0), new Object[]{false, timestamp(6), new long[]{6L, 7L}, "v1"});
  }

  @Test
  public void testLogicalMeasuringTypesAndGroupedResults() {
    ResultTable result = query("SELECT grp, EXPR_MIN(enabled, stamp), EXPR_MAX(stamp, stamp) "
        + "FROM testTable WHERE grp != 'n' GROUP BY grp ORDER BY grp");
    assertTypes(result, ColumnDataType.STRING, ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP);
    assertEquals(result.getRows().size(), 2);
    assertEquals(result.getRows().get(0), new Object[]{"a", false, timestamp(4)});
    assertEquals(result.getRows().get(1), new Object[]{"b", false, timestamp(6)});
  }

  @Test
  public void testEmptyAndUndefinedKeysPreserveProjectionTypes() {
    for (String predicate : List.of("grp = 'missing'", "grp = 'n'")) {
      ResultTable result = query("SELECT EXPR_MIN(enabled, id), EXPR_MAX(stamp, id), EXPR_MIN(mvValues, id) "
          + "FROM testTable WHERE " + predicate);
      assertTypes(result, ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP, ColumnDataType.LONG_ARRAY);
      assertEquals(result.getRows().size(), 1);
      assertEquals(result.getRows().get(0), new Object[]{null, null, null});
    }
    ResultTable result = query("SELECT EXPR_MIN(enabled, id), EXPR_MAX(stamp, id), EXPR_MIN(mvValues, id) "
        + "FROM testTable LIMIT 0");
    assertTypes(result, ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP, ColumnDataType.LONG_ARRAY);
    assertTrue(result.getRows().isEmpty());
  }

  @Test
  public void testJsonProjectionUsesBoundSqlTypeForEveryResultShape() {
    ResultTable result = query("SELECT EXPR_MIN(jsonValue, id), EXPR_MAX(jsonValue, id) FROM testTable");
    assertTypes(result, ColumnDataType.STRING, ColumnDataType.STRING);
    assertEquals(result.getRows().size(), 1);
    assertEquals(result.getRows().get(0), new Object[]{"{\"id\":1}", "{\"id\":6}"});
    for (String predicate : List.of("grp = 'missing'", "grp = 'n'")) {
      result = query("SELECT EXPR_MIN(jsonValue, id) FROM testTable WHERE " + predicate);
      assertTypes(result, ColumnDataType.STRING);
      assertEquals(result.getRows().size(), 1);
      assertEquals(result.getRows().get(0), new Object[]{null});
    }
    result = query("SELECT grp, EXPR_MAX(jsonValue, id) FROM testTable WHERE grp != 'n' GROUP BY grp ORDER BY grp");
    assertTypes(result, ColumnDataType.STRING, ColumnDataType.STRING);
    assertEquals(result.getRows().get(0), new Object[]{"a", "{\"id\":4}"});
    assertEquals(result.getRows().get(1), new Object[]{"b", "{\"id\":6}"});
    result = query("SELECT grp, EXPR_MIN(jsonValue, id) FROM testTable WHERE grp = 'missing' GROUP BY grp");
    assertTypes(result, ColumnDataType.STRING, ColumnDataType.STRING);
    assertTrue(result.getRows().isEmpty());
    result = query("SELECT EXPR_MIN(jsonValue, id) FROM testTable LIMIT 0");
    assertTypes(result, ColumnDataType.STRING);
    assertTrue(result.getRows().isEmpty());
  }

  @Test
  public void testNullProjectionSurvivesSegmentAndBrokerSerialization() {
    ResultTable result = query("SELECT EXPR_MIN(nullableStamp, id), EXPR_MAX(nullableStamp, id) FROM testTable");
    assertTypes(result, ColumnDataType.TIMESTAMP, ColumnDataType.TIMESTAMP);
    assertEquals(result.getRows().size(), 1);
    assertEquals(result.getRows().get(0), new Object[]{null, timestamp(6)});
    result = query("SELECT grp, EXPR_MIN(nullableStamp, id), EXPR_MAX(nullableStamp, id) "
        + "FROM testTable WHERE grp != 'n' GROUP BY grp ORDER BY grp");
    assertTypes(result, ColumnDataType.STRING, ColumnDataType.TIMESTAMP, ColumnDataType.TIMESTAMP);
    assertEquals(result.getRows().size(), 2);
    assertEquals(result.getRows().get(0), new Object[]{"a", null, timestamp(4)});
    assertEquals(result.getRows().get(1), new Object[]{"b", timestamp(5), timestamp(6)});
  }

  private ResultTable query(String sql) {
    BrokerResponseNative response = getBrokerResponseForOptimizedQuery("SET enableNullHandling = true; " + sql, SCHEMA);
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    return response.getResultTable();
  }

  private static void assertTypes(ResultTable result, ColumnDataType... types) {
    assertEquals(result.getDataSchema().getColumnDataTypes(), types);
  }

  private static String timestamp(long id) {
    return new Timestamp(BASE_TIMESTAMP + id * 100).toString();
  }

  private static GenericRow row(String group, Long id) {
    long value = id != null ? id : 0L;
    GenericRow row = new GenericRow();
    row.putValue("grp", group);
    row.putValue("id", id);
    row.putValue("stamp", BASE_TIMESTAMP + value * 100);
    row.putValue("nullableStamp", value == 1 ? null : BASE_TIMESTAMP + value * 100);
    row.putValue("enabled", value % 2 == 0);
    row.putValue("value", "v" + value);
    row.putValue("jsonValue", "{\"id\":" + value + "}");
    row.putValue("mvValues", new Long[]{value, value + 1});
    return row;
  }

  private void buildSegment(int id, boolean raw, List<GenericRow> rows)
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setNoDictionaryColumns(raw
            ? List.of("id", "stamp", "nullableStamp", "enabled", "value", "mvValues", "jsonValue")
            : List.of())
        .build();
    String name = "segment" + id;
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setSegmentName(name);
    config.setOutDir(_indexDir.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    _segments.add(ImmutableSegmentLoader.load(new File(_indexDir, name), ReadMode.mmap));
  }
}
