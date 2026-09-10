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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Exercises bound SSE aggregates through segment execution, wire serialization and broker post-aggregation.
public class PolymorphicAggregationQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "PolymorphicAggregationQueriesTest");
  private static final long BASE_TIMESTAMP = 1_700_000_000_000L;
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName("testTable")
      .setEnableColumnBasedNullHandling(true)
      .addSingleValueDimension("grp", DataType.STRING)
      .addDimensionField("value", DataType.STRING, field -> field.setNullable(true))
      .addDimensionField("stamp", DataType.TIMESTAMP, field -> field.setNullable(true))
      .addDimensionField("enabled", DataType.BOOLEAN, field -> field.setNullable(true))
      .addSingleValueDimension("eventTime", DataType.LONG)
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
    buildSegment(0, false, List.of(row("a", "alpha", 100, false, 10), row("a", "beta", 200, true, 20),
        row("b", "gamma", 300, true, 30)));
    buildSegment(1, true, List.of(row("a", "beta", 200, true, 40), row("b", "delta", 400, false, 50),
        row("b", "gamma", 300, true, 60)));
    buildSegment(2, true, List.of(row("a", "alpha", 100, false, 70), row("a", "beta", 200, true, 80),
        row("b", "delta", 400, false, 90)));
    buildSegment(3, false, List.of(row("n", null, 0, null, 100), row("n", null, 0, null, 110),
        row("b", "gamma", 300, true, 120)));
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
  public void testModeLogicalResultTypes() {
    ResultTable result = query("SELECT MODE(value), MODE(stamp), MODE(enabled), MODE(stamp, 'MAX') FROM testTable");
    assertTypes(result, ColumnDataType.STRING, ColumnDataType.TIMESTAMP, ColumnDataType.BOOLEAN,
        ColumnDataType.TIMESTAMP);
    assertEquals(result.getRows().get(0), new Object[]{"beta", timestamp(200), true, timestamp(300)});
  }

  @Test
  public void testFirstLastInferredTypes() {
    ResultTable result = query("SELECT FIRST_WITH_TIME(value, eventTime), LAST_WITH_TIME(value, eventTime), "
        + "FIRST_WITH_TIME(stamp, eventTime), LAST_WITH_TIME(stamp, eventTime), "
        + "FIRST_WITH_TIME(enabled, eventTime), LAST_WITH_TIME(enabled, eventTime) FROM testTable");
    assertTypes(result, ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.TIMESTAMP,
        ColumnDataType.TIMESTAMP, ColumnDataType.BOOLEAN, ColumnDataType.BOOLEAN);
    assertEquals(result.getRows().get(0), new Object[]{"alpha", "gamma", timestamp(100), timestamp(300), false, true});
    assertEquals(result.getDataSchema().getColumnName(0), "firstwithtime(value,eventTime)");
  }

  @Test
  public void testGroupedHavingOrderByAndPostAggregation() {
    ResultTable result = query("SELECT grp, UPPER(MODE(value)), MODE(stamp), MODE(enabled) FROM testTable "
        + "GROUP BY grp HAVING MODE(value) >= 'beta' ORDER BY MODE(value) DESC");
    assertTypes(result, ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.TIMESTAMP, ColumnDataType.BOOLEAN);
    assertEquals(result.getRows().size(), 2);
    assertEquals(result.getRows().get(0), new Object[]{"b", "GAMMA", timestamp(300), true});
    assertEquals(result.getRows().get(1), new Object[]{"a", "BETA", timestamp(200), true});

    result = query("SELECT grp, MODE(stamp) FROM testTable WHERE grp != 'n' "
        + "GROUP BY grp ORDER BY MODE(stamp) DESC");
    assertTypes(result, ColumnDataType.STRING, ColumnDataType.TIMESTAMP);
    assertEquals(result.getRows().get(0), new Object[]{"b", timestamp(300)});
    assertEquals(result.getRows().get(1), new Object[]{"a", timestamp(200)});

    result = query("SELECT UPPER(MODE(value)), fromTimestamp(MODE(stamp)), MODE(CAST(value AS STRING)) FROM testTable");
    assertTypes(result, ColumnDataType.STRING, ColumnDataType.LONG, ColumnDataType.STRING);
    assertEquals(result.getRows().get(0), new Object[]{"BETA", BASE_TIMESTAMP + 200, "beta"});
  }

  @Test
  public void testFilteredAggregatesAndEmptySegments() {
    ResultTable result = query("SELECT MODE(value) FILTER (WHERE grp = 'a'), "
        + "MODE(stamp) FILTER (WHERE grp = 'a'), FIRST_WITH_TIME(value, eventTime) FILTER (WHERE grp = 'b') "
        + "FROM testTable");
    assertTypes(result, ColumnDataType.STRING, ColumnDataType.TIMESTAMP, ColumnDataType.STRING);
    assertEquals(result.getRows().get(0), new Object[]{"beta", timestamp(200), "gamma"});

    result = query("SELECT MODE(value), MODE(stamp), MODE(enabled) FROM testTable WHERE grp = 'a'");
    assertTypes(result, ColumnDataType.STRING, ColumnDataType.TIMESTAMP, ColumnDataType.BOOLEAN);
    assertEquals(result.getRows().get(0), new Object[]{"beta", timestamp(200), true});
  }

  @Test
  public void testNoMatchesAndAllNullPreserveTypes() {
    for (String predicate : List.of("grp = 'missing'", "grp = 'n'")) {
      ResultTable result = query("SELECT MODE(value), MODE(stamp), MODE(enabled), "
          + "FIRST_WITH_TIME(stamp, eventTime), LAST_WITH_TIME(value, eventTime) FROM testTable WHERE " + predicate);
      assertTypes(result, ColumnDataType.STRING, ColumnDataType.TIMESTAMP, ColumnDataType.BOOLEAN,
          ColumnDataType.TIMESTAMP, ColumnDataType.STRING);
      assertEquals(result.getRows().size(), 1);
      assertEquals(result.getRows().get(0), new Object[]{null, null, null, null, null});
    }
    ResultTable result = query("SELECT MODE(value), MODE(stamp), MODE(enabled) FROM testTable LIMIT 0");
    assertTypes(result, ColumnDataType.STRING, ColumnDataType.TIMESTAMP, ColumnDataType.BOOLEAN);
    assertTrue(result.getRows().isEmpty());
  }

  @DataProvider
  public Object[][] nullHandlingOptions() {
    return new Object[][]{
        {"", false},
        {"SET enableNullHandling = false; ", false},
        {"SET enableNullHandling = true; ", true}
    };
  }

  @Test(dataProvider = "nullHandlingOptions")
  public void testInferredAggregatesWithNullHandlingOptions(String options, boolean nullHandlingEnabled) {
    String aggregates = "MODE(value), MODE(stamp), MODE(enabled), FIRST_WITH_TIME(value, eventTime), "
        + "LAST_WITH_TIME(stamp, eventTime), LAST_WITH_TIME(enabled, eventTime)";
    ColumnDataType[] types = {ColumnDataType.STRING, ColumnDataType.TIMESTAMP, ColumnDataType.BOOLEAN,
        ColumnDataType.STRING, ColumnDataType.TIMESTAMP, ColumnDataType.BOOLEAN};

    // The non-null rows span both raw and dictionary segments on both server instances.
    ResultTable result = query("SELECT " + aggregates + " FROM testTable WHERE grp != 'n'", options);
    assertTypes(result, types);
    assertEquals(result.getRows().size(), 1);
    assertEquals(result.getRows().get(0),
        new Object[]{"beta", timestamp(200), true, "alpha", timestamp(300), true});

    // With null handling disabled, actual rows expose their stored schema defaults, unlike an empty input.
    Object[] allNullRow = nullHandlingEnabled
        ? new Object[6]
        : new Object[]{"null", new Timestamp(0).toString(), false, "null", new Timestamp(0).toString(), false};
    result = query("SELECT " + aggregates + " FROM testTable WHERE grp = 'n'", options);
    assertTypes(result, types);
    assertEquals(result.getRows().size(), 1);
    assertEquals(result.getRows().get(0), allNullRow);

    result = query("SELECT grp, " + aggregates + " FROM testTable GROUP BY grp ORDER BY grp", options);
    assertTypes(result, ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.TIMESTAMP,
        ColumnDataType.BOOLEAN, ColumnDataType.STRING, ColumnDataType.TIMESTAMP, ColumnDataType.BOOLEAN);
    assertEquals(result.getRows().size(), 3);
    assertEquals(result.getRows().get(0),
        new Object[]{"a", "beta", timestamp(200), true, "alpha", timestamp(200), true});
    assertEquals(result.getRows().get(1),
        new Object[]{"b", "gamma", timestamp(300), true, "gamma", timestamp(300), true});
    Object[] nullGroup = new Object[7];
    nullGroup[0] = "n";
    System.arraycopy(allNullRow, 0, nullGroup, 1, allNullRow.length);
    assertEquals(result.getRows().get(2), nullGroup);

    result = query("SELECT " + aggregates + " FROM testTable WHERE grp = 'missing'", options);
    assertTypes(result, types);
    assertEquals(result.getRows().size(), 1);
    assertEquals(result.getRows().get(0), new Object[6]);
    result = query("SELECT " + aggregates + " FROM testTable WHERE grp = 'missing' GROUP BY grp", options);
    assertTypes(result, types);
    assertTrue(result.getRows().isEmpty());
  }

  private ResultTable query(String sql) {
    return query(sql, "SET enableNullHandling = true; ");
  }

  private ResultTable query(String sql, String options) {
    BrokerResponseNative response = getBrokerResponseForOptimizedQuery(options + sql, SCHEMA);
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    return response.getResultTable();
  }

  private static void assertTypes(ResultTable result, ColumnDataType... types) {
    assertEquals(result.getDataSchema().getColumnDataTypes(), types);
  }

  private static String timestamp(long offset) {
    return new Timestamp(BASE_TIMESTAMP + offset).toString();
  }

  private static GenericRow row(String group, String value, long stamp, Boolean enabled, long time) {
    GenericRow row = new GenericRow();
    row.putValue("grp", group);
    row.putValue("value", value);
    row.putValue("stamp", value != null ? BASE_TIMESTAMP + stamp : null);
    row.putValue("enabled", enabled);
    row.putValue("eventTime", time);
    return row;
  }

  private void buildSegment(int id, boolean raw, List<GenericRow> rows)
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setNoDictionaryColumns(raw ? List.of("value", "stamp", "enabled") : List.of()).build();
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
