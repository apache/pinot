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
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
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


/// Exercises inferred ARRAY_AGG across raw and dictionary segments, server serialization, and broker reduction.
public class ArrayAggInferredQueriesTest extends BaseQueriesTest {
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName("testTable")
      .setEnableColumnBasedNullHandling(true)
      .addSingleValueDimension("grp", DataType.STRING)
      .addDimensionField("value", DataType.INT, field -> field.setNullable(true))
      .addDimensionField("stamp", DataType.TIMESTAMP, field -> field.setNullable(true))
      .addDimensionField("flag", DataType.BOOLEAN, field -> field.setNullable(true))
      .addMultiValueDimension("labels", DataType.STRING)
      .build();
  private final List<IndexSegment> _segments = new ArrayList<>();
  private File _indexDir;

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
    return List.of(List.of(_segments.get(0)), List.of(_segments.get(1)));
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    _indexDir = Files.createTempDirectory("array-agg-inferred-").toFile();
    buildSegment(0, false, List.of(row("a", 2, 200L, false, "a", "b"), row("a", 1, 100L, true, "a")));
    buildSegment(1, true, List.of(row("a", 1, 100L, true, "c"), row("n", null, null, null)));
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    for (IndexSegment segment : _segments) {
      segment.destroy();
    }
    if (_indexDir != null) {
      FileUtils.deleteDirectory(_indexDir);
    }
  }

  @Test
  public void testInferredLogicalArraysAndDistinct() {
    ResultTable result = query("SELECT ARRAY_AGG(value), ARRAY_AGG(stamp, true), ARRAY_AGG(flag, true), "
        + "ARRAY_AGG(labels, true) FROM testTable WHERE grp = 'a'");
    assertEquals(result.getDataSchema().getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.INT_ARRAY,
        ColumnDataType.TIMESTAMP_ARRAY, ColumnDataType.BOOLEAN_ARRAY, ColumnDataType.STRING_ARRAY});
    Object[] row = result.getRows().get(0);
    int[] values = (int[]) row[0];
    Arrays.sort(values);
    assertEquals(values, new int[]{1, 1, 2});
    String[] timestamps = (String[]) row[1];
    Arrays.sort(timestamps);
    assertEquals(timestamps, new String[]{new Timestamp(100).toString(), new Timestamp(200).toString()});
    boolean[] flags = (boolean[]) row[2];
    assertEquals(flags.length, 2);
    assertTrue(flags[0] != flags[1]);
    String[] labels = (String[]) row[3];
    Arrays.sort(labels);
    assertEquals(labels, new String[]{"a", "b", "c"});
    assertEquals(result.getDataSchema().getColumnName(0), "arrayagg(value)");
  }

  @Test
  public void testGroupByFilteredAndEmpty() {
    ResultTable result = query("SELECT grp, ARRAY_AGG(value, true), ARRAY_AGG(stamp, true) "
        + "FROM testTable GROUP BY grp ORDER BY grp");
    assertEquals(result.getRows().size(), 2);
    assertEquals(result.getRows().get(0)[0], "a");
    assertEquals(Array.getLength(result.getRows().get(0)[1]), 2);
    assertEquals(result.getRows().get(1)[0], "n");
    assertEquals(Array.getLength(result.getRows().get(1)[1]), 0);
    assertEquals(Array.getLength(result.getRows().get(1)[2]), 0);

    result = query("SELECT ARRAY_AGG(value) FILTER (WHERE grp = 'n'), "
        + "ARRAY_AGG(stamp) FILTER (WHERE grp = 'missing'), ARRAY_AGG(flag) FROM testTable WHERE grp = 'n'");
    assertEquals(result.getDataSchema().getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.INT_ARRAY,
        ColumnDataType.TIMESTAMP_ARRAY, ColumnDataType.BOOLEAN_ARRAY});
    for (Object value : result.getRows().get(0)) {
      assertEquals(Array.getLength(value), 0);
    }
    result = query("SELECT ARRAY_AGG(stamp), ARRAY_AGG(flag) FROM testTable WHERE grp = 'missing'");
    assertEquals(result.getDataSchema().getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.TIMESTAMP_ARRAY, ColumnDataType.BOOLEAN_ARRAY});
    assertEquals(Array.getLength(result.getRows().get(0)[0]), 0);
    assertEquals(Array.getLength(result.getRows().get(0)[1]), 0);
  }

  private ResultTable query(String sql) {
    BrokerResponseNative response = getBrokerResponseForOptimizedQuery("SET enableNullHandling = true; " + sql, SCHEMA);
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    return response.getResultTable();
  }

  private static GenericRow row(String group, Integer value, Long stamp, Boolean flag, String... labels) {
    GenericRow row = new GenericRow();
    row.putValue("grp", group);
    row.putValue("value", value);
    row.putValue("stamp", stamp);
    row.putValue("flag", flag);
    row.putValue("labels", labels);
    return row;
  }

  private void buildSegment(int id, boolean raw, List<GenericRow> rows)
      throws Exception {
    TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setNoDictionaryColumns(raw ? List.of("value", "stamp", "flag") : List.of()).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(table, SCHEMA);
    config.setSegmentName("segment" + id);
    config.setOutDir(_indexDir.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    _segments.add(ImmutableSegmentLoader.load(new File(_indexDir, "segment" + id), ReadMode.mmap));
  }
}
