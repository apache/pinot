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
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.frequencies.LongsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/**
 * Tests for FREQUENT_STRINGS_SKETCH and FREQUENT_LONGS_SKETCH aggregation functions.
 *
 * <ul>
 *   <li>Generates a segment with LONG, STRING, SKETCH and a group-by column</li>
 *   <li>Runs aggregation and group-by queries on the generated segment</li>
 *   <li>Compares the results from sketches to exact calculations via count()</li>
 * </ul>
 */
public class FrequentItemsSketchQueriesTest extends BaseQueriesTest {
  protected static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "FrequentItemsQueriesTest");
  protected static final String TABLE_NAME = "testTable";
  protected static final String SEGMENT_NAME = "testSegment";

  protected static final int MAX_MAP_SIZE = 64;
  protected static final String LONG_COLUMN = "longColumn";
  protected static final String STRING_COLUMN = "stringColumn";
  protected static final String STRING_SKETCH_COLUMN = "stringSketchColumn";
  protected static final String LONG_SKETCH_COLUMN = "longSketchColumn";
  protected static final String GROUP_BY_COLUMN = "groupByColumn";

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return ""; // No filtering required for this test.
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
    FileUtils.deleteQuietly(INDEX_DIR);

    buildSegment();
    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  protected void buildSegment() throws Exception {

    // Values chosen with distinct frequencies not to create ambiguity in testing
    String[] strValues = new String[] {"a", "a", "a", "b", "b", "a", "d", "d", "c", "d"};
    Long[] longValues = new Long[] {1L, 2L, 1L, 1L, 1L, 2L, 5L, 4L, 4L, 4L};
    String[] groups = new String[] {"g1", "g1", "g1", "g1", "g1", "g1", "g2", "g2", "g2", "g2"};

    List<GenericRow> rows = new ArrayList<>(strValues.length);
    for (int i = 0; i < strValues.length; i++) {
      GenericRow row = new GenericRow();

      row.putValue(LONG_COLUMN, longValues[i]);
      row.putValue(STRING_COLUMN, strValues[i]);

      LongsSketch longSketch = new LongsSketch(MAX_MAP_SIZE);
      longSketch.update(longValues[i]);
      row.putValue(LONG_SKETCH_COLUMN, longSketch.toByteArray());

      ItemsSketch<String> strSketch = new ItemsSketch<>(MAX_MAP_SIZE);
      strSketch.update(strValues[i]);
      row.putValue(STRING_SKETCH_COLUMN, strSketch.toByteArray(new ArrayOfStringsSerDe()));

      row.putValue(GROUP_BY_COLUMN, groups[i]);

      rows.add(row);
    }

    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(LONG_COLUMN, FieldSpec.DataType.LONG, true));
    schema.addField(new DimensionFieldSpec(STRING_COLUMN, FieldSpec.DataType.STRING, true));
    schema.addField(new MetricFieldSpec(LONG_SKETCH_COLUMN, FieldSpec.DataType.BYTES));
    schema.addField(new MetricFieldSpec(STRING_SKETCH_COLUMN, FieldSpec.DataType.BYTES));
    schema.addField(new DimensionFieldSpec(GROUP_BY_COLUMN, FieldSpec.DataType.STRING, true));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  @Test
  public void testAggregationForStringValues() {
    // Fetch the sketch object which collects Frequent Items
    String query = String.format(
        "SELECT FREQUENTSTRINGSSKETCH(%1$s) FROM %2$s", STRING_COLUMN, TABLE_NAME);
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    List<Object> aggregationResult = resultsBlock.getResults();

    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 1);

    // Fetch the exact list by count/group-by and compare
    String[] exactOrdered = getExactOrderedStrings();
    assertStringsSketch((ItemsSketch<String>) aggregationResult.get(0), exactOrdered);
  }

  @Test
  public void testAggregationForLongValues() {
    // Fetch the sketch object which collects Frequent Items
    String query = String.format(
        "SELECT FREQUENTLONGSSKETCH(%1$s) FROM %2$s", LONG_COLUMN, TABLE_NAME);
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    List<Object> aggregationResult = resultsBlock.getResults();

    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 1);

    // Fetch the exact list by count/group-by and compare
    Long[] exactOrdered = getExactOrderedLongs();
    assertLongsSketch((LongsSketch) aggregationResult.get(0), exactOrdered);
  }

  @Test
  public void testAggregationForStringSketches() {
    // Retrieve sketches calculated by: 1) merger of sketches, 2) from plain values
    String query = String.format(
        "SELECT FREQUENTSTRINGSSKETCH(%1$s), FREQUENTSTRINGSSKETCH(%2$s) FROM %3$s",
        STRING_SKETCH_COLUMN, STRING_COLUMN, TABLE_NAME);
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    List<Object> aggregationResult = resultsBlock.getResults();

    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 2);

    // Assert the sketches are equivalent
    ItemsSketch<String> sketch1 = (ItemsSketch<String>) aggregationResult.get(0);
    ItemsSketch<String> sketch2 = (ItemsSketch<String>) aggregationResult.get(1);
    assertEquals(
        sketch1.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES),
        sketch2.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES));
  }

  @Test
  public void testAggregationForLongSketches() {
    // Retrieve sketches calculated by: 1) merger of sketches, 2) from plain values
    String query = String.format(
        "SELECT FREQUENTLONGSSKETCH(%1$s), FREQUENTLONGSSKETCH(%2$s) FROM %3$s",
        LONG_SKETCH_COLUMN, LONG_COLUMN, TABLE_NAME);
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    List<Object> aggregationResult = resultsBlock.getResults();

    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 2);

    // Assert the sketches are equivalent
    LongsSketch sketch1 = (LongsSketch) aggregationResult.get(0);
    LongsSketch sketch2 = (LongsSketch) aggregationResult.get(1);
    assertEquals(
        sketch1.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES),
        sketch2.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES));
  }

  @Test
  public void testGroupByStringSketches() {
    // Fetch the sketch object which collects Frequent Items
    String query = String.format(
        "SELECT %1$s, FREQUENTSTRINGSSKETCH(%2$s) FROM %3$s GROUP BY 1",
        GROUP_BY_COLUMN, STRING_COLUMN, TABLE_NAME);
    BrokerResponse brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();

    assertNotNull(rows);
    assertEquals(rows.size(), 2); // should be 2 groups

    // Fetch the exact list by count/group-by and compare
    Map<String, ArrayList<String>> exactOrdered = getExactOrderedStringGroups();
    for (Object[] row: rows) {
      String group = (String) row[0];
      ItemsSketch<String> sketch = decodeStringsSketch((String) row[1]);
      List<String> exactOrder = exactOrdered.get(group);
      assertStringsSketch(sketch, exactOrder);
    }
  }

  @Test
  public void testGroupByLongSketches() {
    // Fetch the sketch object which collects Frequent Items
    String query = String.format(
        "SELECT %1$s, FREQUENTLONGSSKETCH(%2$s) FROM %3$s GROUP BY 1",
        GROUP_BY_COLUMN, LONG_COLUMN, TABLE_NAME);
    BrokerResponse brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();

    assertNotNull(rows);
    assertEquals(rows.size(), 2); // should be 2 groups

    // Fetch the exact list by count/group-by and compare
    Map<String, ArrayList<Long>> exactOrdered = getExactOrderedLongGroups();
    for (Object[] row: rows) {
      String group = (String) row[0];
      LongsSketch sketch = decodeLongsSketch((String) row[1]);
      List<Long> exactOrder = exactOrdered.get(group);
      assertLongsSketch(sketch, exactOrder);
    }
  }


  private String[] getExactOrderedStrings() {
    Object[] objects = getExactOrderForColumn(STRING_COLUMN);
    return Arrays.copyOf(objects, objects.length, String[].class);
  }

  private Long[] getExactOrderedLongs() {
    Object[] objects = getExactOrderForColumn(LONG_COLUMN);
    return Arrays.copyOf(objects, objects.length, Long[].class);
  }

  private Object[] getExactOrderForColumn(String col) {
    String query = String.format(
        "SELECT %1$s, COUNT(1) FROM %2$s GROUP BY 1 ORDER BY 2 DESC", col, TABLE_NAME);
    BrokerResponse resp = getBrokerResponse(query);
    ResultTable results = resp.getResultTable();
    List<Object[]> rows = results.getRows();
    return rows.stream().map((Object[] row) -> row[0]).toArray();
  }

  private Object[] getExactOrderForColumn2(String query) {
    BrokerResponse resp = getBrokerResponse(query);
    ResultTable results = resp.getResultTable();
    List<Object[]> rows = results.getRows();
    return rows.stream().map((Object[] row) -> row[0]).toArray();
  }

  private Map<String, ArrayList<String>> getExactOrderedStringGroups() {
    String query = String.format(
        "SELECT %1$s, %2$s, COUNT(1) FROM %3$s GROUP BY 1,2 ORDER BY 3 DESC",
        GROUP_BY_COLUMN, STRING_COLUMN, TABLE_NAME);
    BrokerResponse resp = getBrokerResponse(query);
    ResultTable results = resp.getResultTable();
    List<Object[]> rows = results.getRows();
    Map<String, ArrayList<String>> order = new HashMap<>();
    for (Object[] row: rows) {
      String group = (String) row[0];
      if (!order.containsKey(group)) {
        order.put(group, new ArrayList<>());
      }
      order.get(group).add((String) row[1]);
    }
    return order;
  }

  private Map<String, ArrayList<Long>> getExactOrderedLongGroups() {
    String query = String.format(
        "SELECT %1$s, %2$s, COUNT(1) FROM %3$s GROUP BY 1,2 ORDER BY 3 DESC",
        GROUP_BY_COLUMN, LONG_COLUMN, TABLE_NAME);
    BrokerResponse resp = getBrokerResponse(query);
    ResultTable results = resp.getResultTable();
    List<Object[]> rows = results.getRows();
    Map<String, ArrayList<Long>> order = new HashMap<>();
    for (Object[] row: rows) {
      String group = (String) row[0];
      if (!order.containsKey(group)) {
        order.put(group, new ArrayList<>());
      }
      order.get(group).add((Long) row[1]);
    }
    return order;
  }

  private void assertStringsSketch(ItemsSketch<String> sketch, List<String> exact) {
    String[] arr = new String[exact.size()];
    exact.toArray(arr);
    assertStringsSketch(sketch, arr);
  }

  private void assertStringsSketch(ItemsSketch<String> sketch, String[] exact) {
    ItemsSketch.Row[] items = sketch.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
    assertEquals(exact.length, items.length);
    for (int i = 0; i < exact.length; i++) {
      assertEquals((String) items[i].getItem(), exact[i]);
    }
  }

  private void assertLongsSketch(LongsSketch sketch, List<Long> exact) {
    Long[] arr = new Long[exact.size()];
    exact.toArray(arr);
    assertLongsSketch(sketch, arr);
  }

  private void assertLongsSketch(LongsSketch sketch, Long[] exact) {
    LongsSketch.Row[] items = sketch.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
    assertEquals(exact.length, items.length);
    for (int i = 0; i < exact.length; i++) {
      assertEquals((Long) items[i].getItem(), exact[i]);
    }
  }

  private ItemsSketch<String> decodeStringsSketch(String encodedSketch) {
    byte[] byteArr = Base64.getDecoder().decode(encodedSketch);
    return ItemsSketch.getInstance(Memory.wrap(byteArr), new ArrayOfStringsSerDe());
  }

  private LongsSketch decodeLongsSketch(String encodedSketch) {
    byte[] byteArr = Base64.getDecoder().decode(encodedSketch);
    return LongsSketch.getInstance(Memory.wrap(byteArr));
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
