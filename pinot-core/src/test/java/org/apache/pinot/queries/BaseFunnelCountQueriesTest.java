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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Base queries test for FUNNEL_COUNT queries.
 * Each strategy gets its own test.
 */
@SuppressWarnings("rawtypes")
abstract public class BaseFunnelCountQueriesTest extends BaseQueriesTest {
  protected static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "FunnelCountQueriesTest");
  protected static final String RAW_TABLE_NAME = "testTable";
  protected static final String SEGMENT_NAME = "testSegment";
  protected static final Random RANDOM = new Random();

  protected static final int NUM_RECORDS = 2000;
  protected static final int MAX_VALUE = 1000;
  protected static final int NUM_GROUPS = 100;
  protected static final int FILTER_LIMIT = 50;
  protected static final String ID_COLUMN = "idColumn";
  protected static final String STEP_COLUMN = "stepColumn";
  protected static final String[] STEPS = {"A", "B"};
  protected static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(ID_COLUMN, DataType.INT)
      .addSingleValueDimension(STEP_COLUMN, DataType.STRING)
      .build();
  protected static final TableConfigBuilder TABLE_CONFIG_BUILDER =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME);

  private Set<Integer>[] _values = new Set[2];
  private List<Integer> _all = new ArrayList<>();
  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  protected abstract int getExpectedNumEntriesScannedInFilter();
  protected abstract TableConfig getTableConfig();
  protected abstract IndexSegment buildSegment(List<GenericRow> records) throws Exception;

  @Override
  protected String getFilter() {
    return String.format(" WHERE idColumn >= %s", FILTER_LIMIT);
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

    List<GenericRow> records = genereateRows();
    _indexSegment = buildSegment(records);
    _indexSegments = Arrays.asList(_indexSegment, _indexSegment);
  }

  private List<GenericRow> genereateRows() {
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    int hashMapCapacity = HashUtil.getHashMapCapacity(MAX_VALUE);
    _values[0] = new HashSet<>(hashMapCapacity);
    _values[1] = new HashSet<>(hashMapCapacity);
    for (int i = 0; i < NUM_RECORDS; i++) {
      int value = RANDOM.nextInt(MAX_VALUE);
      GenericRow record = new GenericRow();
      record.putValue(ID_COLUMN, value);
      record.putValue(STEP_COLUMN, STEPS[i % 2]);
      records.add(record);
      _all.add(Integer.hashCode(value));
      _values[i % 2].add(Integer.hashCode(value));
    }
    return records;
  }

  @Test
  public void testAggregationOnly() {
    String query = String.format("SELECT "
        + "FUNNEL_COUNT("
        + " STEPS(stepColumn = 'A', stepColumn = 'B'),"
        + " CORRELATE_BY(idColumn)"
        + ") FROM testTable");

    // Inner segment
    Predicate<Integer> filter = id -> id >= FILTER_LIMIT;
    long expectedFilteredNumDocs = _all.stream().filter(filter).count();
    Set<Integer> filteredA = _values[0].stream().filter(filter).collect(Collectors.toSet());
    Set<Integer> filteredB = _values[1].stream().filter(filter).collect(Collectors.toSet());
    Set<Integer> intersection = new HashSet<>(filteredA);
    intersection.retainAll(filteredB);
    long[] expectedResult = { filteredA.size(), intersection.size() };

    Operator operator = getOperatorWithFilter(query);
    assertTrue(operator instanceof AggregationOperator);
    AggregationResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(),
        expectedFilteredNumDocs, getExpectedNumEntriesScannedInFilter(), 2 * expectedFilteredNumDocs, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 1);
    for (int i = 0; i < 2; i++) {
      assertEquals(((LongArrayList) aggregationResult.get(0)).getLong(i), expectedResult[i]);
    }

    // Inter segments (expect 4 * inner segment result)
    for (int i = 0; i < 2; i++) {
      expectedResult[i] = 4 * expectedResult[i];
    }
    Object[] expectedResults = { expectedResult };

    QueriesTestUtils.testInterSegmentsResult(getBrokerResponseWithFilter(query),
        4 * expectedFilteredNumDocs, 4 * getExpectedNumEntriesScannedInFilter(), 4 * 2 * expectedFilteredNumDocs,
        4 * NUM_RECORDS, expectedResults);
  }

  @Test
  public void testAggregationGroupBy() {
    String query = String.format("SELECT "
        + "MOD(idColumn, %s), "
        + "FUNNEL_COUNT("
        + " STEPS(stepColumn = 'A', stepColumn = 'B'),"
        + " CORRELATE_BY(idColumn)"
        + ") FROM testTable "
        + "WHERE idColumn >= %s "
        + "GROUP BY 1 ORDER BY 1 LIMIT %s", NUM_GROUPS, FILTER_LIMIT, NUM_GROUPS);

    // Inner segment
    Set<Integer>[] filteredA = new Set[NUM_GROUPS];
    Set<Integer>[] filteredB = new Set[NUM_GROUPS];
    Set<Integer>[] intersection = new Set[NUM_GROUPS];
    long[][] expectedResult = new long[NUM_GROUPS][];

    long expectedFilteredNumDocs = _all.stream().filter(id -> id >= FILTER_LIMIT).count();

    int expectedNumGroups = 0;
    for (int i = 0; i < NUM_GROUPS; i++) {
      final int group = i;
      Predicate<Integer> filter = id -> id >= FILTER_LIMIT && id % NUM_GROUPS == group;
      filteredA[group] = _values[0].stream().filter(filter).collect(Collectors.toSet());
      filteredB[group] = _values[1].stream().filter(filter).collect(Collectors.toSet());
      intersection[group] = new HashSet<>(filteredA[group]);
      intersection[group].retainAll(filteredB[group]);
      if (!filteredA[i].isEmpty() || !filteredB[i].isEmpty()) {
        expectedNumGroups++;
        expectedResult[group] = new long[] { filteredA[group].size(), intersection[group].size() };
      }
    }

    // Inner segment
    GroupByOperator groupByOperator = getOperator(query);
    GroupByResultsBlock resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(),
        expectedFilteredNumDocs, getExpectedNumEntriesScannedInFilter(), 2 * expectedFilteredNumDocs, NUM_RECORDS);

    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);
    int numGroups = 0;
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
    while (groupKeyIterator.hasNext()) {
      numGroups++;
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      int key = ((Double) groupKey._keys[0]).intValue();
      assertEquals(aggregationGroupByResult.getResultForGroupId(0, groupKey._groupId),
          new LongArrayList(expectedResult[key]));
    }
    assertEquals(numGroups, expectedNumGroups);

    // Inter segments (expect 4 * inner segment result)
    List<Object[]> expectedRows = new ArrayList<>();
    for (int i = 0; i < NUM_GROUPS; i++) {
      if (expectedResult[i] == null) {
        continue;
      }
      for (int step = 0; step < 2; step++) {
          expectedResult[i][step] = 4 * expectedResult[i][step];
      }
      Object[] expectedRow = { Double.valueOf(i), expectedResult[i] };
      expectedRows.add(expectedRow);
    }

    // Inter segments (expect 4 * inner segment result)
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(query),
        4 * expectedFilteredNumDocs, 4 * getExpectedNumEntriesScannedInFilter(), 4 * 2 * expectedFilteredNumDocs,
        4 * NUM_RECORDS, expectedRows);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
