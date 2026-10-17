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
package org.apache.pinot.core.data.table;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.groupby.DoubleGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Tests the functionality of [@TableResizer]
public class TableResizerTest {
  private static final String QUERY_PREFIX =
      "SELECT SUM(m1), MAX(m2), DISTINCTCOUNT(m3), AVG(m4) FROM testTable GROUP BY d1, d2, d3 ORDER BY ";
  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"d1", "d2", "d3", "sum(m1)", "max(m2)", "distinctcount(m3)", "avg(m4)"},
          new DataSchema.ColumnDataType[]{
              DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE,
              DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.OBJECT,
              DataSchema.ColumnDataType.OBJECT
          });
  private static final int TRIM_TO_SIZE = 3;
  private static final int NUM_RESULT_HOLDER = 4;

  private Map<Key, Record> _recordsMap;
  private List<Record> _records;
  private List<Key> _keys;
  private GroupKeyGenerator _groupKeyGenerator;
  private GroupByResultHolder[] _groupByResultHolders;

  @BeforeClass
  public void setUp() {
    _records = Arrays.asList(new Record(new Object[]{
        "a", 10, 1.0, 10.0, 100.0, new IntOpenHashSet(new int[]{1}), new AvgPair(10, 2) /* 5 */
    }), new Record(new Object[]{
        "b", 10, 2.0, 20.0, 200.0, new IntOpenHashSet(new int[]{1, 2}), new AvgPair(10, 3) /* 3.33 */
    }), new Record(new Object[]{
        "c", 200, 3.0, 30.0, 300.0, new IntOpenHashSet(new int[]{1, 2}), new AvgPair(20, 4) /* 5 */
    }), new Record(new Object[]{
        "c", 50, 4.0, 30.0, 200.0, new IntOpenHashSet(new int[]{1, 2, 3}), new AvgPair(30, 10) /* 3 */
    }), new Record(new Object[]{
        "c", 300, 5.0, 20.0, 100.0, new IntOpenHashSet(new int[]{1, 2, 3, 4}), new AvgPair(10, 5) /* 2 */
    }));
    _keys = Arrays.asList(new Key(new Object[]{"a", 10, 1.0}), new Key(new Object[]{"b", 10, 2.0}),
        new Key(new Object[]{"c", 200, 3.0}), new Key(new Object[]{"c", 50, 4.0}),
        new Key(new Object[]{"c", 300, 5.0}));

    int numRecords = _records.size();
    _recordsMap = new HashMap<>();
    for (int i = 0; i < numRecords; i++) {
      _recordsMap.put(_keys.get(i), _records.get(i));
    }

    // Use _keys for groupKeys
    List<GroupKeyGenerator.GroupKey> groupKeys = new ArrayList<>(numRecords);
    for (int i = 0; i < numRecords; i++) {
      GroupKeyGenerator.GroupKey groupKey = new GroupKeyGenerator.GroupKey();
      groupKey._groupId = i;
      groupKey._keys = _keys.get(i).getValues();
      groupKeys.add(groupKey);
    }

    // groupByResults are the same as _records
    _groupByResultHolders = new GroupByResultHolder[NUM_RESULT_HOLDER];
    _groupByResultHolders[0] = new DoubleGroupByResultHolder(numRecords, numRecords, 0.0);
    _groupByResultHolders[1] = new DoubleGroupByResultHolder(numRecords, numRecords, 0.0);
    _groupByResultHolders[2] = new ObjectGroupByResultHolder(numRecords, numRecords);
    _groupByResultHolders[3] = new ObjectGroupByResultHolder(numRecords, numRecords);
    for (int i = 0; i < numRecords; i++) {
      Record record = _records.get(i);
      _groupByResultHolders[0].setValueForKey(i, (double) record.getValues()[3]);
      _groupByResultHolders[1].setValueForKey(i, (double) record.getValues()[4]);
      _groupByResultHolders[2].setValueForKey(i, record.getValues()[5]);
      _groupByResultHolders[3].setValueForKey(i, record.getValues()[6]);
    }

    _groupKeyGenerator = mock(GroupKeyGenerator.class);
    when(_groupKeyGenerator.getNumKeys()).thenReturn(numRecords);
    when(_groupKeyGenerator.getGroupKeys()).then(invocation -> groupKeys.iterator());
  }

  @Test
  public void testResizeRecordsMap() {
    // Test resize algorithm with numRecordsToEvict < trimToSize.
    // TotalRecords=5; trimToSize=3; numRecordsToEvict=2

    // d1 asc
    TableResizer tableResizer =
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "d1"));
    Map<Key, Record> recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(recordsMap.size(), TRIM_TO_SIZE);
    assertTrue(recordsMap.containsKey(_keys.get(0))); // a, b
    assertTrue(recordsMap.containsKey(_keys.get(1)));

    // d1 desc
    tableResizer =
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "d1 DESC"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(recordsMap.size(), TRIM_TO_SIZE);
    assertTrue(recordsMap.containsKey(_keys.get(2))); // c, c, c
    assertTrue(recordsMap.containsKey(_keys.get(3)));
    assertTrue(recordsMap.containsKey(_keys.get(4)));

    // d1 asc, d3 desc (tie breaking with 2nd comparator)
    tableResizer =
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "d1, d3 DESC"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(recordsMap.size(), TRIM_TO_SIZE);
    assertTrue(recordsMap.containsKey(_keys.get(0))); // a, b, c (300)
    assertTrue(recordsMap.containsKey(_keys.get(1)));
    assertTrue(recordsMap.containsKey(_keys.get(4)));

    // d1 asc, sum(m1) desc, max(m2) desc
    tableResizer = new TableResizer(DATA_SCHEMA,
        QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "d1, SUM(m1) DESC, max(m2) DESC"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(recordsMap.size(), TRIM_TO_SIZE);
    assertTrue(recordsMap.containsKey(_keys.get(0))); // a, b, c (30, 300)
    assertTrue(recordsMap.containsKey(_keys.get(1)));
    assertTrue(recordsMap.containsKey(_keys.get(2)));

    // avg(m4) asc (object type)
    tableResizer =
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "AVG(m4)"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(recordsMap.size(), TRIM_TO_SIZE);
    assertTrue(recordsMap.containsKey(_keys.get(4))); // 2, 3, 3.33
    assertTrue(recordsMap.containsKey(_keys.get(3)));
    assertTrue(recordsMap.containsKey(_keys.get(1)));

    // distinctcount(m3) desc, d1 asc (non-comparable intermediate result)
    tableResizer = new TableResizer(DATA_SCHEMA,
        QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "DISTINCTCOUNT(m3) DESC, d1"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(recordsMap.size(), TRIM_TO_SIZE);
    assertTrue(recordsMap.containsKey(_keys.get(4))); // 4, 3, 2 (b)
    assertTrue(recordsMap.containsKey(_keys.get(3)));
    assertTrue(recordsMap.containsKey(_keys.get(1)));

    // d2 + d3 asc (post-aggregation)
    tableResizer =
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "d2 + d3"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(recordsMap.size(), TRIM_TO_SIZE);
    assertTrue(recordsMap.containsKey(_keys.get(0))); // 11, 12, 54
    assertTrue(recordsMap.containsKey(_keys.get(1)));
    assertTrue(recordsMap.containsKey(_keys.get(3)));

    // sum(m1) * d3 desc (post-aggregation)
    tableResizer = new TableResizer(DATA_SCHEMA,
        QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "SUM(m1) * d3 DESC"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(recordsMap.size(), TRIM_TO_SIZE);
    assertTrue(recordsMap.containsKey(_keys.get(3))); // 120, 100, 90
    assertTrue(recordsMap.containsKey(_keys.get(4)));
    assertTrue(recordsMap.containsKey(_keys.get(2)));

    // d2 / (distinctcount(m3) + 1) asc, d1 desc (post-aggregation)
    tableResizer = new TableResizer(DATA_SCHEMA,
        QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "d2 / (DISTINCTCOUNT(m3) + 1), d1 DESC"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(recordsMap.size(), TRIM_TO_SIZE);
    assertTrue(recordsMap.containsKey(_keys.get(1))); // 3.33, 12.5, 5
    assertTrue(recordsMap.containsKey(_keys.get(0)));
    assertTrue(recordsMap.containsKey(_keys.get(3)));

    // Test resize algorithm with numRecordsToEvict > trimToSize.
    // TotalRecords=5; trimToSize=2; numRecordsToEvict=3
    int trimToSize = 2;

    // d1 asc
    tableResizer =
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "d1"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    assertEquals(recordsMap.size(), trimToSize);
    assertTrue(recordsMap.containsKey(_keys.get(0))); // a, b
    assertTrue(recordsMap.containsKey(_keys.get(1)));

    // avg(m4) asc (object type)
    tableResizer =
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "AVG(m4)"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    assertEquals(recordsMap.size(), trimToSize);
    assertTrue(recordsMap.containsKey(_keys.get(4))); // 2, 3
    assertTrue(recordsMap.containsKey(_keys.get(3)));

    // distinctcount(m3) desc, d1 asc (non-comparable intermediate result)
    tableResizer = new TableResizer(DATA_SCHEMA,
        QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "DISTINCTCOUNT(m3) DESC, d1"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    assertEquals(recordsMap.size(), trimToSize);
    assertTrue(recordsMap.containsKey(_keys.get(4))); // 4, 3
    assertTrue(recordsMap.containsKey(_keys.get(3)));

    // d2 / (distinctcount(m3) + 1) asc, d1 desc (post-aggregation)
    tableResizer = new TableResizer(DATA_SCHEMA,
        QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "d2 / (DISTINCTCOUNT(m3) + 1), d1 DESC"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    assertEquals(recordsMap.size(), trimToSize);
    assertTrue(recordsMap.containsKey(_keys.get(1))); // 3.33, 12.5
    assertTrue(recordsMap.containsKey(_keys.get(0)));
  }

  /// Tests the sort function for ordered resizer
  @Test
  public void testSortTopRecords() {
    // d1 asc
    TableResizer tableResizer =
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "d1"));
    Map<Key, Record> recordsMap = new HashMap<>(_recordsMap);
    List<Record> sortedRecords = tableResizer.getSortedTopRecords(recordsMap, TRIM_TO_SIZE);
    assertEquals(sortedRecords.size(), TRIM_TO_SIZE);
    assertEquals(sortedRecords.get(0), _records.get(0));  // a, b
    assertEquals(sortedRecords.get(1), _records.get(1));

    // d1 asc - trim to 1
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.getSortedTopRecords(recordsMap, 1);
    assertEquals(sortedRecords.get(0), _records.get(0));  // a

    // d1 asc, d3 desc (tie breaking with 2nd comparator)
    tableResizer =
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "d1, d3 DESC"));
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.getSortedTopRecords(recordsMap, TRIM_TO_SIZE);
    assertEquals(sortedRecords.size(), TRIM_TO_SIZE);
    assertEquals(sortedRecords.get(0), _records.get(0));  // a, b, c (300)
    assertEquals(sortedRecords.get(1), _records.get(1));
    assertEquals(sortedRecords.get(2), _records.get(4));

    // d1 asc, d3 desc (tie breaking with 2nd comparator) - trim to 1
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.getSortedTopRecords(recordsMap, 1);
    assertEquals(sortedRecords.size(), 1);
    assertEquals(sortedRecords.get(0), _records.get(0));  // a

    // d1 asc, sum(m1) desc, max(m2) desc
    tableResizer = new TableResizer(DATA_SCHEMA,
        QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "d1, SUM(m1) DESC, max(m2) DESC"));
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.getSortedTopRecords(recordsMap, TRIM_TO_SIZE);
    assertEquals(sortedRecords.size(), TRIM_TO_SIZE);
    assertEquals(sortedRecords.get(0), _records.get(0));  // a, b, c (30, 300)
    assertEquals(sortedRecords.get(1), _records.get(1));
    assertEquals(sortedRecords.get(2), _records.get(2));

    // d1 asc, sum(m1) desc, max(m2) desc - trim to 1
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.getSortedTopRecords(recordsMap, 1);
    assertEquals(sortedRecords.size(), 1);
    assertEquals(sortedRecords.get(0), _records.get(0));  // a

    // avg(m4) asc (object type)
    tableResizer =
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "AVG(m4)"));
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.getSortedTopRecords(recordsMap, TRIM_TO_SIZE);
    assertEquals(sortedRecords.size(), TRIM_TO_SIZE);
    assertEquals(sortedRecords.get(0), _records.get(4));  // 2, 3, 3.33
    assertEquals(sortedRecords.get(1), _records.get(3));
    assertEquals(sortedRecords.get(2), _records.get(1));

    // distinctcount(m3) desc, d1 asc (non-comparable intermediate result)
    tableResizer = new TableResizer(DATA_SCHEMA,
        QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "DISTINCTCOUNT(m3) DESC, d1"));
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.getSortedTopRecords(recordsMap, TRIM_TO_SIZE);
    assertEquals(sortedRecords.size(), TRIM_TO_SIZE);
    assertEquals(sortedRecords.get(0), _records.get(4));  // 4, 3, 2 (b)
    assertEquals(sortedRecords.get(1), _records.get(3));
    assertEquals(sortedRecords.get(2), _records.get(1));

    // d2 / (distinctcount(m3) + 1) asc, d1 desc (post-aggregation)
    tableResizer = new TableResizer(DATA_SCHEMA,
        QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "d2 / (DISTINCTCOUNT(m3) + 1), d1 DESC"));
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.getSortedTopRecords(recordsMap, TRIM_TO_SIZE);
    assertEquals(sortedRecords.size(), TRIM_TO_SIZE);
    assertEquals(sortedRecords.get(0), _records.get(1));  // 3.33, 12.5, 5
    assertEquals(sortedRecords.get(1), _records.get(0));
    assertEquals(sortedRecords.get(2), _records.get(3));
  }

  /// Tests in-segment trim from 15 records to 10 records
  @Test
  public void testInSegmentTrim() {
    TableResizer tableResizer =
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "d3 DESC"));
    List<IntermediateRecord> results =
        tableResizer.trimInSegmentResults(_groupKeyGenerator, _groupByResultHolders, TRIM_TO_SIZE, false);
    assertEquals(results.size(), TRIM_TO_SIZE);
    //  _records[4],  _records[3],  _records[2]
    assertEquals(results.get(0)._record, _records.get(2));
    if (results.get(1)._record.equals(_records.get(3))) {
      assertEquals(results.get(2)._record, _records.get(4));
    } else {
      assertEquals(results.get(1)._record, _records.get(4));
      assertEquals(results.get(2)._record, _records.get(3));
    }

    tableResizer = new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContext(
        QUERY_PREFIX + "SUM(m1) DESC, max(m2) DESC, DISTINCTCOUNT(m3) DESC"));
    results = tableResizer.trimInSegmentResults(_groupKeyGenerator, _groupByResultHolders, TRIM_TO_SIZE, false);
    assertEquals(results.size(), TRIM_TO_SIZE);
    // _records[2],  _records[3],  _records[1]
    assertEquals(results.get(0)._record, _records.get(1));
    if (results.get(1)._record.equals(_records.get(3))) {
      assertEquals(results.get(2)._record, _records.get(2));
    } else {
      assertEquals(results.get(1)._record, _records.get(2));
      assertEquals(results.get(2)._record, _records.get(3));
    }

    tableResizer = new TableResizer(DATA_SCHEMA,
        QueryContextConverterUtils.getQueryContext(QUERY_PREFIX + "DISTINCTCOUNT(m3) DESC, AVG(m4) ASC"));
    results = tableResizer.trimInSegmentResults(_groupKeyGenerator, _groupByResultHolders, TRIM_TO_SIZE, false);
    assertEquals(results.size(), TRIM_TO_SIZE);
    // _records[4],  _records[3],  _records[1]
    assertEquals(results.get(0)._record, _records.get(1));
    if (results.get(1)._record.equals(_records.get(3))) {
      assertEquals(results.get(2)._record, _records.get(4));
    } else {
      assertEquals(results.get(1)._record, _records.get(4));
      assertEquals(results.get(2)._record, _records.get(3));
    }
  }

  @Test
  public void testTopRecordsAlternatingAcceptedAndRejectedCandidates() {
    Map<Key, Record> records = recordsWithMetrics(30.0, 10.0, 20.0, 5.0, 40.0, 4.0, 50.0, 3.0, 60.0);
    TableResizer resizer = sumResizer("SUM(m) DESC, id", false);

    // With three retained entries, rejected and accepted candidates alternate after the initial heap.
    // The unsorted result pins the existing heap order as well as the retained Record identities.
    assertRecordOrder(resizer.getTopRecords(records, 3, false), records, 4, 6, 8);
    assertRecordOrder(resizer.getTopRecords(records, 3, true), records, 8, 6, 4);
    assertEquals(records.size(), 9);
    assertEquals(records.get(new Key(new Object[]{4})).getValues(), new Object[]{4, 40.0});
  }

  @Test
  public void testTopRecordsPreservesRecordsAcrossRejectionRuns() {
    Map<Key, Record> records = recordsWithMetrics(10.0, 20.0, 30.0, 1.0, 2.0, 40.0, 50.0, 3.0, 4.0);
    TableResizer resizer = sumResizer("SUM(m) DESC, id", false);

    // A new maximum follows two rejected entries, then another maximum and a final rejection run.
    // Check both the retained record identities and the heap order after every admission path.
    assertRecordOrder(resizer.getTopRecords(records, 2, false), records, 5, 6);
    assertRecordOrder(resizer.getTopRecords(records, 2, true), records, 6, 5);
    assertEquals(records.size(), 9);
  }

  @Test
  public void testTopRecordsRetainsExistingEntriesOnTies() {
    Map<Key, Record> records = recordsWithMetrics(1.0, 1.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 2.0);
    TableResizer resizer = sumResizer("SUM(m) DESC", false);

    // ID 8 ties the root after three replacements and must not displace it. Equal children keep the
    // existing left-child preference; changing either rule changes unsorted partial-result order.
    assertRecordOrder(resizer.getTopRecords(records, 3, false), records, 7, 3, 5);
    assertRecordOrder(resizer.getTopRecords(records, 3, true), records, 3, 5, 7);
  }

  @Test
  public void testResizeRetainedAndEvictedHeapsPreserveRecords() {
    Map<Key, Record> records = recordsWithMetrics(30.0, 10.0, 20.0, 5.0, 40.0, 4.0, 50.0, 3.0, 60.0);
    TableResizer resizer = sumResizer("SUM(m) DESC, id", false);

    Map<Key, Record> retainedHeap = new LinkedHashMap<>(records);
    resizer.resizeRecordsMap(retainedHeap, 3);
    assertRecordOrder(retainedHeap.values(), records, 4, 6, 8);

    Map<Key, Record> evictedHeap = new LinkedHashMap<>(records);
    resizer.resizeRecordsMap(evictedHeap, 5);
    assertRecordOrder(evictedHeap.values(), records, 0, 2, 4, 6, 8);
    assertEquals(records.size(), 9);
  }

  @DataProvider
  public Object[][] nullAndFloatingPointOrders() {
    return new Object[][]{
        {"SUM(m) ASC NULLS FIRST, id", new int[]{3, 7, 5, 9, 4, 1, 2, 6, 0, 8}},
        {"SUM(m) ASC NULLS LAST, id", new int[]{5, 9, 4, 1, 2, 6, 0, 8, 3, 7}},
        {"SUM(m) DESC NULLS FIRST, id", new int[]{3, 7, 0, 8, 6, 2, 1, 4, 9, 5}},
        {"SUM(m) DESC NULLS LAST, id", new int[]{0, 8, 6, 2, 1, 4, 9, 5, 3, 7}}
    };
  }

  @Test(dataProvider = "nullAndFloatingPointOrders")
  public void testTopRecordsNullsAndFloatingPointOrdering(String orderBy, int[] expectedIds) {
    double firstNaN = Double.longBitsToDouble(0x7ff8000000000001L);
    double secondNaN = Double.longBitsToDouble(0x7ff8000000000002L);
    Map<Key, Record> records = recordsWithMetrics(firstNaN, 0.0, 10.0, null, -0.0,
        Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, null, secondNaN, -5.0);
    TableResizer resizer = sumResizer(orderBy, true);

    // Cover a single-entry heap, repeated recycling, and the unchanged full-sort path.
    for (int size : new int[]{1, 7, 10}) {
      assertRecordOrder(resizer.getTopRecords(records, size, true), records, Arrays.copyOf(expectedIds, size));
    }
    Map<Key, Record> resized = new LinkedHashMap<>(records);
    resizer.resizeRecordsMap(resized, 7);
    assertEquals(resized.size(), 7);
    for (int i = 0; i < 7; i++) {
      Key key = new Key(new Object[]{expectedIds[i]});
      assertSame(resized.get(key), records.get(key));
    }

    assertEquals(Double.doubleToRawLongBits((double) records.get(new Key(new Object[]{0})).getValues()[1]),
        Double.doubleToRawLongBits(firstNaN));
    assertEquals(Double.doubleToRawLongBits((double) records.get(new Key(new Object[]{8})).getValues()[1]),
        Double.doubleToRawLongBits(secondNaN));
    assertEquals(Double.doubleToRawLongBits((double) records.get(new Key(new Object[]{4})).getValues()[1]),
        Double.doubleToRawLongBits(-0.0));
  }

  @Test
  public void testTopRecordsPreservesIntermediateAggregateStates() {
    double[] averages = {30.0, 10.0, 20.0, 5.0, 40.0, 4.0, 50.0, 3.0, 60.0};
    AvgPair[] states = new AvgPair[averages.length];
    for (int i = 0; i < states.length; i++) {
      states[i] = new AvgPair(averages[i] * (i + 1), i + 1);
    }
    Map<Key, Record> records = recordsWithMetrics((Object[]) states);
    TableResizer resizer = new TableResizer(
        new DataSchema(new String[]{"id", "avg(m)"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.OBJECT}),
        QueryContextConverterUtils.getQueryContext(
            "SELECT id, AVG(m) FROM testTable GROUP BY id ORDER BY AVG(m) DESC, id"));

    assertRecordOrder(resizer.getTopRecords(records, 3, false), records, 4, 6, 8);
    assertRecordOrder(resizer.getTopRecords(records, 3, true), records, 8, 6, 4);
    for (int i = 0; i < states.length; i++) {
      assertSame(records.get(new Key(new Object[]{i})).getValues()[1], states[i]);
      assertEquals(states[i].getSum(), averages[i] * (i + 1));
      assertEquals(states[i].getCount(), (long) i + 1);
    }
  }

  private static TableResizer sumResizer(String orderBy, boolean nullHandlingEnabled) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT id, SUM(m) FROM testTable GROUP BY id ORDER BY " + orderBy);
    queryContext.setNullHandlingEnabled(nullHandlingEnabled);
    return new TableResizer(
        new DataSchema(new String[]{"id", "sum(m)"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.DOUBLE}),
        queryContext);
  }

  private static Map<Key, Record> recordsWithMetrics(Object... metrics) {
    Map<Key, Record> records = new LinkedHashMap<>();
    for (int i = 0; i < metrics.length; i++) {
      records.put(new Key(new Object[]{i}), new Record(new Object[]{i, metrics[i]}));
    }
    return records;
  }

  private static void assertRecordOrder(Collection<Record> actual, Map<Key, Record> records, int... expectedIds) {
    assertEquals(actual.size(), expectedIds.length);
    int index = 0;
    for (Record record : actual) {
      assertSame(record, records.get(new Key(new Object[]{expectedIds[index++]})));
    }
  }

  @Test
  public void testTrimInSegmentResultsByGroupingSet() {
    /// ROLLUP(d1) yields grouping sets {d1} (grouping-id 0) and {} (grouping-id 1). The record layout is
    /// [d1, $groupingId, sum(m1)], so the discriminator column index is 1 (after the single union column).
    TableResizer tableResizer = new TableResizer(
        new DataSchema(new String[]{"d1", "$groupingId", "sum(m1)"}, new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE}),
        QueryContextConverterUtils.getQueryContext(
            "SELECT d1, SUM(m1) FROM testTable GROUP BY ROLLUP(d1) ORDER BY SUM(m1) DESC"));

    /// Bucket {d1}: five detail groups with sums 10..50. Bucket {}: one grand-total group with sum 150.
    String[] details = {"a", "b", "c", "d", "e"};
    double[] sums = {10.0, 20.0, 30.0, 40.0, 50.0};
    List<GroupKeyGenerator.GroupKey> groupKeys = new ArrayList<>();
    DoubleGroupByResultHolder holder = new DoubleGroupByResultHolder(6, 6, 0.0);
    for (int i = 0; i < details.length; i++) {
      GroupKeyGenerator.GroupKey groupKey = new GroupKeyGenerator.GroupKey();
      groupKey._groupId = i;
      groupKey._keys = new Object[]{details[i], 0};   // grouping-id 0 => set {d1}
      groupKeys.add(groupKey);
      holder.setValueForKey(i, sums[i]);
    }
    GroupKeyGenerator.GroupKey grandTotal = new GroupKeyGenerator.GroupKey();
    grandTotal._groupId = 5;
    grandTotal._keys = new Object[]{null, 1};         // grouping-id 1 => set {} (d1 rolled up to NULL)
    groupKeys.add(grandTotal);
    holder.setValueForKey(5, 150.0);

    GroupKeyGenerator groupKeyGenerator = mock(GroupKeyGenerator.class);
    when(groupKeyGenerator.getGroupKeys()).then(invocation -> groupKeys.iterator());

    /// Keep top-2 PER grouping set. A global top-2 by sum would keep only {grand total (150), e (50)} -- a
    /// single detail row. The per-set trim must instead keep the grand total AND the top-2 details, so the
    /// low-cardinality detail set is not starved by the dominant grand-total row.
    List<IntermediateRecord> result =
        tableResizer.trimInSegmentResultsByGroupingSet(groupKeyGenerator, new GroupByResultHolder[]{holder}, 2, 1);

    int numDetailRows = 0;
    int numGrandTotalRows = 0;
    for (IntermediateRecord record : result) {
      int groupingId = (int) record._record.getValues()[1];
      if (groupingId == 0) {
        numDetailRows++;
        /// The kept details must be the two highest sums (e=50, d=40).
        assertTrue((double) record._record.getValues()[2] >= 40.0);
      } else {
        numGrandTotalRows++;
      }
    }
    assertEquals(result.size(), 3);
    assertEquals(numDetailRows, 2);
    assertEquals(numGrandTotalRows, 1);
  }
}
