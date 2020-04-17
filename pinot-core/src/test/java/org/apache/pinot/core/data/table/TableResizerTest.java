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

import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.function.customobject.AvgPair;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests the functionality of {@link @TableResizer}
 */
public class TableResizerTest {

  private DataSchema dataSchema;
  private List<AggregationInfo> aggregationInfos;
  private List<SelectionSort> selectionSort;
  private SelectionSort sel1;
  private SelectionSort sel2;
  private SelectionSort sel3;
  private TableResizer _tableResizer;

  private int trimToSize = 3;
  private Map<Key, Record> _recordsMap;
  private List<Record> _records;
  private List<Key> _keys;

  @BeforeClass
  public void beforeClass() {
    dataSchema = new DataSchema(new String[]{"d1", "d2", "d3", "sum(m1)", "max(m2)", "distinctcount(m3)", "avg(m4)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.OBJECT, DataSchema.ColumnDataType.OBJECT});
    AggregationInfo agg1 = new AggregationInfo();
    List<String> args1 = new ArrayList<>(1);
    args1.add("m1");
    agg1.setAggregationFunctionArgs(args1);
    agg1.setAggregationType("sum");

    AggregationInfo agg2 = new AggregationInfo();
    List<String> args2 = new ArrayList<>(1);
    args2.add("m2");
    agg2.setAggregationFunctionArgs(args2);
    agg2.setAggregationType("max");

    AggregationInfo agg3 = new AggregationInfo();
    List<String> args3 = new ArrayList<>(1);
    args3.add("m3");
    agg3.setAggregationFunctionArgs(args3);
    agg3.setAggregationType("distinctcount");

    AggregationInfo agg4 = new AggregationInfo();
    List<String> args4 = new ArrayList<>(1);
    args4.add("m4");
    agg4.setAggregationFunctionArgs(args4);
    agg4.setAggregationType("avg");
    aggregationInfos = Lists.newArrayList(agg1, agg2, agg3, agg4);

    sel1 = new SelectionSort();
    sel2 = new SelectionSort();
    sel3 = new SelectionSort();

    IntOpenHashSet i1 = new IntOpenHashSet();
    i1.add(1);
    IntOpenHashSet i2 = new IntOpenHashSet();
    i2.add(1);
    i2.add(2);
    IntOpenHashSet i3 = new IntOpenHashSet();
    i3.add(1);
    i3.add(2);
    IntOpenHashSet i4 = new IntOpenHashSet();
    i4.add(1);
    i4.add(2);
    i4.add(3);
    IntOpenHashSet i5 = new IntOpenHashSet();
    i5.add(1);
    i5.add(2);
    i5.add(3);
    i5.add(4);
    _records = Lists.newArrayList(new Record(new Object[]{"a", 10, 1.0, 10, 100, i1, new AvgPair(10, 2) /* 5 */}),
        new Record(new Object[]{"b", 10, 2.0, 20, 200, i2, new AvgPair(10, 3) /* 3.33 */}),
        new Record(new Object[]{"c", 200, 3.0, 30, 300, i3, new AvgPair(20, 4) /* 5 */}),
        new Record(new Object[]{"c", 50, 4.0, 30, 200, i4, new AvgPair(30, 10) /* 3 */}),
        new Record(new Object[]{"c", 300, 5.0, 20, 100, i5, new AvgPair(10, 5) /* 2 */}));

    _keys = Lists.newArrayList(new Key(new Object[]{"a", 10, 1.0}), new Key(new Object[]{"b", 10, 2.0}),
        new Key(new Object[]{"c", 200, 3.0}), new Key(new Object[]{"c", 50, 4.0}),
        new Key(new Object[]{"c", 300, 5.0}));
    _recordsMap = new HashMap<>();
    for (int i = 0; i < _records.size(); i++) {
      _recordsMap.put(_keys.get(i), _records.get(i));
    }
    /*_recordsMap = new HashMap<>();
    _recordsMap.put(new Key(new Object[]{"a", 10, 1.0}), new Record(new Object[]{"a", 10, 1.0,10, 100, i1, new AvgPair(10, 2) *//* 5 *//*}));
    _recordsMap.put(new Key(new Object[]{"b", 10, 2.0}), new Record(new Object[]{"b", 10, 2.0,20, 200, i2, new AvgPair(10, 3) *//* 3.33 *//*}));
    _recordsMap.put(new Key(new Object[]{"c", 200, 3.0}),new Record(new Object[]{"c", 200, 3.0,30, 300, i3, new AvgPair(20, 4) *//* 5 *//*}));
    _recordsMap.put(new Key(new Object[]{"c", 50, 4.0}), new Record(new Object[]{"c", 50, 4.0,30, 200, i4, new AvgPair(30, 10) *//* 3 *//*}));
    _recordsMap.put(new Key(new Object[]{"c", 300, 5.0}), new Record(new Object[]{"c", 300, 5.0,20, 100, i5, new AvgPair(10, 5) *//* 2 *//*}));
     */
  }

  /**
   * {@link TableResizer} resizes the _records map based on SelectionSort
   */
  @Test
  public void testResizeRecordsMap() {
    Map<Key, Record> recordsMap;
    // Test resize algorithm with numRecordsToEvict < trimToSize.
    // TotalRecords=5; trimToSize=3; numRecordsToEvict=2

    // d1 asc
    sel1.setColumn("d1");
    sel1.setIsAsc(true);
    selectionSort = Lists.newArrayList(sel1);
    recordsMap = new HashMap<>(_recordsMap);
    _tableResizer = new TableResizer(dataSchema, aggregationInfos, selectionSort);
    _tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(recordsMap.size(), trimToSize);
    Assert.assertTrue(recordsMap.containsKey(_keys.get(0))); // a, b, c
    Assert.assertTrue(recordsMap.containsKey(_keys.get(1)));

    // d1 desc
    sel1.setColumn("d1");
    sel1.setIsAsc(false);
    selectionSort = Lists.newArrayList(sel1);
    _tableResizer = new TableResizer(dataSchema, aggregationInfos, selectionSort);
    recordsMap = new HashMap<>(_recordsMap);
    _tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(recordsMap.size(), trimToSize);
    Assert.assertTrue(recordsMap.containsKey(_keys.get(2))); // c, c, c
    Assert.assertTrue(recordsMap.containsKey(_keys.get(3)));
    Assert.assertTrue(recordsMap.containsKey(_keys.get(4)));

    // d1 asc, d3 desc (tie breaking with 2nd comparator
    sel1.setColumn("d1");
    sel1.setIsAsc(true);
    sel2.setColumn("d3");
    sel2.setIsAsc(false);
    selectionSort = Lists.newArrayList(sel1, sel2);
    _tableResizer = new TableResizer(dataSchema, aggregationInfos, selectionSort);
    recordsMap = new HashMap<>(_recordsMap);
    _tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(recordsMap.size(), trimToSize);
    Assert.assertTrue(recordsMap.containsKey(_keys.get(0))); // 10, 10, 300
    Assert.assertTrue(recordsMap.containsKey(_keys.get(1)));
    Assert.assertTrue(recordsMap.containsKey(_keys.get(4)));

    // d2 asc
    sel1.setColumn("d2");
    sel1.setIsAsc(true);
    selectionSort = Lists.newArrayList(sel1);
    _tableResizer = new TableResizer(dataSchema, aggregationInfos, selectionSort);
    recordsMap = new HashMap<>(_recordsMap);
    _tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(recordsMap.size(), trimToSize);
    Assert.assertTrue(recordsMap.containsKey(_keys.get(0))); // 10, 10, 50
    Assert.assertTrue(recordsMap.containsKey(_keys.get(1)));
    Assert.assertTrue(recordsMap.containsKey(_keys.get(3)));

    // d1 asc, sum(m1) desc, max(m2) desc
    sel1.setColumn("d1");
    sel1.setIsAsc(true);
    sel2.setColumn("sum(m1)");
    sel2.setIsAsc(false);
    sel3.setColumn("max(m2)");
    sel3.setIsAsc(false);
    selectionSort = Lists.newArrayList(sel1, sel2, sel3);
    _tableResizer = new TableResizer(dataSchema, aggregationInfos, selectionSort);
    recordsMap = new HashMap<>(_recordsMap);
    _tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(recordsMap.size(), trimToSize);
    Assert.assertTrue(recordsMap.containsKey(_keys.get(0))); // a, b, (c (30, 300))
    Assert.assertTrue(recordsMap.containsKey(_keys.get(1)));
    Assert.assertTrue(recordsMap.containsKey(_keys.get(2)));

    // object type avg(m4) asc
    sel1.setColumn("avg(m4)");
    sel1.setIsAsc(true);
    selectionSort = Lists.newArrayList(sel1);
    _tableResizer = new TableResizer(dataSchema, aggregationInfos, selectionSort);
    recordsMap = new HashMap<>(_recordsMap);
    _tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(recordsMap.size(), trimToSize);
    Assert.assertTrue(recordsMap.containsKey(_keys.get(4))); // 2, 3, 3.33,
    Assert.assertTrue(recordsMap.containsKey(_keys.get(3)));
    Assert.assertTrue(recordsMap.containsKey(_keys.get(1)));

    // non-comparable intermediate result
    sel1.setColumn("distinctcount(m3)");
    sel1.setIsAsc(false);
    sel2.setColumn("d1");
    sel2.setIsAsc(true);
    selectionSort = Lists.newArrayList(sel1, sel2);
    _tableResizer = new TableResizer(dataSchema, aggregationInfos, selectionSort);
    recordsMap = new HashMap<>(_recordsMap);
    _tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(recordsMap.size(), trimToSize);
    Assert.assertTrue(recordsMap.containsKey(_keys.get(4))); // 6, 5, 4 (b)
    Assert.assertTrue(recordsMap.containsKey(_keys.get(3)));
    Assert.assertTrue(recordsMap.containsKey(_keys.get(1)));

    // Test resize algorithm with numRecordsToEvict > trimToSize.
    // TotalRecords=5; trimToSize=2; numRecordsToEvict=3
    trimToSize = 2;

    // d1 asc
    sel1.setColumn("d1");
    sel1.setIsAsc(true);
    selectionSort = Lists.newArrayList(sel1);
    _tableResizer = new TableResizer(dataSchema, aggregationInfos, selectionSort);
    recordsMap = new HashMap<>(_recordsMap);
    _tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(recordsMap.size(), trimToSize);
    Assert.assertTrue(recordsMap.containsKey(_keys.get(0))); // a, b
    Assert.assertTrue(recordsMap.containsKey(_keys.get(1)));

    // object type avg(m4) asc
    sel1.setColumn("avg(m4)");
    sel1.setIsAsc(true);
    selectionSort = Lists.newArrayList(sel1);
    _tableResizer = new TableResizer(dataSchema, aggregationInfos, selectionSort);
    recordsMap = new HashMap<>(_recordsMap);
    _tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(recordsMap.size(), trimToSize);
    Assert.assertTrue(recordsMap.containsKey(_keys.get(4))); // 2, 3, 3.33,
    Assert.assertTrue(recordsMap.containsKey(_keys.get(3)));

    // non-comparable intermediate result
    sel1.setColumn("distinctcount(m3)");
    sel1.setIsAsc(false);
    sel2.setColumn("d1");
    sel2.setIsAsc(true);
    selectionSort = Lists.newArrayList(sel1, sel2);
    _tableResizer = new TableResizer(dataSchema, aggregationInfos, selectionSort);
    recordsMap = new HashMap<>(_recordsMap);
    _tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(recordsMap.size(), trimToSize);
    Assert.assertTrue(recordsMap.containsKey(_keys.get(4))); // 6, 5, 4 (b)
    Assert.assertTrue(recordsMap.containsKey(_keys.get(3)));

    // Reset trimToSize
    trimToSize = 3;
  }

  /**
   * Tests the sort function for ordered resizer
   */
  @Test
  public void testResizeAndSortRecordsMap() {
    List<Record> sortedRecords;
    int[] order;
    Map<Key, Record> recordsMap;

    // d1 asc
    sel1.setColumn("d1");
    sel1.setIsAsc(true);
    selectionSort = Lists.newArrayList(sel1);
    _tableResizer = new TableResizer(dataSchema, aggregationInfos, selectionSort);
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = _tableResizer.resizeAndSortRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(sortedRecords.size(), trimToSize);
    order = new int[]{0, 1};
    for (int i = 0; i < order.length; i++) {
      Assert.assertEquals(sortedRecords.get(i), _records.get(order[i]));
    }

    // d1 asc - trim to 1
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = _tableResizer.resizeAndSortRecordsMap(recordsMap, 1);
    Assert.assertEquals(sortedRecords.size(), 1);
    order = new int[]{0};
    for (int i = 0; i < order.length; i++) {
      Assert.assertEquals(sortedRecords.get(i), _records.get(order[i]));
    }

    // d1 asc, d3 desc (tie breaking with 2nd comparator)
    sel1.setColumn("d1");
    sel1.setIsAsc(true);
    sel2.setColumn("d3");
    sel2.setIsAsc(false);
    selectionSort = Lists.newArrayList(sel1, sel2);
    _tableResizer = new TableResizer(dataSchema, aggregationInfos, selectionSort);
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = _tableResizer.resizeAndSortRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(sortedRecords.size(), trimToSize);
    order = new int[]{0, 1, 4};
    for (int i = 0; i < order.length; i++) {
      Assert.assertEquals(sortedRecords.get(i), _records.get(order[i]));
    }

    // d1 asc, d3 desc (tie breaking with 2nd comparator) - trim 1
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = _tableResizer.resizeAndSortRecordsMap(recordsMap, 1);
    Assert.assertEquals(sortedRecords.size(), 1);
    order = new int[]{0};
    for (int i = 0; i < order.length; i++) {
      Assert.assertEquals(sortedRecords.get(i), _records.get(order[i]));
    }

    // d1 asc, sum(m1) desc, max(m2) desc
    sel1.setColumn("d1");
    sel1.setIsAsc(true);
    sel2.setColumn("sum(m1)");
    sel2.setIsAsc(false);
    sel3.setColumn("max(m2)");
    sel3.setIsAsc(false);
    selectionSort = Lists.newArrayList(sel1, sel2, sel3);
    _tableResizer = new TableResizer(dataSchema, aggregationInfos, selectionSort);
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = _tableResizer.resizeAndSortRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(sortedRecords.size(), trimToSize);
    order = new int[]{0, 1, 2};
    for (int i = 0; i < order.length; i++) {
      Assert.assertEquals(sortedRecords.get(i), _records.get(order[i]));
    }

    // trim 1
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = _tableResizer.resizeAndSortRecordsMap(recordsMap, 1);
    Assert.assertEquals(sortedRecords.size(), 1);
    order = new int[]{0};
    for (int i = 0; i < order.length; i++) {
      Assert.assertEquals(sortedRecords.get(i), _records.get(order[i]));
    }

    // object type avg(m4) asc
    sel1.setColumn("avg(m4)");
    sel1.setIsAsc(true);
    sel2.setColumn("d1");
    sel2.setIsAsc(true);
    selectionSort = Lists.newArrayList(sel1, sel2);
    _tableResizer = new TableResizer(dataSchema, aggregationInfos, selectionSort);
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = _tableResizer.resizeAndSortRecordsMap(recordsMap, 10); // high trim to size
    Assert.assertEquals(sortedRecords.size(), recordsMap.size());
    order = new int[]{4, 3, 1, 0, 2};
    for (int i = 0; i < order.length; i++) {
      Assert.assertEquals(sortedRecords.get(i), _records.get(order[i]));
    }

    // non-comparable intermediate result
    sel1.setColumn("distinctcount(m3)");
    sel1.setIsAsc(false);
    sel2.setColumn("avg(m4)");
    sel2.setIsAsc(false);
    selectionSort = Lists.newArrayList(sel1, sel2);
    _tableResizer = new TableResizer(dataSchema, aggregationInfos, selectionSort);
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = _tableResizer.resizeAndSortRecordsMap(recordsMap, recordsMap.size()); // equal trim to size
    Assert.assertEquals(sortedRecords.size(), recordsMap.size());
    order = new int[]{4, 3, 2, 1, 0};
    for (int i = 0; i < order.length; i++) {
      Assert.assertEquals(sortedRecords.get(i), _records.get(order[i]));
    }
  }

  /**
   * Tests the conversion of {@link Record} to {@link TableResizer.IntermediateRecord}
   */
  @Test
  public void testIntermediateRecord() {

    // d2
    sel1.setColumn("d2");
    selectionSort = Lists.newArrayList(sel1);
    _tableResizer = new TableResizer(dataSchema, aggregationInfos, selectionSort);
    for (Map.Entry<Key, Record> entry : _recordsMap.entrySet()) {
      Key key = entry.getKey();
      Record record = entry.getValue();
      TableResizer.IntermediateRecord intermediateRecord =
          _tableResizer.getIntermediateRecord(key, record);
      Assert.assertEquals(intermediateRecord._key, key);
      Assert.assertEquals(intermediateRecord._values.length, 1);
      Assert.assertEquals(intermediateRecord._values[0], record.getValues()[1]);
    }

    // sum(m1)
    sel1.setColumn("sum(m1)");
    selectionSort = Lists.newArrayList(sel1);
    _tableResizer = new TableResizer(dataSchema, aggregationInfos, selectionSort);
    for (Map.Entry<Key, Record> entry : _recordsMap.entrySet()) {
      Key key = entry.getKey();
      Record record = entry.getValue();
      TableResizer.IntermediateRecord intermediateRecord =
          _tableResizer.getIntermediateRecord(key, record);
      Assert.assertEquals(intermediateRecord._key, key);
      Assert.assertEquals(intermediateRecord._values.length, 1);
      Assert.assertEquals(intermediateRecord._values[0], record.getValues()[3]);
    }

    // d1, max(m2)
    sel1.setColumn("d1");
    sel2.setColumn("max(m2)");
    selectionSort = Lists.newArrayList(sel1, sel2);
    _tableResizer = new TableResizer(dataSchema, aggregationInfos, selectionSort);
    for (Map.Entry<Key, Record> entry : _recordsMap.entrySet()) {
      Key key = entry.getKey();
      Record record = entry.getValue();
      TableResizer.IntermediateRecord intermediateRecord =
          _tableResizer.getIntermediateRecord(key, record);
      Assert.assertEquals(intermediateRecord._key, key);
      Assert.assertEquals(intermediateRecord._values.length, 2);
      Assert.assertEquals(intermediateRecord._values[0], record.getValues()[0]);
      Assert.assertEquals(intermediateRecord._values[1], record.getValues()[4]);
    }

    // d2, sum(m1), d3
    sel1.setColumn("d2");
    sel2.setColumn("sum(m1)");
    sel3.setColumn("d3");
    selectionSort = Lists.newArrayList(sel1, sel2, sel3);
    _tableResizer = new TableResizer(dataSchema, aggregationInfos, selectionSort);
    for (Map.Entry<Key, Record> entry : _recordsMap.entrySet()) {
      Key key = entry.getKey();
      Record record = entry.getValue();
      TableResizer.IntermediateRecord intermediateRecord =
          _tableResizer.getIntermediateRecord(key, record);
      Assert.assertEquals(intermediateRecord._key, key);
      Assert.assertEquals(intermediateRecord._values.length, 3);
      Assert.assertEquals(intermediateRecord._values[0], record.getValues()[1]);
      Assert.assertEquals(intermediateRecord._values[1], record.getValues()[3]);
      Assert.assertEquals(intermediateRecord._values[2], record.getValues()[2]);
    }

    // non-comparable intermediate result
    sel1.setColumn("distinctcount(m3)");
    selectionSort = Lists.newArrayList(sel1);
    _tableResizer = new TableResizer(dataSchema, aggregationInfos, selectionSort);
    AggregationFunction distinctCountFunction =
        AggregationFunctionUtils.getAggregationFunctionContext(aggregationInfos.get(2)).getAggregationFunction();
    for (Map.Entry<Key, Record> entry : _recordsMap.entrySet()) {
      Key key = entry.getKey();
      Record record = entry.getValue();
      TableResizer.IntermediateRecord intermediateRecord =
          _tableResizer.getIntermediateRecord(key, record);
      Assert.assertEquals(intermediateRecord._key, key);
      Assert.assertEquals(intermediateRecord._values.length, 1);
      Assert.assertEquals(intermediateRecord._values[0],
          distinctCountFunction.extractFinalResult(record.getValues()[5]));
    }
  }

  @Test
  public void testResizerForSetBasedTable() {
    String[] columns = new String[]{"STRING_COL"};
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING};

    DataSchema schema = new DataSchema(columns, columnDataTypes);
    SelectionSort selectionSort = new SelectionSort();
    selectionSort.setColumn("STRING_COL");
    selectionSort.setIsAsc(true);

    TableResizer tableResizer = new TableResizer(schema, Collections.emptyList(), Lists.newArrayList(selectionSort));
    Set<Record> uniqueRecordsSet = new HashSet<>();

    Record r1 = new Record(new Object[]{"B"});
    Record r2 = new Record(new Object[]{"A"});
    Record r3 = new Record(new Object[]{"D"});
    Record r4 = new Record(new Object[]{"C"});
    Record r5 = new Record(new Object[]{"E"});

    uniqueRecordsSet.add(r1);
    uniqueRecordsSet.add(r2);
    uniqueRecordsSet.add(r3);
    uniqueRecordsSet.add(r4);
    uniqueRecordsSet.add(r5);

    int trimSize = 5;
    // no records should have been evicted
    Set<Record> copiedRecordSet = new HashSet<>(uniqueRecordsSet);
    tableResizer.resizeRecordsSet(copiedRecordSet, trimSize);
    Assert.assertTrue(copiedRecordSet.contains(r1));
    Assert.assertTrue(copiedRecordSet.contains(r2));
    Assert.assertTrue(copiedRecordSet.contains(r3));
    Assert.assertTrue(copiedRecordSet.contains(r4));
    Assert.assertTrue(copiedRecordSet.contains(r5));

    List<Record> sorted = tableResizer.resizeAndSortRecordSet(copiedRecordSet, trimSize);
    Assert.assertEquals(trimSize, sorted.size());
    Assert.assertEquals(r2, sorted.get(0));
    Assert.assertEquals(r1, sorted.get(1));
    Assert.assertEquals(r4, sorted.get(2));
    Assert.assertEquals(r3, sorted.get(3));
    Assert.assertEquals(r5, sorted.get(4));

    trimSize = 3;
    // D and E should have been evicted through PQ
    copiedRecordSet = new HashSet<>(uniqueRecordsSet);
    tableResizer.resizeRecordsSet(copiedRecordSet, trimSize);
    Assert.assertTrue(copiedRecordSet.contains(r1));
    Assert.assertTrue(copiedRecordSet.contains(r2));
    Assert.assertTrue(copiedRecordSet.contains(r4));
    Assert.assertFalse(copiedRecordSet.contains(r3));
    Assert.assertFalse(copiedRecordSet.contains(r5));

    sorted = tableResizer.resizeAndSortRecordSet(copiedRecordSet, trimSize);
    Assert.assertEquals(trimSize, sorted.size());
    Assert.assertEquals(r2, sorted.get(0));
    Assert.assertEquals(r1, sorted.get(1));
    Assert.assertEquals(r4, sorted.get(2));

    trimSize = 2;
    // A and B should have been retained through PQ
    copiedRecordSet = new HashSet<>(uniqueRecordsSet);
    tableResizer.resizeRecordsSet(copiedRecordSet, trimSize);
    Assert.assertTrue(copiedRecordSet.contains(r1));
    Assert.assertTrue(copiedRecordSet.contains(r2));
    Assert.assertFalse(copiedRecordSet.contains(r3));
    Assert.assertFalse(copiedRecordSet.contains(r4));
    Assert.assertFalse(copiedRecordSet.contains(r5));

    sorted = tableResizer.resizeAndSortRecordSet(copiedRecordSet, trimSize);
    Assert.assertEquals(2, sorted.size());
    Assert.assertEquals(r2, sorted.get(0));
    Assert.assertEquals(r1, sorted.get(1));

    trimSize = 1;
    // A should have been retained through PQ
    copiedRecordSet = new HashSet<>(uniqueRecordsSet);
    tableResizer.resizeRecordsSet(copiedRecordSet, trimSize);
    Assert.assertFalse(copiedRecordSet.contains(r1));
    Assert.assertTrue(copiedRecordSet.contains(r2));
    Assert.assertFalse(copiedRecordSet.contains(r3));
    Assert.assertFalse(copiedRecordSet.contains(r4));
    Assert.assertFalse(copiedRecordSet.contains(r5));

    sorted = tableResizer.resizeAndSortRecordSet(copiedRecordSet, trimSize);
    Assert.assertEquals(1, sorted.size());
    Assert.assertEquals(r2, sorted.get(0));

    // change the order to DESC
    selectionSort.setIsAsc(false);
    tableResizer = new TableResizer(schema, Collections.emptyList(), Lists.newArrayList(selectionSort));

    trimSize = 5;
    // no records should have been evicted
    copiedRecordSet = new HashSet<>(uniqueRecordsSet);
    tableResizer.resizeRecordsSet(copiedRecordSet, trimSize);
    Assert.assertTrue(copiedRecordSet.contains(r1));
    Assert.assertTrue(copiedRecordSet.contains(r2));
    Assert.assertTrue(copiedRecordSet.contains(r3));
    Assert.assertTrue(copiedRecordSet.contains(r4));
    Assert.assertTrue(copiedRecordSet.contains(r5));

    sorted = tableResizer.resizeAndSortRecordSet(copiedRecordSet, trimSize);
    Assert.assertEquals(trimSize, sorted.size());
    Assert.assertEquals(r5, sorted.get(0));
    Assert.assertEquals(r3, sorted.get(1));
    Assert.assertEquals(r4, sorted.get(2));
    Assert.assertEquals(r1, sorted.get(3));
    Assert.assertEquals(r2, sorted.get(4));

    trimSize = 3;
    // A and B should have been evicted through PQ
    copiedRecordSet = new HashSet<>(uniqueRecordsSet);
    tableResizer.resizeRecordsSet(copiedRecordSet, trimSize);
    Assert.assertFalse(copiedRecordSet.contains(r1));
    Assert.assertFalse(copiedRecordSet.contains(r2));
    Assert.assertTrue(copiedRecordSet.contains(r4));
    Assert.assertTrue(copiedRecordSet.contains(r3));
    Assert.assertTrue(copiedRecordSet.contains(r5));

    sorted = tableResizer.resizeAndSortRecordSet(copiedRecordSet, trimSize);
    Assert.assertEquals(3, sorted.size());
    Assert.assertEquals(r5, sorted.get(0));
    Assert.assertEquals(r3, sorted.get(1));
    Assert.assertEquals(r4, sorted.get(2));

    trimSize = 2;
    // D and E should have been retained through PQ
    copiedRecordSet = new HashSet<>(uniqueRecordsSet);
    tableResizer.resizeRecordsSet(copiedRecordSet, trimSize);
    Assert.assertFalse(copiedRecordSet.contains(r1));
    Assert.assertFalse(copiedRecordSet.contains(r2));
    Assert.assertFalse(copiedRecordSet.contains(r4));
    Assert.assertTrue(copiedRecordSet.contains(r3));
    Assert.assertTrue(copiedRecordSet.contains(r5));

    sorted = tableResizer.resizeAndSortRecordSet(copiedRecordSet, trimSize);
    Assert.assertEquals(2, sorted.size());
    Assert.assertEquals(r5, sorted.get(0));
    Assert.assertEquals(r3, sorted.get(1));

    trimSize = 1;
    // E should have been retained through PQ
    copiedRecordSet = new HashSet<>(uniqueRecordsSet);
    tableResizer.resizeRecordsSet(copiedRecordSet, trimSize);
    Assert.assertFalse(copiedRecordSet.contains(r1));
    Assert.assertFalse(copiedRecordSet.contains(r2));
    Assert.assertFalse(copiedRecordSet.contains(r4));
    Assert.assertFalse(copiedRecordSet.contains(r3));
    Assert.assertTrue(copiedRecordSet.contains(r5));

    sorted = tableResizer.resizeAndSortRecordSet(copiedRecordSet, trimSize);
    Assert.assertEquals(1, sorted.size());
    Assert.assertEquals(r5, sorted.get(0));
  }
}
