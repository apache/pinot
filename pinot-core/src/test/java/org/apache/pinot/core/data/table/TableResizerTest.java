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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AvgAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.DistinctCountAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.MaxAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.SumAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.customobject.AvgPair;
import org.apache.pinot.core.query.request.context.OrderByExpressionContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests the functionality of {@link @TableResizer}
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class TableResizerTest {
  private DataSchema _dataSchema;
  private AggregationFunction[] _aggregationFunctions;

  private int trimToSize = 3;
  private Map<Key, Record> _recordsMap;
  private List<Record> _records;
  private List<Key> _keys;

  @BeforeClass
  public void setUp() {
    _dataSchema = new DataSchema(new String[]{"d1", "d2", "d3", "sum(m1)", "max(m2)", "distinctcount(m3)", "avg(m4)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.OBJECT, DataSchema.ColumnDataType.OBJECT});
    _aggregationFunctions = new AggregationFunction[]{new SumAggregationFunction("m1"), new MaxAggregationFunction(
        "m2"), new DistinctCountAggregationFunction("m3"), new AvgAggregationFunction("m4")};

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
    recordsMap = new HashMap<>(_recordsMap);
    TableResizer tableResizer = new TableResizer(_dataSchema, _aggregationFunctions,
        Collections.singletonList(new OrderByExpressionContext(QueryContextConverterUtils.getExpression("d1"), true)));
    tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(recordsMap.size(), trimToSize);
    Assert.assertTrue(recordsMap.containsKey(_keys.get(0))); // a, b, c
    Assert.assertTrue(recordsMap.containsKey(_keys.get(1)));

    // d1 desc
    tableResizer = new TableResizer(_dataSchema, _aggregationFunctions,
        Collections.singletonList(new OrderByExpressionContext(QueryContextConverterUtils.getExpression("d1"), false)));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(recordsMap.size(), trimToSize);
    Assert.assertTrue(recordsMap.containsKey(_keys.get(2))); // c, c, c
    Assert.assertTrue(recordsMap.containsKey(_keys.get(3)));
    Assert.assertTrue(recordsMap.containsKey(_keys.get(4)));

    // d1 asc, d3 desc (tie breaking with 2nd comparator)
    tableResizer = new TableResizer(_dataSchema, _aggregationFunctions, Arrays
        .asList(new OrderByExpressionContext(QueryContextConverterUtils.getExpression("d1"), true),
            new OrderByExpressionContext(QueryContextConverterUtils.getExpression("d3"), false)));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(recordsMap.size(), trimToSize);
    Assert.assertTrue(recordsMap.containsKey(_keys.get(0))); // 10, 10, 300
    Assert.assertTrue(recordsMap.containsKey(_keys.get(1)));
    Assert.assertTrue(recordsMap.containsKey(_keys.get(4)));

    // d2 asc
    tableResizer = new TableResizer(_dataSchema, _aggregationFunctions,
        Collections.singletonList(new OrderByExpressionContext(QueryContextConverterUtils.getExpression("d2"), true)));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(recordsMap.size(), trimToSize);
    Assert.assertTrue(recordsMap.containsKey(_keys.get(0))); // 10, 10, 50
    Assert.assertTrue(recordsMap.containsKey(_keys.get(1)));
    Assert.assertTrue(recordsMap.containsKey(_keys.get(3)));

    // d1 asc, sum(m1) desc, max(m2) desc
    tableResizer = new TableResizer(_dataSchema, _aggregationFunctions, Arrays
        .asList(new OrderByExpressionContext(QueryContextConverterUtils.getExpression("d1"), true),
            new OrderByExpressionContext(QueryContextConverterUtils.getExpression("sum(m1)"), false),
            new OrderByExpressionContext(QueryContextConverterUtils.getExpression("max(m2)"), false)));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(recordsMap.size(), trimToSize);
    Assert.assertTrue(recordsMap.containsKey(_keys.get(0))); // a, b, (c (30, 300))
    Assert.assertTrue(recordsMap.containsKey(_keys.get(1)));
    Assert.assertTrue(recordsMap.containsKey(_keys.get(2)));

    // object type avg(m4) asc
    tableResizer = new TableResizer(_dataSchema, _aggregationFunctions, Collections
        .singletonList(new OrderByExpressionContext(QueryContextConverterUtils.getExpression("avg(m4)"), true)));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(recordsMap.size(), trimToSize);
    Assert.assertTrue(recordsMap.containsKey(_keys.get(4))); // 2, 3, 3.33,
    Assert.assertTrue(recordsMap.containsKey(_keys.get(3)));
    Assert.assertTrue(recordsMap.containsKey(_keys.get(1)));

    // non-comparable intermediate result
    tableResizer = new TableResizer(_dataSchema, _aggregationFunctions, Arrays
        .asList(new OrderByExpressionContext(QueryContextConverterUtils.getExpression("distinctcount(m3)"), false),
            new OrderByExpressionContext(QueryContextConverterUtils.getExpression("d1"), true)));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(recordsMap.size(), trimToSize);
    Assert.assertTrue(recordsMap.containsKey(_keys.get(4))); // 6, 5, 4 (b)
    Assert.assertTrue(recordsMap.containsKey(_keys.get(3)));
    Assert.assertTrue(recordsMap.containsKey(_keys.get(1)));

    // Test resize algorithm with numRecordsToEvict > trimToSize.
    // TotalRecords=5; trimToSize=2; numRecordsToEvict=3
    trimToSize = 2;

    // d1 asc
    tableResizer = new TableResizer(_dataSchema, _aggregationFunctions,
        Collections.singletonList(new OrderByExpressionContext(QueryContextConverterUtils.getExpression("d1"), true)));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(recordsMap.size(), trimToSize);
    Assert.assertTrue(recordsMap.containsKey(_keys.get(0))); // a, b
    Assert.assertTrue(recordsMap.containsKey(_keys.get(1)));

    // object type avg(m4) asc
    tableResizer = new TableResizer(_dataSchema, _aggregationFunctions, Collections
        .singletonList(new OrderByExpressionContext(QueryContextConverterUtils.getExpression("avg(m4)"), true)));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(recordsMap.size(), trimToSize);
    Assert.assertTrue(recordsMap.containsKey(_keys.get(4))); // 2, 3, 3.33,
    Assert.assertTrue(recordsMap.containsKey(_keys.get(3)));

    // non-comparable intermediate result
    tableResizer = new TableResizer(_dataSchema, _aggregationFunctions, Arrays
        .asList(new OrderByExpressionContext(QueryContextConverterUtils.getExpression("distinctcount(m3)"), false),
            new OrderByExpressionContext(QueryContextConverterUtils.getExpression("d1"), true)));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, trimToSize);
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
    TableResizer tableResizer = new TableResizer(_dataSchema, _aggregationFunctions,
        Collections.singletonList(new OrderByExpressionContext(QueryContextConverterUtils.getExpression("d1"), true)));
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.resizeAndSortRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(sortedRecords.size(), trimToSize);
    order = new int[]{0, 1};
    for (int i = 0; i < order.length; i++) {
      Assert.assertEquals(sortedRecords.get(i), _records.get(order[i]));
    }

    // d1 asc - trim to 1
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.resizeAndSortRecordsMap(recordsMap, 1);
    Assert.assertEquals(sortedRecords.size(), 1);
    order = new int[]{0};
    for (int i = 0; i < order.length; i++) {
      Assert.assertEquals(sortedRecords.get(i), _records.get(order[i]));
    }

    // d1 asc, d3 desc (tie breaking with 2nd comparator)
    tableResizer = new TableResizer(_dataSchema, _aggregationFunctions, Arrays
        .asList(new OrderByExpressionContext(QueryContextConverterUtils.getExpression("d1"), true),
            new OrderByExpressionContext(QueryContextConverterUtils.getExpression("d3"), false)));
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.resizeAndSortRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(sortedRecords.size(), trimToSize);
    order = new int[]{0, 1, 4};
    for (int i = 0; i < order.length; i++) {
      Assert.assertEquals(sortedRecords.get(i), _records.get(order[i]));
    }

    // d1 asc, d3 desc (tie breaking with 2nd comparator) - trim 1
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.resizeAndSortRecordsMap(recordsMap, 1);
    Assert.assertEquals(sortedRecords.size(), 1);
    order = new int[]{0};
    for (int i = 0; i < order.length; i++) {
      Assert.assertEquals(sortedRecords.get(i), _records.get(order[i]));
    }

    // d1 asc, sum(m1) desc, max(m2) desc
    tableResizer = new TableResizer(_dataSchema, _aggregationFunctions, Arrays
        .asList(new OrderByExpressionContext(QueryContextConverterUtils.getExpression("d1"), true),
            new OrderByExpressionContext(QueryContextConverterUtils.getExpression("sum(m1)"), false),
            new OrderByExpressionContext(QueryContextConverterUtils.getExpression("max(m2)"), false)));
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.resizeAndSortRecordsMap(recordsMap, trimToSize);
    Assert.assertEquals(sortedRecords.size(), trimToSize);
    order = new int[]{0, 1, 2};
    for (int i = 0; i < order.length; i++) {
      Assert.assertEquals(sortedRecords.get(i), _records.get(order[i]));
    }

    // trim 1
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.resizeAndSortRecordsMap(recordsMap, 1);
    Assert.assertEquals(sortedRecords.size(), 1);
    order = new int[]{0};
    for (int i = 0; i < order.length; i++) {
      Assert.assertEquals(sortedRecords.get(i), _records.get(order[i]));
    }

    // object type avg(m4) asc
    tableResizer = new TableResizer(_dataSchema, _aggregationFunctions, Arrays
        .asList(new OrderByExpressionContext(QueryContextConverterUtils.getExpression("avg(m4)"), true),
            new OrderByExpressionContext(QueryContextConverterUtils.getExpression("d1"), true)));
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.resizeAndSortRecordsMap(recordsMap, 10); // high trim to size
    Assert.assertEquals(sortedRecords.size(), recordsMap.size());
    order = new int[]{4, 3, 1, 0, 2};
    for (int i = 0; i < order.length; i++) {
      Assert.assertEquals(sortedRecords.get(i), _records.get(order[i]));
    }

    // non-comparable intermediate result
    tableResizer = new TableResizer(_dataSchema, _aggregationFunctions, Arrays
        .asList(new OrderByExpressionContext(QueryContextConverterUtils.getExpression("distinctcount(m3)"), false),
            new OrderByExpressionContext(QueryContextConverterUtils.getExpression("avg(m4)"), false)));
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.resizeAndSortRecordsMap(recordsMap, recordsMap.size()); // equal trim to size
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
    TableResizer tableResizer = new TableResizer(_dataSchema, _aggregationFunctions,
        Collections.singletonList(new OrderByExpressionContext(QueryContextConverterUtils.getExpression("d2"), true)));
    for (Map.Entry<Key, Record> entry : _recordsMap.entrySet()) {
      Key key = entry.getKey();
      Record record = entry.getValue();
      TableResizer.IntermediateRecord intermediateRecord = tableResizer.getIntermediateRecord(key, record);
      Assert.assertEquals(intermediateRecord._key, key);
      Assert.assertEquals(intermediateRecord._values.length, 1);
      Assert.assertEquals(intermediateRecord._values[0], record.getValues()[1]);
    }

    // sum(m1)
    tableResizer = new TableResizer(_dataSchema, _aggregationFunctions, Collections
        .singletonList(new OrderByExpressionContext(QueryContextConverterUtils.getExpression("sum(m1)"), true)));
    for (Map.Entry<Key, Record> entry : _recordsMap.entrySet()) {
      Key key = entry.getKey();
      Record record = entry.getValue();
      TableResizer.IntermediateRecord intermediateRecord = tableResizer.getIntermediateRecord(key, record);
      Assert.assertEquals(intermediateRecord._key, key);
      Assert.assertEquals(intermediateRecord._values.length, 1);
      Assert.assertEquals(intermediateRecord._values[0], record.getValues()[3]);
    }

    // d1, max(m2)
    tableResizer = new TableResizer(_dataSchema, _aggregationFunctions, Arrays
        .asList(new OrderByExpressionContext(QueryContextConverterUtils.getExpression("d1"), true),
            new OrderByExpressionContext(QueryContextConverterUtils.getExpression("max(m2)"), true)));
    for (Map.Entry<Key, Record> entry : _recordsMap.entrySet()) {
      Key key = entry.getKey();
      Record record = entry.getValue();
      TableResizer.IntermediateRecord intermediateRecord = tableResizer.getIntermediateRecord(key, record);
      Assert.assertEquals(intermediateRecord._key, key);
      Assert.assertEquals(intermediateRecord._values.length, 2);
      Assert.assertEquals(intermediateRecord._values[0], record.getValues()[0]);
      Assert.assertEquals(intermediateRecord._values[1], record.getValues()[4]);
    }

    // d2, sum(m1), d3
    tableResizer = new TableResizer(_dataSchema, _aggregationFunctions, Arrays
        .asList(new OrderByExpressionContext(QueryContextConverterUtils.getExpression("d2"), true),
            new OrderByExpressionContext(QueryContextConverterUtils.getExpression("sum(m1)"), true),
            new OrderByExpressionContext(QueryContextConverterUtils.getExpression("d3"), true)));
    for (Map.Entry<Key, Record> entry : _recordsMap.entrySet()) {
      Key key = entry.getKey();
      Record record = entry.getValue();
      TableResizer.IntermediateRecord intermediateRecord = tableResizer.getIntermediateRecord(key, record);
      Assert.assertEquals(intermediateRecord._key, key);
      Assert.assertEquals(intermediateRecord._values.length, 3);
      Assert.assertEquals(intermediateRecord._values[0], record.getValues()[1]);
      Assert.assertEquals(intermediateRecord._values[1], record.getValues()[3]);
      Assert.assertEquals(intermediateRecord._values[2], record.getValues()[2]);
    }

    // non-comparable intermediate result
    tableResizer = new TableResizer(_dataSchema, _aggregationFunctions, Collections.singletonList(
        new OrderByExpressionContext(QueryContextConverterUtils.getExpression("distinctcount(m3)"), true)));
    AggregationFunction distinctCountFunction = _aggregationFunctions[2];
    for (Map.Entry<Key, Record> entry : _recordsMap.entrySet()) {
      Key key = entry.getKey();
      Record record = entry.getValue();
      TableResizer.IntermediateRecord intermediateRecord = tableResizer.getIntermediateRecord(key, record);
      Assert.assertEquals(intermediateRecord._key, key);
      Assert.assertEquals(intermediateRecord._values.length, 1);
      Assert
          .assertEquals(intermediateRecord._values[0], distinctCountFunction.extractFinalResult(record.getValues()[5]));
    }
  }
}
