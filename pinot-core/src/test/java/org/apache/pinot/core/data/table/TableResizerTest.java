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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.customobject.AvgPair;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests the functionality of {@link @TableResizer}
 */
public class TableResizerTest {
  private static final String QUERY_PREFIX =
      "SELECT SUM(m1), MAX(m2), DISTINCTCOUNT(m3), AVG(m4) FROM testTable GROUP BY d1, d2, d3 ORDER BY ";
  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"d1", "d2", "d3", "sum(m1)", "max(m2)", "distinctcount(m3)", "avg(m4)"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.OBJECT, DataSchema.ColumnDataType.OBJECT});
  private static final int TRIM_TO_SIZE = 3;

  private Map<Key, Record> _recordsMap;
  private List<Record> _records;
  private List<Key> _keys;

  @BeforeClass
  public void setUp() {
    //@formatter:off
    _records = Arrays.asList(
        new Record(new Object[]{"a", 10, 1.0, 10.0, 100.0, new IntOpenHashSet(new int[]{1}), new AvgPair(10, 2) /* 5 */}),
        new Record(new Object[]{"b", 10, 2.0, 20.0, 200.0, new IntOpenHashSet(new int[]{1, 2}), new AvgPair(10, 3) /* 3.33 */}),
        new Record(new Object[]{"c", 200, 3.0, 30.0, 300.0, new IntOpenHashSet(new int[]{1, 2}), new AvgPair(20, 4) /* 5 */}),
        new Record(new Object[]{"c", 50, 4.0, 30.0, 200.0, new IntOpenHashSet(new int[]{1, 2, 3}), new AvgPair(30, 10) /* 3 */}),
        new Record(new Object[]{"c", 300, 5.0, 20.0, 100.0, new IntOpenHashSet(new int[]{1, 2, 3, 4}), new AvgPair(10, 5) /* 2 */})
    );
    _keys = Arrays.asList(
        new Key(new Object[]{"a", 10, 1.0}),
        new Key(new Object[]{"b", 10, 2.0}),
        new Key(new Object[]{"c", 200, 3.0}),
        new Key(new Object[]{"c", 50, 4.0}),
        new Key(new Object[]{"c", 300, 5.0})
    );
    //@formatter:on
    _recordsMap = new HashMap<>();
    int numRecords = _records.size();
    for (int i = 0; i < numRecords; i++) {
      _recordsMap.put(_keys.get(i), _records.get(i));
    }
  }

  @Test
  public void testResizeRecordsMap() {
    // Test resize algorithm with numRecordsToEvict < trimToSize.
    // TotalRecords=5; trimToSize=3; numRecordsToEvict=2

    // d1 asc
    TableResizer tableResizer =
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContextFromSQL(QUERY_PREFIX + "d1"));
    Map<Key, Record> recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(recordsMap.size(), TRIM_TO_SIZE);
    assertTrue(recordsMap.containsKey(_keys.get(0))); // a, b
    assertTrue(recordsMap.containsKey(_keys.get(1)));

    // d1 desc
    tableResizer =
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContextFromSQL(QUERY_PREFIX + "d1 DESC"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(recordsMap.size(), TRIM_TO_SIZE);
    assertTrue(recordsMap.containsKey(_keys.get(2))); // c, c, c
    assertTrue(recordsMap.containsKey(_keys.get(3)));
    assertTrue(recordsMap.containsKey(_keys.get(4)));

    // d1 asc, d3 desc (tie breaking with 2nd comparator)
    tableResizer =
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContextFromSQL(QUERY_PREFIX + "d1, d3 DESC"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(recordsMap.size(), TRIM_TO_SIZE);
    assertTrue(recordsMap.containsKey(_keys.get(0))); // a, b, c (300)
    assertTrue(recordsMap.containsKey(_keys.get(1)));
    assertTrue(recordsMap.containsKey(_keys.get(4)));

    // d1 asc, sum(m1) desc, max(m2) desc
    tableResizer = new TableResizer(DATA_SCHEMA,
        QueryContextConverterUtils.getQueryContextFromSQL(QUERY_PREFIX + "d1, SUM(m1) DESC, max(m2) DESC"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(recordsMap.size(), TRIM_TO_SIZE);
    assertTrue(recordsMap.containsKey(_keys.get(0))); // a, b, c (30, 300)
    assertTrue(recordsMap.containsKey(_keys.get(1)));
    assertTrue(recordsMap.containsKey(_keys.get(2)));

    // avg(m4) asc (object type)
    tableResizer =
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContextFromSQL(QUERY_PREFIX + "AVG(m4)"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(recordsMap.size(), TRIM_TO_SIZE);
    assertTrue(recordsMap.containsKey(_keys.get(4))); // 2, 3, 3.33
    assertTrue(recordsMap.containsKey(_keys.get(3)));
    assertTrue(recordsMap.containsKey(_keys.get(1)));

    // distinctcount(m3) desc, d1 asc (non-comparable intermediate result)
    tableResizer = new TableResizer(DATA_SCHEMA,
        QueryContextConverterUtils.getQueryContextFromSQL(QUERY_PREFIX + "DISTINCTCOUNT(m3) DESC, d1"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(recordsMap.size(), TRIM_TO_SIZE);
    assertTrue(recordsMap.containsKey(_keys.get(4))); // 4, 3, 2 (b)
    assertTrue(recordsMap.containsKey(_keys.get(3)));
    assertTrue(recordsMap.containsKey(_keys.get(1)));

    // d2 + d3 asc (post-aggregation)
    tableResizer =
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContextFromSQL(QUERY_PREFIX + "d2 + d3"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(recordsMap.size(), TRIM_TO_SIZE);
    assertTrue(recordsMap.containsKey(_keys.get(0))); // 11, 12, 54
    assertTrue(recordsMap.containsKey(_keys.get(1)));
    assertTrue(recordsMap.containsKey(_keys.get(3)));

    // sum(m1) * d3 desc (post-aggregation)
    tableResizer = new TableResizer(DATA_SCHEMA,
        QueryContextConverterUtils.getQueryContextFromSQL(QUERY_PREFIX + "SUM(m1) * d3 DESC"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(recordsMap.size(), TRIM_TO_SIZE);
    assertTrue(recordsMap.containsKey(_keys.get(3))); // 120, 100, 90
    assertTrue(recordsMap.containsKey(_keys.get(4)));
    assertTrue(recordsMap.containsKey(_keys.get(2)));

    // d2 / (distinctcount(m3) + 1) asc, d1 desc (post-aggregation)
    tableResizer = new TableResizer(DATA_SCHEMA,
        QueryContextConverterUtils.getQueryContextFromSQL(QUERY_PREFIX + "d2 / (DISTINCTCOUNT(m3) + 1), d1 DESC"));
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
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContextFromSQL(QUERY_PREFIX + "d1"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    assertEquals(recordsMap.size(), trimToSize);
    assertTrue(recordsMap.containsKey(_keys.get(0))); // a, b
    assertTrue(recordsMap.containsKey(_keys.get(1)));

    // avg(m4) asc (object type)
    tableResizer =
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContextFromSQL(QUERY_PREFIX + "AVG(m4)"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    assertEquals(recordsMap.size(), trimToSize);
    assertTrue(recordsMap.containsKey(_keys.get(4))); // 2, 3
    assertTrue(recordsMap.containsKey(_keys.get(3)));

    // distinctcount(m3) desc, d1 asc (non-comparable intermediate result)
    tableResizer = new TableResizer(DATA_SCHEMA,
        QueryContextConverterUtils.getQueryContextFromSQL(QUERY_PREFIX + "DISTINCTCOUNT(m3) DESC, d1"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    assertEquals(recordsMap.size(), trimToSize);
    assertTrue(recordsMap.containsKey(_keys.get(4))); // 4, 3
    assertTrue(recordsMap.containsKey(_keys.get(3)));

    // d2 / (distinctcount(m3) + 1) asc, d1 desc (post-aggregation)
    tableResizer = new TableResizer(DATA_SCHEMA,
        QueryContextConverterUtils.getQueryContextFromSQL(QUERY_PREFIX + "d2 / (DISTINCTCOUNT(m3) + 1), d1 DESC"));
    recordsMap = new HashMap<>(_recordsMap);
    tableResizer.resizeRecordsMap(recordsMap, trimToSize);
    assertEquals(recordsMap.size(), trimToSize);
    assertTrue(recordsMap.containsKey(_keys.get(1))); // 3.33, 12.5
    assertTrue(recordsMap.containsKey(_keys.get(0)));
  }

  /**
   * Tests the sort function for ordered resizer
   */
  @Test
  public void testResizeAndSortRecordsMap() {
    // d1 asc
    TableResizer tableResizer =
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContextFromSQL(QUERY_PREFIX + "d1"));
    Map<Key, Record> recordsMap = new HashMap<>(_recordsMap);
    List<Record> sortedRecords = tableResizer.resizeAndSortRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(sortedRecords.size(), TRIM_TO_SIZE);
    assertEquals(sortedRecords.get(0), _records.get(0));  // a, b
    assertEquals(sortedRecords.get(1), _records.get(1));

    // d1 asc - trim to 1
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.resizeAndSortRecordsMap(recordsMap, 1);
    assertEquals(sortedRecords.get(0), _records.get(0));  // a

    // d1 asc, d3 desc (tie breaking with 2nd comparator)
    tableResizer =
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContextFromSQL(QUERY_PREFIX + "d1, d3 DESC"));
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.resizeAndSortRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(sortedRecords.size(), TRIM_TO_SIZE);
    assertEquals(sortedRecords.get(0), _records.get(0));  // a, b, c (300)
    assertEquals(sortedRecords.get(1), _records.get(1));
    assertEquals(sortedRecords.get(2), _records.get(4));

    // d1 asc, d3 desc (tie breaking with 2nd comparator) - trim to 1
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.resizeAndSortRecordsMap(recordsMap, 1);
    assertEquals(sortedRecords.size(), 1);
    assertEquals(sortedRecords.get(0), _records.get(0));  // a

    // d1 asc, sum(m1) desc, max(m2) desc
    tableResizer = new TableResizer(DATA_SCHEMA,
        QueryContextConverterUtils.getQueryContextFromSQL(QUERY_PREFIX + "d1, SUM(m1) DESC, max(m2) DESC"));
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.resizeAndSortRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(sortedRecords.size(), TRIM_TO_SIZE);
    assertEquals(sortedRecords.get(0), _records.get(0));  // a, b, c (30, 300)
    assertEquals(sortedRecords.get(1), _records.get(1));
    assertEquals(sortedRecords.get(2), _records.get(2));

    // d1 asc, sum(m1) desc, max(m2) desc - trim to 1
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.resizeAndSortRecordsMap(recordsMap, 1);
    assertEquals(sortedRecords.size(), 1);
    assertEquals(sortedRecords.get(0), _records.get(0));  // a

    // avg(m4) asc (object type)
    tableResizer =
        new TableResizer(DATA_SCHEMA, QueryContextConverterUtils.getQueryContextFromSQL(QUERY_PREFIX + "AVG(m4)"));
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.resizeAndSortRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(sortedRecords.size(), TRIM_TO_SIZE);
    assertEquals(sortedRecords.get(0), _records.get(4));  // 2, 3, 3.33
    assertEquals(sortedRecords.get(1), _records.get(3));
    assertEquals(sortedRecords.get(2), _records.get(1));

    // distinctcount(m3) desc, d1 asc (non-comparable intermediate result)
    tableResizer = new TableResizer(DATA_SCHEMA,
        QueryContextConverterUtils.getQueryContextFromSQL(QUERY_PREFIX + "DISTINCTCOUNT(m3) DESC, d1"));
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.resizeAndSortRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(sortedRecords.size(), TRIM_TO_SIZE);
    assertEquals(sortedRecords.get(0), _records.get(4));  // 4, 3, 2 (b)
    assertEquals(sortedRecords.get(1), _records.get(3));
    assertEquals(sortedRecords.get(2), _records.get(1));

    // d2 / (distinctcount(m3) + 1) asc, d1 desc (post-aggregation)
    tableResizer = new TableResizer(DATA_SCHEMA,
        QueryContextConverterUtils.getQueryContextFromSQL(QUERY_PREFIX + "d2 / (DISTINCTCOUNT(m3) + 1), d1 DESC"));
    recordsMap = new HashMap<>(_recordsMap);
    sortedRecords = tableResizer.resizeAndSortRecordsMap(recordsMap, TRIM_TO_SIZE);
    assertEquals(sortedRecords.size(), TRIM_TO_SIZE);
    assertEquals(sortedRecords.get(0), _records.get(1));  // 3.33, 12.5, 5
    assertEquals(sortedRecords.get(1), _records.get(0));
    assertEquals(sortedRecords.get(2), _records.get(3));
  }
}
