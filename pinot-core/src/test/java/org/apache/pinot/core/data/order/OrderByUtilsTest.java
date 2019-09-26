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
package org.apache.pinot.core.data.order;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OrderByUtilsTest {

  @Test
  public void testComparators() {
    DataSchema dataSchema = new DataSchema(new String[]{"dim0", "dim1", "dim2", "dim3", "metric0"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    List<Record> records = new ArrayList<>();
    records.add(getRecord(new Object[]{"abc", "p125", 10, "30"}, new Object[]{10d}));
    records.add(getRecord(new Object[]{"abc", "p125", 50, "30"}, new Object[]{200d}));
    records.add(getRecord(new Object[]{"abc", "r666", 6, "200"}, new Object[]{200d}));
    records.add(getRecord(new Object[]{"mno", "h776", 10, "100"}, new Object[]{100d}));
    records.add(getRecord(new Object[]{"ghi", "i889", 66, "5"}, new Object[]{50d}));
    records.add(getRecord(new Object[]{"mno", "p125", 10, "30"}, new Object[]{250d}));
    records.add(getRecord(new Object[]{"bcd", "i889", 6, "209"}, new Object[]{100d}));

    List<SelectionSort> orderBy;
    SelectionSort s0;
    SelectionSort s1;
    SelectionSort s2;
    SelectionSort s3;
    SelectionSort s4;
    List<Object> expected0;
    List<Object> expected1;
    List<Object> expected2;
    List<Object> expected3;
    List<Object> expected4;
    List<Object> actual0;
    List<Object> actual1;
    List<Object> actual2;
    List<Object> actual3;
    List<Object> actual4;

    // string column
    s0 = new SelectionSort();
    s0.setColumn("dim0");
    s0.setIsAsc(true);
    orderBy = Lists.newArrayList(s0);
    Comparator<Record> keysComparator = OrderByUtils.getKeysComparator(dataSchema, orderBy);
    records.sort(keysComparator);
    expected0 = Lists.newArrayList("abc", "abc", "abc", "bcd", "ghi", "mno", "mno");
    actual0 = records.stream().map(k -> k.getKey().getColumns()[0]).collect(Collectors.toList());
    Assert.assertEquals(actual0, expected0);

    // string column desc
    s0 = new SelectionSort();
    s0.setColumn("dim0");
    orderBy = Lists.newArrayList(s0);
    keysComparator = OrderByUtils.getKeysComparator(dataSchema, orderBy);
    records.sort(keysComparator);
    expected0 = Lists.newArrayList("mno", "mno", "ghi", "bcd", "abc", "abc", "abc");
    actual0 = records.stream().map(k -> k.getKey().getColumns()[0]).collect(Collectors.toList());
    Assert.assertEquals(actual0, expected0);

    // numeric dimension
    s2 = new SelectionSort();
    s2.setColumn("dim2");
    s2.setIsAsc(true);
    orderBy = Lists.newArrayList(s2);
    keysComparator = OrderByUtils.getKeysComparator(dataSchema, orderBy);
    records.sort(keysComparator);
    expected2 = Lists.newArrayList(6, 6, 10, 10, 10, 50, 66);
    actual2 = records.stream().map(k -> k.getKey().getColumns()[2]).collect(Collectors.toList());
    Assert.assertEquals(actual2, expected2);

    // desc
    s2 = new SelectionSort();
    s2.setColumn("dim2");
    s2.setIsAsc(false);
    orderBy = Lists.newArrayList(s2);
    keysComparator = OrderByUtils.getKeysComparator(dataSchema, orderBy);
    records.sort(keysComparator);
    expected2 = Lists.newArrayList(66, 50, 10, 10, 10, 6, 6);
    actual2 = records.stream().map(k -> k.getKey().getColumns()[2]).collect(Collectors.toList());
    Assert.assertEquals(actual2, expected2);

    // string numeric dimension
    s3 = new SelectionSort();
    s3.setColumn("dim3");
    s3.setIsAsc(true);
    orderBy = Lists.newArrayList(s3);
    keysComparator = OrderByUtils.getKeysComparator(dataSchema, orderBy);
    records.sort(keysComparator);
    expected3 = Lists.newArrayList("100", "200", "209", "30", "30", "30", "5");
    actual3 = records.stream().map(k -> k.getKey().getColumns()[3]).collect(Collectors.toList());
    Assert.assertEquals(actual3, expected3);

    // desc
    s3 = new SelectionSort();
    s3.setColumn("dim3");
    orderBy = Lists.newArrayList(s3);
    keysComparator = OrderByUtils.getKeysComparator(dataSchema, orderBy);
    records.sort(keysComparator);
    expected3 = Lists.newArrayList("5", "30", "30", "30", "209", "200", "100");
    actual3 = records.stream().map(k -> k.getKey().getColumns()[3]).collect(Collectors.toList());
    Assert.assertEquals(actual3, expected3);

    // multiple dimensions
    s0 = new SelectionSort();
    s0.setColumn("dim0");
    s1 = new SelectionSort();
    s1.setColumn("dim1");
    s1.setIsAsc(true);
    orderBy = Lists.newArrayList(s0, s1);
    keysComparator = OrderByUtils.getKeysComparator(dataSchema, orderBy);
    records.sort(keysComparator);
    expected0 = Lists.newArrayList("mno", "mno", "ghi", "bcd", "abc", "abc", "abc");
    actual0 = records.stream().map(k -> k.getKey().getColumns()[0]).collect(Collectors.toList());
    Assert.assertEquals(actual0, expected0);
    expected1 = Lists.newArrayList("h776", "p125", "i889", "i889", "p125", "p125", "r666");
    actual1 = records.stream().map(k -> k.getKey().getColumns()[1]).collect(Collectors.toList());
    Assert.assertEquals(actual1, expected1);

    s2 = new SelectionSort();
    s2.setColumn("dim2");
    s1 = new SelectionSort();
    s1.setColumn("dim1");
    orderBy = Lists.newArrayList(s2, s1);
    keysComparator = OrderByUtils.getKeysComparator(dataSchema, orderBy);
    records.sort(keysComparator);
    expected2 = Lists.newArrayList(66, 50, 10, 10, 10, 6, 6);
    actual2 = records.stream().map(k -> k.getKey().getColumns()[2]).collect(Collectors.toList());
    Assert.assertEquals(actual2, expected2);
    expected1 = Lists.newArrayList("i889", "p125", "p125", "p125", "h776", "r666", "i889");
    actual1 = records.stream().map(k -> k.getKey().getColumns()[1]).collect(Collectors.toList());
    Assert.assertEquals(actual1, expected1);

    s2 = new SelectionSort();
    s2.setColumn("dim2");
    s1 = new SelectionSort();
    s1.setColumn("dim1");
    s3 = new SelectionSort();
    s3.setColumn("dim3");
    s3.setIsAsc(true);
    orderBy = Lists.newArrayList(s2, s1, s3);
    keysComparator = OrderByUtils.getKeysComparator(dataSchema, orderBy);
    records.sort(keysComparator);
    expected2 = Lists.newArrayList(66, 50, 10, 10, 10, 6, 6);
    actual2 = records.stream().map(k -> k.getKey().getColumns()[2]).collect(Collectors.toList());
    Assert.assertEquals(actual2, expected2);
    expected1 = Lists.newArrayList("i889", "p125", "p125", "p125", "h776", "r666", "i889");
    actual1 = records.stream().map(k -> k.getKey().getColumns()[1]).collect(Collectors.toList());
    Assert.assertEquals(actual1, expected1);
    expected3 = Lists.newArrayList("5", "30", "30", "30", "100", "200", "209");
    actual3 = records.stream().map(k -> k.getKey().getColumns()[3]).collect(Collectors.toList());
    Assert.assertEquals(actual3, expected3);

    // all columns
    s0 = new SelectionSort();
    s0.setColumn("dim0");
    s0.setIsAsc(true);
    s2 = new SelectionSort();
    s2.setColumn("dim2");
    s1 = new SelectionSort();
    s1.setColumn("dim1");
    s3 = new SelectionSort();
    s3.setColumn("dim3");
    s3.setIsAsc(true);
    orderBy = Lists.newArrayList(s0, s2, s1, s3);
    keysComparator = OrderByUtils.getKeysComparator(dataSchema, orderBy);
    records.sort(keysComparator);
    expected0 = Lists.newArrayList( "abc", "abc", "abc", "bcd", "ghi", "mno", "mno");
    actual0 = records.stream().map(k -> k.getKey().getColumns()[0]).collect(Collectors.toList());
    Assert.assertEquals(actual0, expected0);
    expected2 = Lists.newArrayList(50, 10, 6, 6, 66, 10, 10);
    actual2 = records.stream().map(k -> k.getKey().getColumns()[2]).collect(Collectors.toList());
    Assert.assertEquals(actual2, expected2);
    expected1 = Lists.newArrayList("p125", "p125", "r666", "i889", "i889", "p125", "h776");
    actual1 = records.stream().map(k -> k.getKey().getColumns()[1]).collect(Collectors.toList());
    Assert.assertEquals(actual1, expected1);
    expected3 = Lists.newArrayList("30", "30", "200", "209", "5", "30", "100");
    actual3 = records.stream().map(k -> k.getKey().getColumns()[3]).collect(Collectors.toList());
    Assert.assertEquals(actual3, expected3);

    // non existent column
    s0 = new SelectionSort();
    s0.setColumn("dim10");
    s0.setIsAsc(true);
    orderBy = Lists.newArrayList(s0);
    boolean exception = false;
    try {
      keysComparator = OrderByUtils.getKeysComparator(dataSchema, orderBy);
      records.sort(keysComparator);
    } catch (UnsupportedOperationException e) {
      exception = true;
    }
    Assert.assertTrue(exception);

    // keys and values
    Map<String, String> aggregationParams = new HashMap<>();
    aggregationParams.put("column", "metric0");
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("SUM");
    aggregationInfo.setAggregationParams(aggregationParams);
    List<AggregationInfo> aggregationInfos = Lists.newArrayList(aggregationInfo);
    s0 = new SelectionSort();
    s0.setColumn("dim0");
    s0.setIsAsc(true);
    s4 = new SelectionSort();
    s4.setColumn("sum(metric0)");
    orderBy = Lists.newArrayList(s0, s4);
    Comparator<Record> keysAndValuesComparator =
        OrderByUtils.getKeysAndValuesComparator(dataSchema, orderBy, aggregationInfos);
    records.sort(keysAndValuesComparator);
    expected0 = Lists.newArrayList("abc", "abc", "abc", "bcd", "ghi", "mno", "mno");
    actual0 = records.stream().map(k -> k.getKey().getColumns()[0]).collect(Collectors.toList());
    Assert.assertEquals(actual0, expected0);
    expected4 = Lists.newArrayList(200d, 200d, 10d, 100d, 50d, 250d, 100d);
    actual4 = records.stream().map(k -> k.getValues()[0]).collect(Collectors.toList());
    Assert.assertEquals(actual4, expected4);

    // values only
    dataSchema = new DataSchema(new String[]{"metric0"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE});
    s4 = new SelectionSort();
    s4.setColumn("metric0");
    s4.setIsAsc(true);
    orderBy = Lists.newArrayList(s4);
    Comparator<Record> valuesComparator = OrderByUtils.getValuesComparator(dataSchema, orderBy);
    records.sort(valuesComparator);
    expected4 = Lists.newArrayList(10d, 50d, 100d, 100d, 200d, 200d, 250d);
    actual4 = records.stream().map(k -> k.getValues()[0]).collect(Collectors.toList());
    Assert.assertEquals(actual4, expected4);
  }

  private Record getRecord(Object[] keys, Object[] values) {
    return new Record(new Key(keys), values);
  }
}
