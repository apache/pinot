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
package org.apache.pinot.segment.local.indexsegment.mutable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MutableSegmentImplIngestionAggregationTest {
  private static final String DIMENSION_1 = "dim1";
  private static final String DIMENSION_2 = "dim2";

  private static final String METRIC = "metric";
  private static final String METRIC_2 = "metric_2";

  private static final String TIME_COLUMN1 = "time1";
  private static final String TIME_COLUMN2 = "time2";
  private static final String KEY_SEPARATOR = "\t\t";
  private static final int NUM_ROWS = 10001;

  private static final Schema.SchemaBuilder getSchemaBuilder() {
    return new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addSingleValueDimension(DIMENSION_1, FieldSpec.DataType.INT)
        .addSingleValueDimension(DIMENSION_2, FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN1, FieldSpec.DataType.INT, "1:DAYS:EPOCH", "1:DAYS")
        .addDateTime(TIME_COLUMN2, FieldSpec.DataType.INT, "1:HOURS:EPOCH", "1:HOURS");
  }

  private static final Set<String> VAR_LENGTH_SET = Collections.singleton(DIMENSION_2);
  private static final Set<String> INVERTED_INDEX_SET =
      new HashSet<>(Arrays.asList(DIMENSION_1, DIMENSION_2, TIME_COLUMN1, TIME_COLUMN2));

  private static final List<String> STRING_VALUES =
      Collections.unmodifiableList(Arrays.asList("aa", "bbb", "cc", "ddd", "ee", "fff", "gg", "hhh", "ii", "jjj"));

  @Test
  public void testSameSrcDifferentAggregations()
      throws Exception {
    String m1 = "metric_MAX";
    String m2 = "metric_MIN";

    Schema schema =
        getSchemaBuilder().addMetric(m2, FieldSpec.DataType.DOUBLE).addMetric(m1, FieldSpec.DataType.DOUBLE).build();
    MutableSegmentImpl mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, new HashSet<>(Arrays.asList(m2, m1)),
            VAR_LENGTH_SET, INVERTED_INDEX_SET,
            Arrays.asList(new AggregationConfig(m1, "MAX(metric)"), new AggregationConfig(m2, "MIN(metric)")));

    Map<String, Double> expectedMin = new HashMap<>();
    Map<String, Double> expectedMax = new HashMap<>();
    for (List<Metric> metrics : addRows(1, mutableSegmentImpl)) {
      expectedMin.put(metrics.get(0).getKey(),
          Math.min(expectedMin.getOrDefault(metrics.get(0).getKey(), Double.POSITIVE_INFINITY),
              metrics.get(0).getValue()));
      expectedMax.put(metrics.get(0).getKey(),
          Math.max(expectedMax.getOrDefault(metrics.get(0).getKey(), Double.NEGATIVE_INFINITY),
              metrics.get(0).getValue()));
    }

    GenericRow reuse = new GenericRow();
    for (int docId = 0; docId < expectedMax.size(); docId++) {
      GenericRow row = mutableSegmentImpl.getRecord(docId, reuse);
      String key = buildKey(row);
      Assert.assertEquals(row.getValue(m2), expectedMin.get(key), key);
      Assert.assertEquals(row.getValue(m1), expectedMax.get(key), key);
    }

    mutableSegmentImpl.destroy();
  }

  @Test
  public void testSameAggregationDifferentSrc()
      throws Exception {
    String m1 = "sum1";
    String m2 = "sum2";

    Schema schema =
        getSchemaBuilder().addMetric(m1, FieldSpec.DataType.INT).addMetric(m2, FieldSpec.DataType.LONG).build();
    MutableSegmentImpl mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, new HashSet<>(Arrays.asList(m2, m1)),
            VAR_LENGTH_SET, INVERTED_INDEX_SET,
            Arrays.asList(new AggregationConfig(m1, "SUM(metric)"), new AggregationConfig(m2, "SUM(metric_2)")));

    Map<String, Integer> expectedSum1 = new HashMap<>();
    Map<String, Long> expectedSum2 = new HashMap<>();
    for (List<Metric> metrics : addRows(2, mutableSegmentImpl)) {
      expectedSum1.put(metrics.get(0).getKey(),
          expectedSum1.getOrDefault(metrics.get(0).getKey(), 0) + metrics.get(0).getValue());
      expectedSum2.put(metrics.get(1).getKey(),
          expectedSum2.getOrDefault(metrics.get(1).getKey(), 0L) + metrics.get(1).getValue().longValue());
    }

    GenericRow reuse = new GenericRow();
    for (int docId = 0; docId < expectedSum1.size(); docId++) {
      GenericRow row = mutableSegmentImpl.getRecord(docId, reuse);
      String key = buildKey(row);
      Assert.assertEquals(row.getValue(m1), expectedSum1.get(key), key);
      Assert.assertEquals(row.getValue(m2), expectedSum2.get(key), key);
    }

    mutableSegmentImpl.destroy();
  }

  @Test
  public void testCOUNT()
      throws Exception {
    String m1 = "count1";
    String m2 = "count2";

    Schema schema =
        getSchemaBuilder().addMetric(m1, FieldSpec.DataType.LONG).addMetric(m2, FieldSpec.DataType.LONG).build();
    MutableSegmentImpl mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, new HashSet<>(Arrays.asList(m1, m2)),
            VAR_LENGTH_SET, INVERTED_INDEX_SET,
            Arrays.asList(new AggregationConfig(m1, "COUNT(metric)"), new AggregationConfig(m2, "COUNT(*)")));

    Map<String, Long> expectedCount = new HashMap<>();
    for (List<Metric> metrics : addRows(3, mutableSegmentImpl)) {
      expectedCount.put(metrics.get(0).getKey(),
          expectedCount.getOrDefault(metrics.get(0).getKey(), 0L) + 1L);
    }

    GenericRow reuse = new GenericRow();
    for (int docId = 0; docId < expectedCount.size(); docId++) {
      GenericRow row = mutableSegmentImpl.getRecord(docId, reuse);
      String key = buildKey(row);
      Assert.assertEquals(row.getValue(m1), expectedCount.get(key), key);
      Assert.assertEquals(row.getValue(m2), expectedCount.get(key), key);
    }

    mutableSegmentImpl.destroy();
  }

  private String buildKey(GenericRow row) {
    return row.getValue(DIMENSION_1) + KEY_SEPARATOR + row.getValue(DIMENSION_2) + KEY_SEPARATOR + row.getValue(
        TIME_COLUMN1) + KEY_SEPARATOR + row.getValue(TIME_COLUMN2);
  }

  private GenericRow getRow(Random random) {
    GenericRow row = new GenericRow();

    row.putValue(DIMENSION_1, random.nextInt(10));
    row.putValue(DIMENSION_2, STRING_VALUES.get(random.nextInt(STRING_VALUES.size())));
    row.putValue(TIME_COLUMN1, random.nextInt(10));
    row.putValue(TIME_COLUMN2, random.nextInt(5));

    return row;
  }

  private class Metric {
    private final String _key;
    private final Integer _value;

    Metric(String key, Integer value) {
      _key = key;
      _value = value;
    }

    public String getKey() {
      return _key;
    }

    public Integer getValue() {
      return _value;
    }
  }

  private List<List<Metric>> addRows(long seed, MutableSegmentImpl mutableSegmentImpl)
      throws Exception {
    List<List<Metric>> metrics = new ArrayList<>();
    Set<String> keys = new HashSet<>();


    Random random = new Random(seed);
    StreamMessageMetadata defaultMetadata = new StreamMessageMetadata(System.currentTimeMillis(), new GenericRow());

    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = getRow(random);
      // This needs to be relatively low since it will tend to overflow with the Int-to-Double conversion.
      Integer metricValue = random.nextInt(10000);
      Integer metric2Value = random.nextInt();
      row.putValue(METRIC, metricValue);
      row.putValue(METRIC_2, metric2Value);

      mutableSegmentImpl.index(row, defaultMetadata);

      String key = buildKey(row);
      metrics.add(Arrays.asList(new Metric(key, metricValue), new Metric(key, metric2Value)));
      keys.add(key);
    }

    int numDocsIndexed = mutableSegmentImpl.getNumDocsIndexed();
    Assert.assertEquals(numDocsIndexed, keys.size());

    // Assert that aggregation happened.
    Assert.assertTrue(numDocsIndexed < NUM_ROWS);

    return metrics;
  }
}
