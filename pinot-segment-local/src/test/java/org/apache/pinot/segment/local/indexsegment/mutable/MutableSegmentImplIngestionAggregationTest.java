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

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class MutableSegmentImplIngestionAggregationTest {
  private static final String DIMENSION_1 = "dim1";
  private static final String DIMENSION_2 = "dim2";

  private static final String METRIC = "metric";
  private static final String METRIC_2 = "metric_2";

  private static final String TIME_COLUMN1 = "time1";
  private static final String TIME_COLUMN2 = "time2";
  private static final String KEY_SEPARATOR = "\t\t";
  private static final int NUM_ROWS = 10001;

  private static final StreamMessageMetadata METADATA = mock(StreamMessageMetadata.class);

  private static Schema.SchemaBuilder getSchemaBuilder() {
    return new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addSingleValueDimension(DIMENSION_1, DataType.INT)
        .addSingleValueDimension(DIMENSION_2, DataType.STRING)
        .addDateTime(TIME_COLUMN1, DataType.INT, "1:DAYS:EPOCH", "1:DAYS")
        .addDateTime(TIME_COLUMN2, DataType.INT, "1:HOURS:EPOCH", "1:HOURS");
  }

  private static final Set<String> VAR_LENGTH_SET = Set.of(DIMENSION_2);
  private static final Set<String> INVERTED_INDEX_SET = Set.of(DIMENSION_1, DIMENSION_2, TIME_COLUMN1, TIME_COLUMN2);

  private static final List<String> STRING_VALUES =
      List.of("aa", "bbb", "cc", "ddd", "ee", "fff", "gg", "hhh", "ii", "jjj");

  @Test
  public void testSameSrcDifferentAggregations()
      throws Exception {
    String m1 = "metric_MAX";
    String m2 = "metric_MIN";

    Schema schema = getSchemaBuilder().addMetric(m2, DataType.DOUBLE).addMetric(m1, DataType.DOUBLE).build();
    MutableSegmentImpl mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(m1, m2), VAR_LENGTH_SET, INVERTED_INDEX_SET,
            List.of(new AggregationConfig(m1, "MAX(metric)"), new AggregationConfig(m2, "MIN(metric)")));

    Map<String, Double> expectedMin = new HashMap<>();
    Map<String, Double> expectedMax = new HashMap<>();
    for (List<Metric> metrics : addRows(1, mutableSegmentImpl)) {
      expectedMin.put(metrics.get(0).getKey(),
          Math.min(expectedMin.getOrDefault(metrics.get(0).getKey(), Double.POSITIVE_INFINITY),
              (Integer) metrics.get(0).getValue()));
      expectedMax.put(metrics.get(0).getKey(),
          Math.max(expectedMax.getOrDefault(metrics.get(0).getKey(), Double.NEGATIVE_INFINITY),
              (Integer) metrics.get(0).getValue()));
    }

    int numDocsIndexed = mutableSegmentImpl.getNumDocsIndexed();
    GenericRow reuse = new GenericRow();
    for (int docId = 0; docId < numDocsIndexed; docId++) {
      GenericRow row = mutableSegmentImpl.getRecord(docId, reuse);
      String key = buildKey(row);
      assertEquals(row.getValue(m2), expectedMin.get(key), key);
      assertEquals(row.getValue(m1), expectedMax.get(key), key);
    }

    mutableSegmentImpl.destroy();
  }

  @Test
  public void testSameAggregationDifferentSrc()
      throws Exception {
    String m1 = "sum1";
    String m2 = "sum2";

    Schema schema = getSchemaBuilder().addMetric(m1, DataType.INT).addMetric(m2, DataType.LONG).build();
    MutableSegmentImpl mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(m1, m2), VAR_LENGTH_SET, INVERTED_INDEX_SET,
            List.of(new AggregationConfig(m1, "SUM(metric)"), new AggregationConfig(m2, "SUM(metric_2)")));

    Map<String, Integer> expectedSum1 = new HashMap<>();
    Map<String, Long> expectedSum2 = new HashMap<>();
    for (List<Metric> metrics : addRows(2, mutableSegmentImpl)) {
      expectedSum1.put(metrics.get(0).getKey(),
          expectedSum1.getOrDefault(metrics.get(0).getKey(), 0) + (Integer) (metrics.get(0).getValue()));
      expectedSum2.put(metrics.get(1).getKey(),
          expectedSum2.getOrDefault(metrics.get(1).getKey(), 0L) + ((Integer) metrics.get(1).getValue()).longValue());
    }

    int numDocsIndexed = mutableSegmentImpl.getNumDocsIndexed();
    GenericRow reuse = new GenericRow();
    for (int docId = 0; docId < numDocsIndexed; docId++) {
      GenericRow row = mutableSegmentImpl.getRecord(docId, reuse);
      String key = buildKey(row);
      assertEquals(row.getValue(m1), expectedSum1.get(key), key);
      assertEquals(row.getValue(m2), expectedSum2.get(key), key);
    }

    mutableSegmentImpl.destroy();
  }

  @Test
  public void testNullValues()
      throws Exception {
    String m1 = "sum1";

    Schema schema = getSchemaBuilder().addMetric(m1, DataType.INT).build();
    MutableSegmentImpl mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(m1), VAR_LENGTH_SET, INVERTED_INDEX_SET,
            List.of(new AggregationConfig(m1, "SUM(metric)")));

    long seed = 2;
    Random random = new Random(seed);

    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = getRow(random, 1);
      mutableSegmentImpl.index(row, METADATA);
    }

    int numDocsIndexed = mutableSegmentImpl.getNumDocsIndexed();
    assertTrue(numDocsIndexed < NUM_ROWS);

    GenericRow reuse = new GenericRow();
    for (int i = 0; i < numDocsIndexed; i++) {
      GenericRow row = mutableSegmentImpl.getRecord(i, reuse);
      String key = buildKey(row);
      assertEquals(row.getValue(m1), 0, key);
    }

    mutableSegmentImpl.destroy();
  }

  @Test
  public void testDistinctCountHLL()
      throws Exception {
    String m1 = "hll1";

    Schema schema = getSchemaBuilder().addMetric(m1, DataType.BYTES).build();
    MutableSegmentImpl mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(m1), VAR_LENGTH_SET, INVERTED_INDEX_SET,
            List.of(new AggregationConfig(m1, "distinctCountHLL(metric, 12)")));

    Map<String, HyperLogLog> expectedValues = addRowsDistinctCountHLL(998, mutableSegmentImpl);

    int numDocsIndexed = mutableSegmentImpl.getNumDocsIndexed();
    assertTrue(numDocsIndexed < NUM_ROWS);
    assertEquals(numDocsIndexed, expectedValues.size());

    GenericRow reuse = new GenericRow();
    for (int docId = 0; docId < numDocsIndexed; docId++) {
      GenericRow row = mutableSegmentImpl.getRecord(docId, reuse);
      HyperLogLog actualHLL = CustomSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize((byte[]) row.getValue(m1));
      HyperLogLog expectedHLL = expectedValues.get(buildKey(row));
      assertEquals(actualHLL.cardinality(), expectedHLL.cardinality());
    }

    mutableSegmentImpl.destroy();
  }

  @Test
  public void testCount()
      throws Exception {
    String m1 = "count1";
    String m2 = "count2";

    Schema schema = getSchemaBuilder().addMetric(m1, DataType.LONG).addMetric(m2, DataType.LONG).build();
    MutableSegmentImpl mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(m1, m2), VAR_LENGTH_SET, INVERTED_INDEX_SET,
            List.of(new AggregationConfig(m1, "COUNT(metric)"), new AggregationConfig(m2, "COUNT(*)")));

    Map<String, Long> expectedCount = new HashMap<>();
    for (List<Metric> metrics : addRows(3, mutableSegmentImpl)) {
      expectedCount.put(metrics.get(0).getKey(), expectedCount.getOrDefault(metrics.get(0).getKey(), 0L) + 1L);
    }

    int numDocsIndexed = mutableSegmentImpl.getNumDocsIndexed();
    GenericRow reuse = new GenericRow();
    for (int docId = 0; docId < numDocsIndexed; docId++) {
      GenericRow row = mutableSegmentImpl.getRecord(docId, reuse);
      String key = buildKey(row);
      assertEquals(row.getValue(m1), expectedCount.get(key), key);
      assertEquals(row.getValue(m2), expectedCount.get(key), key);
    }

    mutableSegmentImpl.destroy();
  }

  private String buildKey(GenericRow row) {
    return row.getValue(DIMENSION_1) + KEY_SEPARATOR + row.getValue(DIMENSION_2) + KEY_SEPARATOR + row.getValue(
        TIME_COLUMN1) + KEY_SEPARATOR + row.getValue(TIME_COLUMN2);
  }

  private GenericRow getRow(Random random, int multiplicationFactor) {
    GenericRow row = new GenericRow();

    row.putValue(DIMENSION_1, random.nextInt(2 * multiplicationFactor));
    row.putValue(DIMENSION_2, STRING_VALUES.get(random.nextInt(STRING_VALUES.size())));
    row.putValue(TIME_COLUMN1, random.nextInt(2 * multiplicationFactor));
    row.putValue(TIME_COLUMN2, random.nextInt(2 * multiplicationFactor));

    return row;
  }

  private static class Metric {
    final String _key;
    final Object _value;

    Metric(String key, Object value) {
      _key = key;
      _value = value;
    }

    String getKey() {
      return _key;
    }

    Object getValue() {
      return _value;
    }
  }

  private Map<String, HyperLogLog> addRowsDistinctCountHLL(long seed, MutableSegmentImpl mutableSegmentImpl)
      throws Exception {
    Map<String, HyperLogLog> valueMap = new HashMap<>();

    Random random = new Random(seed);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = getRow(random, 1);
      String key = buildKey(row);

      int metricValue = random.nextInt(5000000);
      row.putValue(METRIC, metricValue);
      mutableSegmentImpl.index(row, METADATA);

      valueMap.computeIfAbsent(key, k -> new HyperLogLog(12)).offer(metricValue);
    }

    return valueMap;
  }

  private Map<String, BigDecimal> addRowsSumPrecision(long seed, MutableSegmentImpl mutableSegmentImpl)
      throws Exception {
    Map<String, BigDecimal> valueMap = new HashMap<>();

    Random random = new Random(seed);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = getRow(random, 1);
      String key = buildKey(row);

      BigDecimal metricValue = generateRandomBigDecimal(random, 5, 6);
      row.putValue(METRIC, metricValue);
      mutableSegmentImpl.index(row, METADATA);

      valueMap.compute(key, (k, v) -> {
        if (v == null) {
          return metricValue;
        } else {
          return v.add(metricValue);
        }
      });
    }

    return valueMap;
  }

  private List<List<Metric>> addRows(long seed, MutableSegmentImpl mutableSegmentImpl)
      throws Exception {
    List<List<Metric>> metrics = new ArrayList<>();
    Set<String> keys = new HashSet<>();

    Random random = new Random(seed);

    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = getRow(random, 1);
      Integer metricValue = random.nextInt(10000);
      Integer metric2Value = random.nextInt();
      row.putValue(METRIC, metricValue);
      row.putValue(METRIC_2, metric2Value);

      mutableSegmentImpl.index(row, METADATA);

      String key = buildKey(row);
      metrics.add(List.of(new Metric(key, metricValue), new Metric(key, metric2Value)));
      keys.add(key);
    }

    int numDocsIndexed = mutableSegmentImpl.getNumDocsIndexed();
    assertEquals(numDocsIndexed, keys.size());

    // Assert that aggregation happened.
    assertTrue(numDocsIndexed < NUM_ROWS);

    return metrics;
  }

  @Test
  public void testSumPrecision()
      throws Exception {
    String m1 = "sumPrecision1";
    Schema schema = getSchemaBuilder().addMetric(m1, DataType.BIG_DECIMAL).build();

    MutableSegmentImpl mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(m1), VAR_LENGTH_SET, INVERTED_INDEX_SET,
            // Setting precision to 38 in the arguments for SUM_PRECISION
            List.of(new AggregationConfig(m1, "SUM_PRECISION(metric, 38)")));

    Map<String, BigDecimal> expectedValues = addRowsSumPrecision(998, mutableSegmentImpl);

    int numDocsIndexed = mutableSegmentImpl.getNumDocsIndexed();
    assertTrue(numDocsIndexed < NUM_ROWS);
    assertEquals(numDocsIndexed, expectedValues.size());

    GenericRow reuse = new GenericRow();
    for (int docId = 0; docId < numDocsIndexed; docId++) {
      GenericRow row = mutableSegmentImpl.getRecord(docId, reuse);
      BigDecimal actualBigDecimal = (BigDecimal) row.getValue(m1);
      BigDecimal expectedBigDecimal = expectedValues.get(buildKey(row));
      assertEquals(actualBigDecimal, expectedBigDecimal);
    }

    mutableSegmentImpl.destroy();
  }

  @Test
  public void testBigDecimalTooBig() {
    String m1 = "sumPrecision1";
    Schema schema = getSchemaBuilder().addMetric(m1, DataType.BIG_DECIMAL).build();

    int seed = 1;
    Random random = new Random(seed);

    MutableSegmentImpl mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(m1), VAR_LENGTH_SET, INVERTED_INDEX_SET,
            List.of(new AggregationConfig(m1, "SUM_PRECISION(metric, 3)")));

    // Make a big decimal larger than 3 precision and try to index it
    BigDecimal large = BigDecimalUtils.generateMaximumNumberWithPrecision(5);
    GenericRow row = getRow(random, 1);

    row.putValue("metric", large);
    assertThrows(IllegalArgumentException.class, () -> {
      mutableSegmentImpl.index(row, METADATA);
    });

    mutableSegmentImpl.destroy();
  }

  private static BigDecimal generateRandomBigDecimal(Random random, int maxPrecision, int scale) {
    int precision = 1 + random.nextInt(maxPrecision);
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < precision; i++) {
      stringBuilder.append(1 + random.nextInt(9));
    }
    BigDecimal bigDecimal = new BigDecimal(stringBuilder.toString()).setScale(scale, RoundingMode.UNNECESSARY);
    return random.nextBoolean() ? bigDecimal : bigDecimal.negate();
  }
}
