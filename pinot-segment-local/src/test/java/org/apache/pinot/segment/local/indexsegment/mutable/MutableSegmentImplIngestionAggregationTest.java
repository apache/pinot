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

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.local.aggregator.DistinctCountHLLValueAggregator;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.utils.BigDecimalUtils;
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
              (Integer) metrics.get(0).getValue()));
      expectedMax.put(metrics.get(0).getKey(),
          Math.max(expectedMax.getOrDefault(metrics.get(0).getKey(), Double.NEGATIVE_INFINITY),
              (Integer) metrics.get(0).getValue()));
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
          expectedSum1.getOrDefault(metrics.get(0).getKey(), 0) + (Integer) (metrics.get(0).getValue()));
      expectedSum2.put(metrics.get(1).getKey(),
          expectedSum2.getOrDefault(metrics.get(1).getKey(), 0L) + ((Integer) metrics.get(1).getValue()).longValue());
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
  public void testValuesAreNullThrowsException()
      throws Exception {
    String m1 = "sum1";

    Schema schema =
        getSchemaBuilder().addMetric(m1, FieldSpec.DataType.INT).build();
    MutableSegmentImpl mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, new HashSet<>(Arrays.asList(m1)),
            VAR_LENGTH_SET, INVERTED_INDEX_SET,
            Arrays.asList(new AggregationConfig(m1, "SUM(metric)")));


    Set<String> keys = new HashSet<>();

    long seed = 2;
    Random random = new Random(seed);
    StreamMessageMetadata defaultMetadata = new StreamMessageMetadata(System.currentTimeMillis(), null);

    // Generate random int to prevent overflow
    GenericRow row = getRow(random, 1);
    row.putValue(METRIC, null);
    try {
mutableSegmentImpl.index(row, defaultMetadata);
      Assert.fail();
    } catch (NullPointerException e) {
      Assert.assertTrue(e.getMessage().contains("Cannot invoke \"java.lang.Number.doubleValue()\" because"
          + " \"rawValue\" is null"));
    }

    mutableSegmentImpl.destroy();
  }

  @Test
  public void testDISTINCTCOUNTHLL() throws Exception {
    String m1 = "metric_DISTINCTCOUNTHLL";
    Schema schema = getSchemaBuilder().addMetric(m1, FieldSpec.DataType.BYTES).build();
    MutableSegmentImpl mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, new HashSet<>(Arrays.asList(m1)),
            VAR_LENGTH_SET, INVERTED_INDEX_SET,
            Arrays.asList(new AggregationConfig(m1, "DISTINCTCOUNTHLL(metric, 12)")));

    Map<String, HLLTestData> expected = new HashMap<>();
    List<Metric> metrics = addRowsDistinctCountHLL(998, mutableSegmentImpl);
    for (Metric metric : metrics) {
      expected.put(metric.getKey(), (HLLTestData) metric.getValue());
    }

    List<ExpressionContext> arguments =
        List.of(
            ExpressionContext.forIdentifier("distinctcounthll"),
            ExpressionContext.forLiteralContext(Literal.stringValue("12"))
        );
    DistinctCountHLLValueAggregator valueAggregator = new DistinctCountHLLValueAggregator(arguments);

    Set<Integer> integers = new HashSet<>();

    /*
    Assert that the distinct count is within an error margin. We assert on the cardinality of the HLL in the docID
    and the HLL we made, but also on the cardinality of the HLL in the docID and the actual cardinality from
    the set of integers.
     */
    GenericRow reuse = new GenericRow();
    for (int docId = 0; docId < expected.size(); docId++) {
      GenericRow row = mutableSegmentImpl.getRecord(docId, reuse);
      String key = buildKey(row);

      integers.addAll(expected.get(key)._integers);

      HyperLogLog expectedHLL = expected.get(key)._hll;
      HyperLogLog actualHLL = valueAggregator.deserializeAggregatedValue((byte[]) row.getValue(m1));

      Assert.assertEquals(actualHLL.cardinality(), expectedHLL.cardinality(), (int) (expectedHLL.cardinality() * 0.04),
          "The HLL cardinality from the index is within a tolerable error margin (4%) of the cardinality of the "
              + "expected HLL.");
      Assert.assertEquals(actualHLL.cardinality(), expected.get(key)._integers.size(),
          expected.get(key)._integers.size() * 0.04,
          "The HLL cardinality from the index is within a tolerable error margin (4%) of the actual cardinality of "
              + "the integers.");
    }

    /*
    Assert that the aggregated HyperLogLog is also within the error margin
     */
    HyperLogLog togetherHLL = new HyperLogLog(12);
    expected.forEach((key, value) -> {
      try {
        togetherHLL.addAll(value._hll);
      } catch (CardinalityMergeException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });

    Assert.assertEquals(togetherHLL.cardinality(), integers.size(), (int) (integers.size() * 0.04),
        "The aggregated HLL cardinality is within a tolerable error margin (4%) of the actual cardinality of the "
            + "integers.");
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

  private GenericRow getRow(Random random, Integer multiplicationFactor) {
    GenericRow row = new GenericRow();

    row.putValue(DIMENSION_1, random.nextInt(2 * multiplicationFactor));
    row.putValue(DIMENSION_2, STRING_VALUES.get(random.nextInt(STRING_VALUES.size())));
    row.putValue(TIME_COLUMN1, random.nextInt(2 * multiplicationFactor));
    row.putValue(TIME_COLUMN2, random.nextInt(2 * multiplicationFactor));

    return row;
  }

  private class HLLTestData {
    private HyperLogLog _hll;
    private Set<Integer> _integers;

    public HLLTestData(HyperLogLog hll, Set<Integer> integers) {
      _hll = hll;
      _integers = integers;
    }

    public HyperLogLog getHll() {
      return _hll;
    }

    public Set<Integer> getIntegers() {
      return _integers;
    }
  }

  private class Metric {
    private final String _key;
    private final Object _value;

    Metric(String key, Object value) {
      _key = key;
      _value = value;
    }

    public String getKey() {
      return _key;
    }

    public Object getValue() {
      return _value;
    }
  }

  private List<Metric> addRowsDistinctCountHLL(long seed, MutableSegmentImpl mutableSegmentImpl)
      throws Exception {
    List<Metric> metrics = new ArrayList<>();

    Random random = new Random(seed);
    StreamMessageMetadata defaultMetadata = new StreamMessageMetadata(System.currentTimeMillis(), null);

    HashMap<String, HyperLogLog> hllMap = new HashMap<>();
    HashMap<String, Set<Integer>> distinctMap = new HashMap<>();

    Integer rows = 500000;

    for (int i = 0; i < (rows); i++) {
      GenericRow row = getRow(random, 1);
      String key = buildKey(row);

      int metricValue = random.nextInt(5000000);
      row.putValue(METRIC, metricValue);

      if (hllMap.containsKey(key)) {
        hllMap.get(key).offer(row.getValue(METRIC));
        distinctMap.get(key).add(metricValue);
      } else {
        HyperLogLog hll = new HyperLogLog(12);
        hll.offer(row.getValue(METRIC));
        hllMap.put(key, hll);
        distinctMap.put(key, new HashSet<>(metricValue));
      }

      mutableSegmentImpl.index(row, defaultMetadata);
    }

    distinctMap.forEach(
        (key, value) -> metrics.add(new Metric(key, new HLLTestData(hllMap.get(key), distinctMap.get(key)))));

    int numDocsIndexed = mutableSegmentImpl.getNumDocsIndexed();
    Assert.assertEquals(numDocsIndexed, hllMap.keySet().size());

    // Assert that aggregation happened.
    Assert.assertTrue(numDocsIndexed < NUM_ROWS);

    return metrics;
  }

  private List<Metric> addRowsSUMPRECISION(long seed, MutableSegmentImpl mutableSegmentImpl)
      throws Exception {
    List<Metric> metrics = new ArrayList<>();

    Random random = new Random(seed);
    StreamMessageMetadata defaultMetadata = new StreamMessageMetadata(System.currentTimeMillis(), null);

    HashMap<String, BigDecimal> bdMap = new HashMap<>();
    HashMap<String, ArrayList<BigDecimal>> bdIndividualMap = new HashMap<>();

    Integer rows = 50000;

    for (int i = 0; i < (rows); i++) {
      GenericRow row = getRow(random, 1);
      String key = buildKey(row);

      BigDecimal metricValue = generateRandomBigDecimal(random, 5, 6);
      row.putValue(METRIC, metricValue.toString());

      if (bdMap.containsKey(key)) {
        bdMap.put(key, bdMap.get(key).add(metricValue));
        bdIndividualMap.get(key).add(metricValue);
      } else {
        bdMap.put(key, metricValue);
        ArrayList<BigDecimal> bdList = new ArrayList<>();
        bdList.add(metricValue);
        bdIndividualMap.put(key, bdList);
      }

      mutableSegmentImpl.index(row, defaultMetadata);
    }

    for (String key : bdMap.keySet()) {
      metrics.add(new Metric(key, bdMap.get(key)));
    }

    int numDocsIndexed = mutableSegmentImpl.getNumDocsIndexed();
    Assert.assertEquals(numDocsIndexed, bdMap.keySet().size());

    // Assert that aggregation happened.
    Assert.assertTrue(numDocsIndexed < NUM_ROWS);

    return metrics;
  }

  private List<List<Metric>> addRows(long seed, MutableSegmentImpl mutableSegmentImpl)
      throws Exception {
    List<List<Metric>> metrics = new ArrayList<>();
    Set<String> keys = new HashSet<>();


    Random random = new Random(seed);
    StreamMessageMetadata defaultMetadata = new StreamMessageMetadata(System.currentTimeMillis(), new GenericRow());

    for (int i = 0; i < NUM_ROWS; i++) {
      // Generate random int to prevent overflow
      GenericRow row = getRow(random, 1);
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

  @Test
  public void testSUMPRECISION() throws Exception {
    String m1 = "metric_SUMPRECISION";
    Schema schema = getSchemaBuilder().addMetric(m1, FieldSpec.DataType.BIG_DECIMAL).build();

    MutableSegmentImpl mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, new HashSet<>(Arrays.asList(m1)),
            VAR_LENGTH_SET, INVERTED_INDEX_SET,
            // Setting precision to 38 in the arguments for SUMPRECISION
            Arrays.asList(new AggregationConfig(m1, "SUMPRECISION(metric, 38)")));

    Map<String, BigDecimal> expected = new HashMap<>();
    List<Metric> metrics = addRowsSUMPRECISION(998, mutableSegmentImpl);
    for (Metric metric : metrics) {
      expected.put(metric.getKey(), (BigDecimal) metric.getValue());
    }

    /*
    Assert that the aggregated values are correct
     */
    GenericRow reuse = new GenericRow();
    for (int docId = 0; docId < expected.size(); docId++) {
      GenericRow row = mutableSegmentImpl.getRecord(docId, reuse);
      String key = buildKey(row);

      BigDecimal expectedBigDecimal = expected.get(key);
      BigDecimal actualBigDecimal = (BigDecimal) row.getValue(m1);

      Assert.assertEquals(actualBigDecimal, expectedBigDecimal,
          "The aggregated SUM does not match the expected SUM");
    }
    mutableSegmentImpl.destroy();
  }

  @Test
  public void testBigDecimalTooBig() {
    String m1 = "metric_SUMPRECISION";
    Schema schema = getSchemaBuilder().addMetric(m1, FieldSpec.DataType.BIG_DECIMAL).build();

    int seed = 1;
    Random random = new Random(seed);
    StreamMessageMetadata defaultMetadata = new StreamMessageMetadata(System.currentTimeMillis(), null);

    MutableSegmentImpl mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, new HashSet<>(Arrays.asList(m1)),
            VAR_LENGTH_SET, INVERTED_INDEX_SET,
            Arrays.asList(new AggregationConfig(m1, "SUMPRECISION(metric, 3)")));

    // Make a big decimal larger than 3 precision and try to index it
    BigDecimal large = BigDecimalUtils.generateMaximumNumberWithPrecision(5);
    GenericRow row = getRow(random, 1);

    row.putValue("metric", large);
    Assert.assertThrows(IllegalArgumentException.class, () -> {
      mutableSegmentImpl.index(row, defaultMetadata);
    });
  }

  private BigDecimal generateRandomBigDecimal(Random random, int maxPrecision, int scale) {
    int precision = 1 + random.nextInt(maxPrecision);

    String s = "";
    for (int i = 0; i < precision; i++) {
      s = s + (1 + random.nextInt(9));
    }

    if ((1 + random.nextInt(2)) == 1) {
      return (new BigDecimal(s).setScale(scale)).negate();
    } else {
      return new BigDecimal(s).setScale(scale);
    }
  }
}
