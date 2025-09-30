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
package org.apache.pinot.segment.local.segment.creator.impl.stats;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class NoDictColumnStatisticsCollectorTest {

  private static StatsCollectorConfig newConfig(FieldSpec.DataType dataType, boolean isSingleValue) {
    TableConfig tableConfig = new TableConfigBuilder(org.apache.pinot.spi.config.table.TableType.OFFLINE)
        .setTableName("testTable")
        .setSegmentPartitionConfig(new SegmentPartitionConfig(
            Collections.singletonMap("col", new ColumnPartitionConfig("murmur", 4))))
        .build();
    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec("col", dataType, isSingleValue));

    return new StatsCollectorConfig(tableConfig, schema, tableConfig.getIndexingConfig().getSegmentPartitionConfig());
  }

  @DataProvider(name = "primitiveTypeTestData")
  public Object[][] primitiveTypeTestData() {
    return new Object[][] {
        // Ensure data has exactly 1 duplicate entry and total 4 entries

        // Sorted data
        {FieldSpec.DataType.INT, new Object[]{5, 5, 10, 20}, true},
        {FieldSpec.DataType.LONG, new Object[]{1L, 1L, 15L, 25L}, true},
        {FieldSpec.DataType.FLOAT, new Object[]{1.5f, 1.5f, 3.5f, 7.5f}, true},
        {FieldSpec.DataType.DOUBLE, new Object[]{2.5, 2.5, 4.5, 8.5}, true},

        // Unsorted data
        {FieldSpec.DataType.INT, new Object[]{10, 5, 20, 5}, false},
        {FieldSpec.DataType.LONG, new Object[]{15L, 1L, 25L, 1L}, false},
        {FieldSpec.DataType.FLOAT, new Object[]{3.5f, 1.5f, 7.5f, 1.5f}, false},
        {FieldSpec.DataType.DOUBLE, new Object[]{4.5, 2.5, 8.5, 2.5}, false}
    };
  }

  @DataProvider(name = "stringTypeTestData")
  public Object[][] stringTypeTestData() {
    return new Object[][] {
        // Ensure data has exactly 1 duplicate entry and total 4 entries
        // Sorted data
        {new String[]{"a", "a", "bbb", "ccc"}, true},
        // Unsorted data
        {new String[]{"bbb", "a", "ccc", "a"}, false}
    };
  }

  @DataProvider(name = "bytesTypeTestData")
  public Object[][] bytesTypeTestData() {
    return new Object[][] {
        // Ensure data has exactly 1 duplicate entry and total 4 entries
        // Sorted data
        {new byte[][]{new byte[]{1}, new byte[]{1}, new byte[]{2}, new byte[]{3}}, true},
        // Unsorted data
        {new byte[][]{new byte[]{2}, new byte[]{1}, new byte[]{1}, new byte[]{3}}, false}
    };
  }

  @DataProvider(name = "bigDecimalTypeTestData")
  public Object[][] bigDecimalTypeTestData() {
    return new Object[][] {
        // Ensure data has exactly 1 duplicate entry and total 4 entries
        // Sorted data
        {new BigDecimal[]{
            new BigDecimal("1.23"), new BigDecimal("1.23"), new BigDecimal("2.34"), new BigDecimal("9.99")}, true},
        // Unsorted data
        {new BigDecimal[]{
            new BigDecimal("2.34"), new BigDecimal("1.23"), new BigDecimal("9.99"), new BigDecimal("1.23")}, false}
    };
  }

  @Test(dataProvider = "primitiveTypeTestData")
  public void testSVPrimitiveTypes(FieldSpec.DataType dataType, Object[] entries, boolean isSorted) {
    NoDictColumnStatisticsCollector c = new NoDictColumnStatisticsCollector("col",
        newConfig(dataType, true));
    for (Object entry : entries) {
      c.collect(entry);
    }
    c.seal();

    AbstractColumnStatisticsCollector expectedStatsCollector = null;
    switch (dataType) {
      case INT:
        expectedStatsCollector = new IntColumnPreIndexStatsCollector("col", newConfig(dataType, true));
        break;
      case LONG:
        expectedStatsCollector = new LongColumnPreIndexStatsCollector("col", newConfig(dataType, true));
        break;
      case FLOAT:
        expectedStatsCollector = new FloatColumnPreIndexStatsCollector("col", newConfig(dataType, true));
        break;
      case DOUBLE:
        expectedStatsCollector = new DoubleColumnPreIndexStatsCollector("col", newConfig(dataType, true));
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
    for (Object entry : entries) {
      expectedStatsCollector.collect(entry);
    }
    expectedStatsCollector.seal();

    assertEquals(c.getCardinality(), 3);
    assertEquals(c.getMinValue(), expectedStatsCollector.getMinValue());
    assertEquals(c.getMaxValue(), expectedStatsCollector.getMaxValue());
    assertEquals(c.getTotalNumberOfEntries(), 4);
    assertEquals(c.getMaxNumberOfMultiValues(), 0);
    assertEquals(c.isSorted(), isSorted);
    assertEquals(c.getLengthOfShortestElement(), 8);
    assertEquals(c.getLengthOfLargestElement(), 8);
    assertEquals(c.getMaxRowLengthInBytes(), 8);
    assertEquals(c.getPartitions(), expectedStatsCollector.getPartitions());
  }

  @Test(dataProvider = "stringTypeTestData")
  public void testSVString(String[] entries, boolean isSorted) {
    NoDictColumnStatisticsCollector c = new NoDictColumnStatisticsCollector("col",
        newConfig(FieldSpec.DataType.STRING, true));
    for (String e : entries) {
      c.collect(e);
    }
    c.seal();

    StringColumnPreIndexStatsCollector stringStats = new StringColumnPreIndexStatsCollector("col",
        newConfig(FieldSpec.DataType.STRING, true));
    for (String e : entries) {
      stringStats.collect(e);
    }
    stringStats.seal();

    assertEquals(c.getCardinality(), 3);
    assertEquals(c.getMinValue(), "a");
    assertEquals(c.getMaxValue(), "ccc");
    assertEquals(c.getTotalNumberOfEntries(), entries.length);
    assertEquals(c.getMaxNumberOfMultiValues(), 0);
    assertEquals(c.isSorted(), isSorted);
    assertEquals(c.getLengthOfShortestElement(), 1);
    assertEquals(c.getLengthOfLargestElement(), 3);
    assertEquals(c.getMaxRowLengthInBytes(), 3);
    assertEquals(c.getPartitions(), stringStats.getPartitions());
  }

  @Test(dataProvider = "bytesTypeTestData")
  public void testSVBytes(byte[][] entries, boolean isSorted) {
    NoDictColumnStatisticsCollector c = new NoDictColumnStatisticsCollector("col",
        newConfig(FieldSpec.DataType.BYTES, true));
    for (byte[] e : entries) {
      c.collect(e);
    }
    c.seal();

    BytesColumnPredIndexStatsCollector bytesStats = new BytesColumnPredIndexStatsCollector("col",
        newConfig(FieldSpec.DataType.BYTES, true));
    for (byte[] e : entries) {
      bytesStats.collect(e);
    }
    bytesStats.seal();

    assertEquals(c.getCardinality(), bytesStats.getCardinality());
    assertEquals(c.getMinValue(), bytesStats.getMinValue());
    assertEquals(c.getMaxValue(), bytesStats.getMaxValue());
    assertEquals(c.getTotalNumberOfEntries(), entries.length);
    assertEquals(c.getMaxNumberOfMultiValues(), 0);
    assertEquals(c.isSorted(), isSorted);
    assertEquals(c.getLengthOfShortestElement(), bytesStats.getLengthOfShortestElement());
    assertEquals(c.getLengthOfLargestElement(), bytesStats.getLengthOfLargestElement());
    assertEquals(c.getMaxRowLengthInBytes(), bytesStats.getMaxRowLengthInBytes());
    assertEquals(c.getPartitions(), bytesStats.getPartitions());
  }

  @Test(dataProvider = "bigDecimalTypeTestData")
  public void testSVBigDecimal(BigDecimal[] entries, boolean isSorted) {
    NoDictColumnStatisticsCollector c = new NoDictColumnStatisticsCollector("col",
        newConfig(FieldSpec.DataType.BIG_DECIMAL, true));
    for (BigDecimal e : entries) {
      c.collect(e);
    }
    c.seal();

    BigDecimalColumnPreIndexStatsCollector bigDecimalStats = new BigDecimalColumnPreIndexStatsCollector("col",
        newConfig(FieldSpec.DataType.BIG_DECIMAL, true));
    for (BigDecimal e : entries) {
      bigDecimalStats.collect(e);
    }
    bigDecimalStats.seal();

    assertEquals(c.getCardinality(), 3);
    assertEquals(c.getMinValue(), bigDecimalStats.getMinValue());
    assertEquals(c.getMaxValue(), bigDecimalStats.getMaxValue());
    assertEquals(c.getTotalNumberOfEntries(), entries.length);
    assertEquals(c.getMaxNumberOfMultiValues(), 0);
    assertEquals(c.isSorted(), isSorted);
    assertEquals(c.getLengthOfShortestElement(), bigDecimalStats.getLengthOfShortestElement());
    assertEquals(c.getLengthOfLargestElement(), bigDecimalStats.getLengthOfLargestElement());
    assertEquals(c.getMaxRowLengthInBytes(), bigDecimalStats.getMaxRowLengthInBytes());
    assertEquals(c.getPartitions(), bigDecimalStats.getPartitions());
  }

  @DataProvider(name = "primitiveMVTypeTestData")
  public Object[][] primitiveMVTypeTestData() {
    return new Object[][] {
        // Two MV rows with one duplicate across total 4 values -> cardinality 3
        {FieldSpec.DataType.INT, new int[][]{new int[]{5, 10}, new int[]{5, 20}}},
        {FieldSpec.DataType.LONG, new long[][]{new long[]{1L, 15L}, new long[]{1L, 25L}}},
        {FieldSpec.DataType.FLOAT, new float[][]{new float[]{1.5f, 3.5f}, new float[]{1.5f, 7.5f}}},
        {FieldSpec.DataType.DOUBLE, new double[][]{new double[]{2.5, 4.5}, new double[]{2.5, 8.5}}}
    };
  }

  @Test(dataProvider = "primitiveMVTypeTestData")
  public void testMVPrimitiveTypes(FieldSpec.DataType dataType, Object entries) {
    // Validate MV behavior for numeric primitives using native arrays
    // - isSorted should be false for MV columns
    // - lengths are not tracked for native MV primitives (expect -1)
    // - maxNumberOfMultiValues should reflect per-row MV length
    NoDictColumnStatisticsCollector c;
    AbstractColumnStatisticsCollector expectedStatsCollector;

    c = new NoDictColumnStatisticsCollector("col", newConfig(dataType, false));

    if (entries instanceof int[][]) {
      expectedStatsCollector = new IntColumnPreIndexStatsCollector("col", newConfig(dataType, false));
      for (int[] row : (int[][]) entries) {
        c.collect(row);
        expectedStatsCollector.collect(row);
      }
    } else if (entries instanceof long[][]) {
      expectedStatsCollector = new LongColumnPreIndexStatsCollector("col", newConfig(dataType, false));
      for (long[] row : (long[][]) entries) {
        c.collect(row);
        expectedStatsCollector.collect(row);
      }
    } else if (entries instanceof float[][]) {
      expectedStatsCollector = new FloatColumnPreIndexStatsCollector("col", newConfig(dataType, false));
      for (float[] row : (float[][]) entries) {
        c.collect(row);
        expectedStatsCollector.collect(row);
      }
    } else {
      expectedStatsCollector = new DoubleColumnPreIndexStatsCollector("col", newConfig(dataType, false));
      for (double[] row : (double[][]) entries) {
        c.collect(row);
        expectedStatsCollector.collect(row);
      }
    }

    c.seal();
    expectedStatsCollector.seal();

    assertEquals(c.getCardinality(), expectedStatsCollector.getCardinality());
    assertEquals(c.getMinValue(), expectedStatsCollector.getMinValue());
    assertEquals(c.getMaxValue(), expectedStatsCollector.getMaxValue());
    assertEquals(c.getTotalNumberOfEntries(), 4);
    assertEquals(c.getMaxNumberOfMultiValues(), 2);
    assertFalse(c.isSorted());
    assertEquals(c.getLengthOfShortestElement(), expectedStatsCollector.getLengthOfShortestElement());
    assertEquals(c.getLengthOfLargestElement(), expectedStatsCollector.getLengthOfLargestElement());
    assertEquals(c.getMaxRowLengthInBytes(), expectedStatsCollector.getMaxRowLengthInBytes());
    assertEquals(c.getPartitions(), expectedStatsCollector.getPartitions());
  }

  @DataProvider(name = "stringMVTypeTestData")
  public Object[][] stringMVTypeTestData() {
    return new Object[][] {
        // Two MV rows with one duplicate across total 4 values -> cardinality 3
        {new String[][]{new String[]{"a", "bbb"}, new String[]{"a", "ccc"}}}
    };
  }

  @Test(dataProvider = "stringMVTypeTestData")
  public void testMVString(String[][] rows) {
    NoDictColumnStatisticsCollector c = new NoDictColumnStatisticsCollector("col",
        newConfig(FieldSpec.DataType.STRING, false));
    for (String[] r : rows) {
      c.collect(r);
    }
    c.seal();

    StringColumnPreIndexStatsCollector stringStats = new StringColumnPreIndexStatsCollector("col",
        newConfig(FieldSpec.DataType.STRING, false));
    for (String[] r : rows) {
      stringStats.collect(r);
    }
    stringStats.seal();

    assertEquals(c.getCardinality(), stringStats.getCardinality());
    assertEquals(c.getMinValue(), stringStats.getMinValue());
    assertEquals(c.getMaxValue(), stringStats.getMaxValue());
    assertEquals(c.getTotalNumberOfEntries(), 4);
    assertEquals(c.getMaxNumberOfMultiValues(), 2);
    assertFalse(c.isSorted());
    assertEquals(c.getLengthOfShortestElement(), 1);
    assertEquals(c.getLengthOfLargestElement(), 3);
    assertEquals(c.getMaxRowLengthInBytes(), 4);
    assertEquals(c.getPartitions(), stringStats.getPartitions());
  }

  @DataProvider(name = "bytesMVTypeTestData")
  public Object[][] bytesMVTypeTestData() {
    return new Object[][] {
        // Two MV rows with one duplicate across total 4 values
        {new byte[][][]{new byte[][]{new byte[]{1}, new byte[]{2}}, new byte[][]{new byte[]{1}, new byte[]{3}}}}
    };
  }

  @Test(dataProvider = "bytesMVTypeTestData")
  public void testMVBytes(byte[][][] rows) {
    NoDictColumnStatisticsCollector c = new NoDictColumnStatisticsCollector("col",
        newConfig(FieldSpec.DataType.BYTES, false));
    for (byte[][] r : rows) {
      c.collect(r);
    }
    c.seal();

    BytesColumnPredIndexStatsCollector bytesStats = new BytesColumnPredIndexStatsCollector("col",
        newConfig(FieldSpec.DataType.BYTES, false));
    for (byte[][] r : rows) {
      bytesStats.collect(r);
    }
    bytesStats.seal();

    assertEquals(c.getCardinality(), bytesStats.getCardinality());
    assertEquals(c.getMinValue(), bytesStats.getMinValue());
    assertEquals(c.getMaxValue(), bytesStats.getMaxValue());
    assertEquals(c.getTotalNumberOfEntries(), 4);
    assertEquals(c.getMaxNumberOfMultiValues(), 2);
    assertFalse(c.isSorted());
    assertEquals(c.getLengthOfShortestElement(), bytesStats.getLengthOfShortestElement());
    assertEquals(c.getLengthOfLargestElement(), bytesStats.getLengthOfLargestElement());
    assertEquals(c.getMaxRowLengthInBytes(), bytesStats.getMaxRowLengthInBytes());
    assertEquals(c.getPartitions(), bytesStats.getPartitions());
  }

  @Test
  public void testMVBigDecimal()
      throws Exception {
    NoDictColumnStatisticsCollector c = new NoDictColumnStatisticsCollector("col",
        newConfig(FieldSpec.DataType.BIG_DECIMAL, false));
    try {
      c.collect(new Object[] {new BigDecimal("1.1"), new BigDecimal("2.2")});
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
      // expected: BigDecimal MV not supported by NoDict collector by design
    }
  }

  // A test that picks a random data type, generates random data for that type (sorted or unsorted,
  // single or multi value, high or low or medium cardinality), collects stats using NoDictColumnStatisticsCollector
  // and the corresponding PreIndexStatsCollector, and compares the stats.
  // Runs this setup multiple times to cover different scenarios.
  @Test
  public void testRandomizedDataComparison() throws Exception {
    Random random = new Random();

    // Run multiple iterations with different random scenarios
    for (int iteration = 0; iteration < 100; iteration++) {
      runRandomizedTest(random, iteration);
    }
  }

  private void runRandomizedTest(Random random, int iteration) throws Exception {
    // Randomly select data type (excluding BIG_DECIMAL for MV as it's unsupported)
    FieldSpec.DataType[] supportedTypes = {
        FieldSpec.DataType.INT, FieldSpec.DataType.LONG, FieldSpec.DataType.FLOAT,
        FieldSpec.DataType.DOUBLE, FieldSpec.DataType.STRING, FieldSpec.DataType.BYTES,
        FieldSpec.DataType.BIG_DECIMAL
    };
    FieldSpec.DataType dataType = supportedTypes[random.nextInt(supportedTypes.length)];

    // Randomly choose single vs multi-value (skip MV for BIG_DECIMAL)
    boolean isSingleValue = dataType == FieldSpec.DataType.BIG_DECIMAL || random.nextBoolean();

    // Randomly choose cardinality level
    CardinalityLevel cardinalityLevel = CardinalityLevel.values()[random.nextInt(CardinalityLevel.values().length)];

    // Randomly choose if data should be sorted
    boolean shouldBeSorted = random.nextBoolean();

    try {
      runTestForConfiguration(dataType, isSingleValue, cardinalityLevel, shouldBeSorted, random, iteration);
    } catch (Exception e) {
      throw new RuntimeException(String.format(
          "Test failed for iteration %d: dataType=%s, isSingleValue=%s, cardinality=%s, sorted=%s",
          iteration, dataType, isSingleValue, cardinalityLevel, shouldBeSorted), e);
    }
  }

  private enum CardinalityLevel {
    LOW(1, 20),      // 5-20 unique values
    MEDIUM(50, 100), // 50-100 unique values
    HIGH(200, 500);  // 200-500 unique values

    private final int minUnique;
    private final int maxUnique;

    CardinalityLevel(int minUnique, int maxUnique) {
      this.minUnique = minUnique;
      this.maxUnique = maxUnique;
    }

    public int getUniqueCount(Random random) {
      return minUnique + random.nextInt(maxUnique - minUnique + 1);
    }
  }

  private void runTestForConfiguration(FieldSpec.DataType dataType, boolean isSingleValue,
      CardinalityLevel cardinalityLevel, boolean shouldBeSorted, Random random, int iteration) throws Exception {

    int uniqueValueCount = cardinalityLevel.getUniqueCount(random);
    int totalEntries = uniqueValueCount + random.nextInt(uniqueValueCount * 2); // Add some duplicates

    // Generate test data
    Object[] testData = generateRandomData(dataType, isSingleValue, uniqueValueCount, totalEntries, shouldBeSorted, random);

    // Skip if we can't generate valid test data
    if (testData == null) {
      return;
    }

    // Create collectors
    NoDictColumnStatisticsCollector noDictCollector =
        new NoDictColumnStatisticsCollector("col", newConfig(dataType, isSingleValue));
    AbstractColumnStatisticsCollector expectedCollector =
        createExpectedCollector(dataType, isSingleValue);

    // Collect stats from both collectors
    for (Object entry : testData) {
      noDictCollector.collect(entry);
      expectedCollector.collect(entry);
    }

    noDictCollector.seal();
    expectedCollector.seal();

    // Compare all stats
    compareCollectorStats(noDictCollector, expectedCollector, dataType, isSingleValue, iteration, testData);
  }

  private Object[] generateRandomData(FieldSpec.DataType dataType, boolean isSingleValue,
      int uniqueValueCount, int totalEntries, boolean shouldBeSorted, Random random) {

    try {
      switch (dataType) {
        case INT:
          return generateIntData(isSingleValue, uniqueValueCount, totalEntries, shouldBeSorted, random);
        case LONG:
          return generateLongData(isSingleValue, uniqueValueCount, totalEntries, shouldBeSorted, random);
        case FLOAT:
          return generateFloatData(isSingleValue, uniqueValueCount, totalEntries, shouldBeSorted, random);
        case DOUBLE:
          return generateDoubleData(isSingleValue, uniqueValueCount, totalEntries, shouldBeSorted, random);
        case STRING:
          return generateStringData(isSingleValue, uniqueValueCount, totalEntries, shouldBeSorted, random);
        case BYTES:
          return generateBytesData(isSingleValue, uniqueValueCount, totalEntries, shouldBeSorted, random);
        case BIG_DECIMAL:
          return generateBigDecimalData(isSingleValue, uniqueValueCount, totalEntries, shouldBeSorted, random);
        default:
          return null;
      }
    } catch (Exception e) {
      // Return null for unsupported combinations
      return null;
    }
  }

  private Object[] generateIntData(boolean isSingleValue, int uniqueValueCount, int totalEntries,
      boolean shouldBeSorted, Random random) {
    Set<Integer> uniqueValues = new HashSet<>();
    while (uniqueValues.size() < uniqueValueCount) {
      uniqueValues.add(random.nextInt(10000));
    }
    List<Integer> values = new ArrayList<>(uniqueValues);

    if (isSingleValue) {
      return generateSingleValueData(values, totalEntries, shouldBeSorted, random).toArray();
    } else {
      return generateMultiValueIntData(values, totalEntries, random);
    }
  }

  private Object[] generateLongData(boolean isSingleValue, int uniqueValueCount, int totalEntries,
      boolean shouldBeSorted, Random random) {
    Set<Long> uniqueValues = new HashSet<>();
    while (uniqueValues.size() < uniqueValueCount) {
      uniqueValues.add(random.nextLong() % 100000L);
    }
    List<Long> values = new ArrayList<>(uniqueValues);

    if (isSingleValue) {
      return generateSingleValueData(values, totalEntries, shouldBeSorted, random).toArray();
    } else {
      return generateMultiValueLongData(values, totalEntries, random);
    }
  }

  private Object[] generateFloatData(boolean isSingleValue, int uniqueValueCount, int totalEntries,
      boolean shouldBeSorted, Random random) {
    Set<Float> uniqueValues = new HashSet<>();
    while (uniqueValues.size() < uniqueValueCount) {
      uniqueValues.add(random.nextFloat() * 1000);
    }
    List<Float> values = new ArrayList<>(uniqueValues);

    if (isSingleValue) {
      return generateSingleValueData(values, totalEntries, shouldBeSorted, random).toArray();
    } else {
      return generateMultiValueFloatData(values, totalEntries, random);
    }
  }

  private Object[] generateDoubleData(boolean isSingleValue, int uniqueValueCount, int totalEntries,
      boolean shouldBeSorted, Random random) {
    Set<Double> uniqueValues = new HashSet<>();
    while (uniqueValues.size() < uniqueValueCount) {
      uniqueValues.add(random.nextDouble() * 1000);
    }
    List<Double> values = new ArrayList<>(uniqueValues);

    if (isSingleValue) {
      return generateSingleValueData(values, totalEntries, shouldBeSorted, random).toArray();
    } else {
      return generateMultiValueDoubleData(values, totalEntries, random);
    }
  }

  private Object[] generateStringData(boolean isSingleValue, int uniqueValueCount, int totalEntries,
      boolean shouldBeSorted, Random random) {
    Set<String> uniqueValues = new HashSet<>();
    while (uniqueValues.size() < uniqueValueCount) {
      uniqueValues.add(generateRandomString(random));
    }
    List<String> values = new ArrayList<>(uniqueValues);

    if (isSingleValue) {
      return generateSingleValueData(values, totalEntries, shouldBeSorted, random).toArray();
    } else {
      return generateMultiValueStringData(values, totalEntries, random);
    }
  }

  private Object[] generateBytesData(boolean isSingleValue, int uniqueValueCount, int totalEntries,
      boolean shouldBeSorted, Random random) {
    List<byte[]> uniqueValues = new ArrayList<>();
    Set<String> seen = new HashSet<>(); // Use string representation to avoid duplicate byte arrays
    while (uniqueValues.size() < uniqueValueCount) {
      byte[] bytes = generateRandomBytes(random);
      String key = Arrays.toString(bytes);
      if (!seen.contains(key)) {
        uniqueValues.add(bytes);
        seen.add(key);
      }
    }

    if (isSingleValue) {
      return generateSingleValueByteArrayData(uniqueValues, totalEntries, shouldBeSorted, random);
    } else {
      return generateMultiValueBytesData(uniqueValues, totalEntries, random);
    }
  }

  private Object[] generateBigDecimalData(boolean isSingleValue, int uniqueValueCount, int totalEntries,
      boolean shouldBeSorted, Random random) {
    if (!isSingleValue) {
      throw new UnsupportedOperationException("BigDecimal MV not supported");
    }

    Set<BigDecimal> uniqueValues = new HashSet<>();
    while (uniqueValues.size() < uniqueValueCount) {
      uniqueValues.add(new BigDecimal(random.nextDouble() * 1000).setScale(2, BigDecimal.ROUND_HALF_UP));
    }
    List<BigDecimal> values = new ArrayList<>(uniqueValues);

    return generateSingleValueData(values, totalEntries, shouldBeSorted, random).toArray();
  }

  private <T extends Comparable<T>> List<T> generateSingleValueData(List<T> uniqueValues,
      int totalEntries, boolean shouldBeSorted, Random random) {
    List<T> result = new ArrayList<>();
    for (int i = 0; i < totalEntries; i++) {
      result.add(uniqueValues.get(random.nextInt(uniqueValues.size())));
    }

    if (shouldBeSorted) {
      Collections.sort(result);
    }

    return result;
  }

  private Object[] generateSingleValueByteArrayData(List<byte[]> uniqueValues,
      int totalEntries, boolean shouldBeSorted, Random random) {
    List<byte[]> result = new ArrayList<>();
    for (int i = 0; i < totalEntries; i++) {
      result.add(uniqueValues.get(random.nextInt(uniqueValues.size())));
    }

    if (shouldBeSorted) {
      // sort result by comparing ByteArray representation of each entry
      result.sort((a, b) -> {
        ByteArray a1 = new ByteArray(a);
        ByteArray b1 = new ByteArray(b);
        return a1.compareTo(b1);
      });
    }

    return result.toArray();
  }

  private Object[] generateMultiValueIntData(List<Integer> values, int totalEntries, Random random) {
    List<int[]> result = new ArrayList<>();
    int entriesAdded = 0;
    while (entriesAdded < totalEntries) {
      int mvSize = 1 + random.nextInt(3); // 1-3 values per MV entry
      int[] mvEntry = new int[Math.min(mvSize, totalEntries - entriesAdded)];
      for (int i = 0; i < mvEntry.length; i++) {
        mvEntry[i] = values.get(random.nextInt(values.size()));
      }
      result.add(mvEntry);
      entriesAdded += mvEntry.length;
    }
    return result.toArray();
  }

  private Object[] generateMultiValueLongData(List<Long> values, int totalEntries, Random random) {
    List<long[]> result = new ArrayList<>();
    int entriesAdded = 0;
    while (entriesAdded < totalEntries) {
      int mvSize = 1 + random.nextInt(3);
      long[] mvEntry = new long[Math.min(mvSize, totalEntries - entriesAdded)];
      for (int i = 0; i < mvEntry.length; i++) {
        mvEntry[i] = values.get(random.nextInt(values.size()));
      }
      result.add(mvEntry);
      entriesAdded += mvEntry.length;
    }
    return result.toArray();
  }

  private Object[] generateMultiValueFloatData(List<Float> values, int totalEntries, Random random) {
    List<float[]> result = new ArrayList<>();
    int entriesAdded = 0;
    while (entriesAdded < totalEntries) {
      int mvSize = 1 + random.nextInt(3);
      float[] mvEntry = new float[Math.min(mvSize, totalEntries - entriesAdded)];
      for (int i = 0; i < mvEntry.length; i++) {
        mvEntry[i] = values.get(random.nextInt(values.size()));
      }
      result.add(mvEntry);
      entriesAdded += mvEntry.length;
    }
    return result.toArray();
  }

  private Object[] generateMultiValueDoubleData(List<Double> values, int totalEntries, Random random) {
    List<double[]> result = new ArrayList<>();
    int entriesAdded = 0;
    while (entriesAdded < totalEntries) {
      int mvSize = 1 + random.nextInt(3);
      double[] mvEntry = new double[Math.min(mvSize, totalEntries - entriesAdded)];
      for (int i = 0; i < mvEntry.length; i++) {
        mvEntry[i] = values.get(random.nextInt(values.size()));
      }
      result.add(mvEntry);
      entriesAdded += mvEntry.length;
    }
    return result.toArray();
  }

  private Object[] generateMultiValueStringData(List<String> values, int totalEntries, Random random) {
    List<String[]> result = new ArrayList<>();
    int entriesAdded = 0;
    while (entriesAdded < totalEntries) {
      int mvSize = 1 + random.nextInt(3);
      String[] mvEntry = new String[Math.min(mvSize, totalEntries - entriesAdded)];
      for (int i = 0; i < mvEntry.length; i++) {
        mvEntry[i] = values.get(random.nextInt(values.size()));
      }
      result.add(mvEntry);
      entriesAdded += mvEntry.length;
    }
    return result.toArray();
  }

  private Object[] generateMultiValueBytesData(List<byte[]> values, int totalEntries, Random random) {
    List<byte[][]> result = new ArrayList<>();
    int entriesAdded = 0;
    while (entriesAdded < totalEntries) {
      int mvSize = 1 + random.nextInt(3);
      byte[][] mvEntry = new byte[Math.min(mvSize, totalEntries - entriesAdded)][];
      for (int i = 0; i < mvEntry.length; i++) {
        mvEntry[i] = values.get(random.nextInt(values.size()));
      }
      result.add(mvEntry);
      entriesAdded += mvEntry.length;
    }
    return result.toArray();
  }

  private String generateRandomString(Random random) {
    int length = 1 + random.nextInt(10); // 1-10 characters
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      sb.append((char) ('a' + random.nextInt(26)));
    }
    return sb.toString();
  }

  private byte[] generateRandomBytes(Random random) {
    int length = 1 + random.nextInt(5); // 1-5 bytes
    byte[] bytes = new byte[length];
    random.nextBytes(bytes);
    return bytes;
  }

  private AbstractColumnStatisticsCollector createExpectedCollector(FieldSpec.DataType dataType, boolean isSingleValue) {
    switch (dataType) {
      case INT:
        return new IntColumnPreIndexStatsCollector("col", newConfig(dataType, isSingleValue));
      case LONG:
        return new LongColumnPreIndexStatsCollector("col", newConfig(dataType, isSingleValue));
      case FLOAT:
        return new FloatColumnPreIndexStatsCollector("col", newConfig(dataType, isSingleValue));
      case DOUBLE:
        return new DoubleColumnPreIndexStatsCollector("col", newConfig(dataType, isSingleValue));
      case STRING:
        return new StringColumnPreIndexStatsCollector("col", newConfig(dataType, isSingleValue));
      case BYTES:
        return new BytesColumnPredIndexStatsCollector("col", newConfig(dataType, isSingleValue));
      case BIG_DECIMAL:
        return new BigDecimalColumnPreIndexStatsCollector("col", newConfig(dataType, isSingleValue));
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  private void compareCollectorStats(NoDictColumnStatisticsCollector noDictCollector,
      AbstractColumnStatisticsCollector expectedCollector, FieldSpec.DataType dataType,
      boolean isSingleValue, int iteration, Object[] testData) {

    String context = String.format("Iteration %d, DataType: %s, SingleValue: %s, Data: %s",
        iteration, dataType, isSingleValue, Arrays.deepToString(testData));

    assertTrue(noDictCollector.getCardinality() >= expectedCollector.getCardinality(),
        "Approx Cardinality " + noDictCollector.getCardinality() + " is lower than actual cardinality "
            + expectedCollector.getCardinality() + " for " + context);
    assertEquals(noDictCollector.getMinValue(), expectedCollector.getMinValue(),
        "MinValue mismatch - " + context);
    assertEquals(noDictCollector.getMaxValue(), expectedCollector.getMaxValue(),
        "MaxValue mismatch - " + context);
    assertEquals(noDictCollector.getTotalNumberOfEntries(), expectedCollector.getTotalNumberOfEntries(),
        "TotalNumberOfEntries mismatch - " + context);
    assertEquals(noDictCollector.getMaxNumberOfMultiValues(), expectedCollector.getMaxNumberOfMultiValues(),
        "MaxNumberOfMultiValues mismatch - " + context);
    assertEquals(noDictCollector.isSorted(), expectedCollector.isSorted(),
        "isSorted mismatch - " + context);
    if (dataType != FieldSpec.DataType.INT && dataType != FieldSpec.DataType.LONG && dataType != FieldSpec.DataType.FLOAT
        && dataType != FieldSpec.DataType.DOUBLE) {
      if (dataType != FieldSpec.DataType.STRING) {
        // StringCollector currently does not return shortest element length
        assertEquals(noDictCollector.getLengthOfShortestElement(), expectedCollector.getLengthOfShortestElement(),
            "LengthOfShortestElement mismatch - " + context);
      }
      assertEquals(noDictCollector.getLengthOfLargestElement(), expectedCollector.getLengthOfLargestElement(),
          "LengthOfLargestElement mismatch - " + context);
      assertEquals(noDictCollector.getMaxRowLengthInBytes(), expectedCollector.getMaxRowLengthInBytes(),
          "MaxRowLengthInBytes mismatch - " + context);
    }
    assertEquals(noDictCollector.getPartitions(), expectedCollector.getPartitions(),
        "Partitions mismatch - " + context);
  }

}
