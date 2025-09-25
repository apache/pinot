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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
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
  public void testSVMap() {
    // Build a MAP field spec: key: STRING, value: INT
    Map<String, FieldSpec> children = new HashMap<>();
    children.put("key", new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true));
    children.put("value", new DimensionFieldSpec("value", FieldSpec.DataType.INT, true));
    ComplexFieldSpec mapSpec = new ComplexFieldSpec("col", FieldSpec.DataType.MAP, true, children);

    TableConfig tableConfig = new TableConfigBuilder(org.apache.pinot.spi.config.table.TableType.OFFLINE)
        .setSegmentPartitionConfig(new SegmentPartitionConfig(
            Collections.singletonMap("col", new ColumnPartitionConfig("murmur", 4))))
        .setTableName("testTable").build();
    Schema schema = new Schema();
    schema.addField(mapSpec);
    StatsCollectorConfig cfg = new StatsCollectorConfig(tableConfig, schema,
        tableConfig.getIndexingConfig().getSegmentPartitionConfig());

    Map<String, Object> m1 = new HashMap<>();
    m1.put("key1", 1);
    m1.put("largeKey1", 3);
    Map<String, Object> m2 = new HashMap<>();
    m2.put("key1", 2);
    m2.put("largeKey2", 4);
    Map<String, Object> m3 = new HashMap<>();
    m3.put("key1", 5);
    m3.put("largeKey2", 6);
    Map<String, Object> m4 = new HashMap<>();
    m4.put("key1", 7);
    m4.put("largeKey1", 8);

    NoDictColumnStatisticsCollector c = new NoDictColumnStatisticsCollector("col", cfg);
    c.collect(m1);
    c.collect(m2);
    c.collect(m3);
    c.collect(m4);
    c.seal();

    MapColumnPreIndexStatsCollector mapStats = new MapColumnPreIndexStatsCollector("col", cfg);
    mapStats.collect(m1);
    mapStats.collect(m2);
    mapStats.collect(m3);
    mapStats.collect(m4);
    mapStats.seal();

    assertEquals(c.getCardinality(), mapStats.getCardinality());
    assertEquals(c.getMinValue(), mapStats.getMinValue());
    assertEquals(c.getMaxValue(), mapStats.getMaxValue());
    assertEquals(c.getTotalNumberOfEntries(), mapStats.getTotalNumberOfEntries());
    assertEquals(c.getMaxNumberOfMultiValues(), mapStats.getMaxNumberOfMultiValues());
    assertEquals(c.isSorted(), mapStats.isSorted());
    assertEquals(c.getLengthOfShortestElement(), mapStats.getLengthOfShortestElement());
    assertEquals(c.getLengthOfLargestElement(), mapStats.getLengthOfLargestElement());
    assertEquals(c.getMaxRowLengthInBytes(), mapStats.getMaxRowLengthInBytes());
    assertEquals(c.getPartitions(), mapStats.getPartitions());
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
}
