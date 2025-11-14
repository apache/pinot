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


/**
 * Comprehensive tests for AbstractColumnStatisticsCollector implementations.
 * Tests all data types with both primitive and Object collect methods,
 * single and multi-value scenarios, edge cases, and partition functionality.
 */
public class AbstractColumnStatisticsCollectorTest {

  private static final String COLUMN_NAME = "testColumn";
  private static final String TABLE_NAME = "testTable";
  private static final int NUM_PARTITIONS = 4;

  // Test helper methods

  /**
   * Creates a StatsCollectorConfig with optional partitioning.
   */
  private StatsCollectorConfig createStatsCollectorConfig(FieldSpec.DataType dataType, boolean isSingleValue,
      boolean withPartitioning) {
    TableConfigBuilder builder = new TableConfigBuilder(org.apache.pinot.spi.config.table.TableType.OFFLINE)
        .setTableName(TABLE_NAME);

    if (withPartitioning) {
      builder.setSegmentPartitionConfig(new SegmentPartitionConfig(
          Collections.singletonMap(COLUMN_NAME, new ColumnPartitionConfig("Murmur", NUM_PARTITIONS))));
    }

    TableConfig tableConfig = builder.build();
    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(COLUMN_NAME, dataType, isSingleValue));

    return new StatsCollectorConfig(tableConfig, schema,
        withPartitioning ? tableConfig.getIndexingConfig().getSegmentPartitionConfig() : null);
  }

  /**
   * Factory method to create the appropriate collector for a given data type.
   */
  private AbstractColumnStatisticsCollector createCollector(FieldSpec.DataType dataType, boolean isSingleValue,
      boolean withPartitioning) {
    StatsCollectorConfig config = createStatsCollectorConfig(dataType, isSingleValue, withPartitioning);
    switch (dataType) {
      case INT:
        return new IntColumnPreIndexStatsCollector(COLUMN_NAME, config);
      case LONG:
        return new LongColumnPreIndexStatsCollector(COLUMN_NAME, config);
      case FLOAT:
        return new FloatColumnPreIndexStatsCollector(COLUMN_NAME, config);
      case DOUBLE:
        return new DoubleColumnPreIndexStatsCollector(COLUMN_NAME, config);
      case STRING:
        return new StringColumnPreIndexStatsCollector(COLUMN_NAME, config);
      case BYTES:
        return new BytesColumnPredIndexStatsCollector(COLUMN_NAME, config);
      case BIG_DECIMAL:
        return new BigDecimalColumnPreIndexStatsCollector(COLUMN_NAME, config);
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  /**
   * Helper method to assert basic statistics after collection.
   */
  private void assertBasicStats(AbstractColumnStatisticsCollector collector, int expectedCardinality,
      int expectedTotalEntries, Object expectedMin, Object expectedMax, boolean expectedSorted) {
    assertEquals(collector.getCardinality(), expectedCardinality);
    assertEquals(collector.getTotalNumberOfEntries(), expectedTotalEntries);
    assertEquals(collector.isSorted(), expectedSorted);

    if (expectedMin != null && expectedMax != null) {
      assertEquals(collector.getMinValue(), expectedMin);
      assertEquals(collector.getMaxValue(), expectedMax);
    }
  }

  // DataProviders

  @DataProvider(name = "allDataTypes")
  public Object[][] allDataTypes() {
    return new Object[][] {
        {FieldSpec.DataType.INT},
        {FieldSpec.DataType.LONG},
        {FieldSpec.DataType.FLOAT},
        {FieldSpec.DataType.DOUBLE},
        {FieldSpec.DataType.STRING},
        {FieldSpec.DataType.BYTES},
        {FieldSpec.DataType.BIG_DECIMAL}
    };
  }

  @DataProvider(name = "primitiveDataTypes")
  public Object[][] primitiveDataTypes() {
    return new Object[][] {
        {FieldSpec.DataType.INT},
        {FieldSpec.DataType.LONG},
        {FieldSpec.DataType.FLOAT},
        {FieldSpec.DataType.DOUBLE}
    };
  }

  @DataProvider(name = "numericDataTypes")
  public Object[][] numericDataTypes() {
    return new Object[][] {
        {FieldSpec.DataType.INT},
        {FieldSpec.DataType.LONG},
        {FieldSpec.DataType.FLOAT},
        {FieldSpec.DataType.DOUBLE},
        {FieldSpec.DataType.BIG_DECIMAL}
    };
  }

  @DataProvider(name = "multiValueSupportedTypes")
  public Object[][] multiValueSupportedTypes() {
    return new Object[][] {
        {FieldSpec.DataType.INT},
        {FieldSpec.DataType.LONG},
        {FieldSpec.DataType.FLOAT},
        {FieldSpec.DataType.DOUBLE},
        {FieldSpec.DataType.STRING},
        {FieldSpec.DataType.BYTES}
    };
  }

  @DataProvider(name = "lengthTrackingTypes")
  public Object[][] lengthTrackingTypes() {
    return new Object[][] {
        {FieldSpec.DataType.STRING},
        {FieldSpec.DataType.BYTES},
        {FieldSpec.DataType.BIG_DECIMAL}
    };
  }

  // Test 1: Single-Value Collection using Primitive Methods

  @Test(dataProvider = "primitiveDataTypes")
  public void testSingleValuePrimitiveCollectionSorted(FieldSpec.DataType dataType) {
    AbstractColumnStatisticsCollector collector = createCollector(dataType, true, false);

    // Collect sorted values
    switch (dataType) {
      case INT:
        collector.collect(1);
        collector.collect(5);
        collector.collect(5);  // duplicate
        collector.collect(10);
        break;
      case LONG:
        collector.collect(1L);
        collector.collect(5L);
        collector.collect(5L);  // duplicate
        collector.collect(10L);
        break;
      case FLOAT:
        collector.collect(1.5f);
        collector.collect(5.5f);
        collector.collect(5.5f);  // duplicate
        collector.collect(10.5f);
        break;
      case DOUBLE:
        collector.collect(1.5);
        collector.collect(5.5);
        collector.collect(5.5);  // duplicate
        collector.collect(10.5);
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }

    collector.seal();

    // Verify stats
    assertBasicStats(collector, 3, 4, getMinValue(dataType), getMaxValue(dataType), true);

    // Verify unique values set
    Object uniqueValuesSet = collector.getUniqueValuesSet();
    assertNotNull(uniqueValuesSet);
    assertEquals(getArrayLength(uniqueValuesSet), 3);
  }

  @Test(dataProvider = "primitiveDataTypes")
  public void testSingleValuePrimitiveCollectionUnsorted(FieldSpec.DataType dataType) {
    AbstractColumnStatisticsCollector collector = createCollector(dataType, true, false);

    // Collect unsorted values
    switch (dataType) {
      case INT:
        collector.collect(10);
        collector.collect(1);
        collector.collect(5);
        collector.collect(5);  // duplicate
        break;
      case LONG:
        collector.collect(10L);
        collector.collect(1L);
        collector.collect(5L);
        collector.collect(5L);  // duplicate
        break;
      case FLOAT:
        collector.collect(10.5f);
        collector.collect(1.5f);
        collector.collect(5.5f);
        collector.collect(5.5f);  // duplicate
        break;
      case DOUBLE:
        collector.collect(10.5);
        collector.collect(1.5);
        collector.collect(5.5);
        collector.collect(5.5);  // duplicate
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }

    collector.seal();

    // Verify stats - should not be sorted
    assertBasicStats(collector, 3, 4, getMinValue(dataType), getMaxValue(dataType), false);
  }

  // Test 2: Single-Value Collection using collect(Object)

  @Test(dataProvider = "allDataTypes")
  public void testSingleValueObjectCollection(FieldSpec.DataType dataType) {
    AbstractColumnStatisticsCollector collector = createCollector(dataType, true, false);

    // Collect values using Object method
    Object[] testData = getTestDataForType(dataType, true);
    for (Object value : testData) {
      collector.collect(value);
    }

    collector.seal();

    // Verify stats
    int expectedCardinality = 3;  // test data has 1 duplicate
    int expectedTotalEntries = 4;
    assertBasicStats(collector, expectedCardinality, expectedTotalEntries,
        getExpectedMinForType(dataType), getExpectedMaxForType(dataType), true);
  }

  // Test 3: Multi-Value Collection using Primitive Arrays

  @Test(dataProvider = "multiValueSupportedTypes")
  public void testMultiValuePrimitiveArrayCollection(FieldSpec.DataType dataType) {
    AbstractColumnStatisticsCollector collector = createCollector(dataType, false, false);

    // Collect multi-value entries
    switch (dataType) {
      case INT:
        collector.collect(new int[]{1, 2});
        collector.collect(new int[]{3, 4, 5});
        break;
      case LONG:
        collector.collect(new long[]{1L, 2L});
        collector.collect(new long[]{3L, 4L, 5L});
        break;
      case FLOAT:
        collector.collect(new float[]{1.5f, 2.5f});
        collector.collect(new float[]{3.5f, 4.5f, 5.5f});
        break;
      case DOUBLE:
        collector.collect(new double[]{1.5, 2.5});
        collector.collect(new double[]{3.5, 4.5, 5.5});
        break;
      case STRING:
        collector.collect(new String[]{"a", "b"});
        collector.collect(new String[]{"c", "d", "e"});
        break;
      case BYTES:
        collector.collect(new byte[][]{{1}, {2}});
        collector.collect(new byte[][]{{3}, {4}, {5}});
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }

    collector.seal();

    // Verify stats
    assertEquals(collector.getCardinality(), 5);
    assertEquals(collector.getTotalNumberOfEntries(), 5);
    assertEquals(collector.getMaxNumberOfMultiValues(), 3);
    assertFalse(collector.isSorted());  // multi-value is never sorted
  }

  @Test(dataProvider = "multiValueSupportedTypes")
  public void testMultiValueObjectArrayCollection(FieldSpec.DataType dataType) {
    AbstractColumnStatisticsCollector collector = createCollector(dataType, false, false);

    // Collect multi-value entries using Object[]
    Object[][] testData = getMultiValueTestData(dataType);
    for (Object[] values : testData) {
      collector.collect((Object) values);
    }

    collector.seal();

    // Verify stats
    assertEquals(collector.getCardinality(), 5);
    assertEquals(collector.getTotalNumberOfEntries(), 5);
    assertEquals(collector.getMaxNumberOfMultiValues(), 3);
    assertFalse(collector.isSorted());
  }

  // Test 4: Edge Cases

  @Test(dataProvider = "allDataTypes")
  public void testEmptyCollection(FieldSpec.DataType dataType) {
    AbstractColumnStatisticsCollector collector = createCollector(dataType, true, false);

    // Seal without collecting any data
    collector.seal();

    // Verify stats
    assertEquals(collector.getCardinality(), 0);
    assertEquals(collector.getTotalNumberOfEntries(), 0);
    assertTrue(collector.isSorted());  // Empty is considered sorted
  }

  @Test(dataProvider = "allDataTypes")
  public void testSingleValue(FieldSpec.DataType dataType) {
    AbstractColumnStatisticsCollector collector = createCollector(dataType, true, false);

    // Collect single value
    Object singleValue = getSingleValueForType(dataType);
    collector.collect(singleValue);
    collector.seal();

    // Verify stats - for BYTES, need to use ByteArray for comparison
    Object expectedValue = dataType == FieldSpec.DataType.BYTES
        ? new ByteArray((byte[]) singleValue)
        : singleValue;
    assertBasicStats(collector, 1, 1, expectedValue, expectedValue, true);
  }

  @Test(dataProvider = "allDataTypes")
  public void testAllDuplicates(FieldSpec.DataType dataType) {
    AbstractColumnStatisticsCollector collector = createCollector(dataType, true, false);

    // Collect same value multiple times
    Object value = getSingleValueForType(dataType);
    for (int i = 0; i < 10; i++) {
      collector.collect(value);
    }
    collector.seal();

    // Verify stats - for BYTES, need to use ByteArray for comparison
    Object expectedValue = dataType == FieldSpec.DataType.BYTES
        ? new ByteArray((byte[]) value)
        : value;
    assertBasicStats(collector, 1, 10, expectedValue, expectedValue, true);
  }

  @Test(dataProvider = "allDataTypes", expectedExceptions = IllegalStateException.class)
  public void testUnsealedAccessThrowsException(FieldSpec.DataType dataType) {
    AbstractColumnStatisticsCollector collector = createCollector(dataType, true, false);

    // Try to access stats before sealing - should throw
    collector.getMinValue();
  }

  // Test 5: Partition Function Tests

  @Test(dataProvider = "allDataTypes")
  public void testPartitionFunctionEnabled(FieldSpec.DataType dataType) {
    if (dataType == FieldSpec.DataType.BIG_DECIMAL) {
      // Skip BigDecimal as partition testing is less relevant
      return;
    }

    AbstractColumnStatisticsCollector collector = createCollector(dataType, true, true);

    // Collect values
    Object[] testData = getTestDataForType(dataType, true);
    for (Object value : testData) {
      collector.collect(value);
    }
    collector.seal();

    // Verify partition function is present
    assertNotNull(collector.getPartitionFunction());
    assertEquals(collector.getNumPartitions(), NUM_PARTITIONS);

    // Verify partitions are populated
    Set<Integer> partitions = collector.getPartitions();
    assertNotNull(partitions);
    assertTrue(partitions.size() > 0);
    assertTrue(partitions.size() <= NUM_PARTITIONS);

    // All partition values should be within range
    for (Integer partition : partitions) {
      assertTrue(partition >= 0 && partition < NUM_PARTITIONS);
    }
  }

  @Test(dataProvider = "allDataTypes")
  public void testPartitionFunctionDisabled(FieldSpec.DataType dataType) {
    AbstractColumnStatisticsCollector collector = createCollector(dataType, true, false);

    // Collect values
    Object[] testData = getTestDataForType(dataType, true);
    for (Object value : testData) {
      collector.collect(value);
    }
    collector.seal();

    // Verify partition function is not present
    assertNull(collector.getPartitionFunction());
    assertNull(collector.getPartitions());
    assertEquals(collector.getNumPartitions(), -1);
  }

  // Test 6: Type-Specific Tests

  @Test
  public void testStringLengthTracking() {
    AbstractColumnStatisticsCollector collector = createCollector(FieldSpec.DataType.STRING, true, false);

    collector.collect("a");          // length 1
    collector.collect("xyz");        // length 3
    collector.collect("hello");      // length 5

    collector.seal();

    assertEquals(collector.getLengthOfLargestElement(), 5);
    assertEquals(collector.getMaxRowLengthInBytes(), 5);
  }

  @Test
  public void testStringMultiValueLengthTracking() {
    AbstractColumnStatisticsCollector collector = createCollector(FieldSpec.DataType.STRING, false, false);

    collector.collect(new String[]{"a", "xy"});        // row length: 1 + 2 = 3
    collector.collect(new String[]{"hello", "world"}); // row length: 5 + 5 = 10

    collector.seal();

    assertEquals(collector.getLengthOfLargestElement(), 5);
    assertEquals(collector.getMaxRowLengthInBytes(), 10);
  }

  @Test
  public void testBytesLengthTracking() {
    AbstractColumnStatisticsCollector collector = createCollector(FieldSpec.DataType.BYTES, true, false);

    collector.collect(new byte[]{1});           // length 1
    collector.collect(new byte[]{1, 2, 3});     // length 3
    collector.collect(new byte[]{1, 2, 3, 4, 5}); // length 5

    collector.seal();

    assertEquals(collector.getLengthOfShortestElement(), 1);
    assertEquals(collector.getLengthOfLargestElement(), 5);
    assertEquals(collector.getMaxRowLengthInBytes(), 5);
  }

  @Test
  public void testBytesMultiValueLengthTracking() {
    AbstractColumnStatisticsCollector collector = createCollector(FieldSpec.DataType.BYTES, false, false);

    collector.collect(new byte[][]{{1}, {1, 2}});              // row length: 1 + 2 = 3
    collector.collect(new byte[][]{{1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}}); // row length: 5 + 5 = 10

    collector.seal();

    assertEquals(collector.getLengthOfShortestElement(), 1);
    assertEquals(collector.getLengthOfLargestElement(), 5);
    assertEquals(collector.getMaxRowLengthInBytes(), 10);
  }

  @Test
  public void testBigDecimalLengthTracking() {
    AbstractColumnStatisticsCollector collector = createCollector(FieldSpec.DataType.BIG_DECIMAL, true, false);

    collector.collect(new BigDecimal("1.0"));
    collector.collect(new BigDecimal("123456.789"));
    collector.collect(new BigDecimal("99.99"));

    collector.seal();

    // BigDecimal tracks length
    assertTrue(collector.getLengthOfShortestElement() > 0);
    assertTrue(collector.getLengthOfLargestElement() > 0);
    assertTrue(collector.getLengthOfLargestElement() >= collector.getLengthOfShortestElement());
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testBigDecimalMultiValueNotSupported() {
    AbstractColumnStatisticsCollector collector = createCollector(FieldSpec.DataType.BIG_DECIMAL, true, false);

    // BigDecimal does not support multi-value
    collector.collect((Object) new Object[]{new BigDecimal("1.0"), new BigDecimal("2.0")});
  }

  // Helper methods for test data generation

  private Object[] getTestDataForType(FieldSpec.DataType dataType, boolean sorted) {
    switch (dataType) {
      case INT:
        return sorted ? new Object[]{1, 5, 5, 10} : new Object[]{10, 1, 5, 5};
      case LONG:
        return sorted ? new Object[]{1L, 5L, 5L, 10L} : new Object[]{10L, 1L, 5L, 5L};
      case FLOAT:
        return sorted ? new Object[]{1.5f, 5.5f, 5.5f, 10.5f} : new Object[]{10.5f, 1.5f, 5.5f, 5.5f};
      case DOUBLE:
        return sorted ? new Object[]{1.5, 5.5, 5.5, 10.5} : new Object[]{10.5, 1.5, 5.5, 5.5};
      case STRING:
        return sorted ? new Object[]{"a", "b", "b", "c"} : new Object[]{"c", "a", "b", "b"};
      case BYTES:
        return sorted ? new Object[]{
            new byte[]{1}, new byte[]{2}, new byte[]{2}, new byte[]{3}
        } : new Object[]{
            new byte[]{3}, new byte[]{1}, new byte[]{2}, new byte[]{2}
        };
      case BIG_DECIMAL:
        return sorted ? new Object[]{
            new BigDecimal("1.0"), new BigDecimal("5.0"), new BigDecimal("5.0"), new BigDecimal("10.0")
        } : new Object[]{
            new BigDecimal("10.0"), new BigDecimal("1.0"), new BigDecimal("5.0"), new BigDecimal("5.0")
        };
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  private Object[][] getMultiValueTestData(FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return new Object[][]{
            {1, 2},
            {3, 4, 5}
        };
      case LONG:
        return new Object[][]{
            {1L, 2L},
            {3L, 4L, 5L}
        };
      case FLOAT:
        return new Object[][]{
            {1.5f, 2.5f},
            {3.5f, 4.5f, 5.5f}
        };
      case DOUBLE:
        return new Object[][]{
            {1.5, 2.5},
            {3.5, 4.5, 5.5}
        };
      case STRING:
        return new Object[][]{
            {"a", "b"},
            {"c", "d", "e"}
        };
      case BYTES:
        return new Object[][]{
            {new byte[]{1}, new byte[]{2}},
            {new byte[]{3}, new byte[]{4}, new byte[]{5}}
        };
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  private Object getSingleValueForType(FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return 42;
      case LONG:
        return 42L;
      case FLOAT:
        return 42.5f;
      case DOUBLE:
        return 42.5;
      case STRING:
        return "test";
      case BYTES:
        return new byte[]{1, 2, 3};
      case BIG_DECIMAL:
        return new BigDecimal("42.5");
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  private Object getMinValue(FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return 1;
      case LONG:
        return 1L;
      case FLOAT:
        return 1.5f;
      case DOUBLE:
        return 1.5;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  private Object getMaxValue(FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return 10;
      case LONG:
        return 10L;
      case FLOAT:
        return 10.5f;
      case DOUBLE:
        return 10.5;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  private Object getExpectedMinForType(FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return 1;
      case LONG:
        return 1L;
      case FLOAT:
        return 1.5f;
      case DOUBLE:
        return 1.5;
      case STRING:
        return "a";
      case BYTES:
        return new ByteArray(new byte[]{1});
      case BIG_DECIMAL:
        return new BigDecimal("1.0");
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  private Object getExpectedMaxForType(FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return 10;
      case LONG:
        return 10L;
      case FLOAT:
        return 10.5f;
      case DOUBLE:
        return 10.5;
      case STRING:
        return "c";
      case BYTES:
        return new ByteArray(new byte[]{3});
      case BIG_DECIMAL:
        return new BigDecimal("10.0");
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  private int getArrayLength(Object array) {
    if (array instanceof int[]) {
      return ((int[]) array).length;
    } else if (array instanceof long[]) {
      return ((long[]) array).length;
    } else if (array instanceof float[]) {
      return ((float[]) array).length;
    } else if (array instanceof double[]) {
      return ((double[]) array).length;
    } else if (array instanceof Object[]) {
      return ((Object[]) array).length;
    }
    throw new IllegalArgumentException("Unsupported array type: " + array.getClass());
  }
}
