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
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.BooleanUtils;
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
  private StatsCollectorConfig createStatsCollectorConfig(DataType dataType, boolean isSingleValue,
      boolean withPartitioning) {
    TableConfigBuilder builder = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME);

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
  private AbstractColumnStatisticsCollector createCollector(DataType dataType, boolean isSingleValue,
      boolean withPartitioning) {
    StatsCollectorConfig config = createStatsCollectorConfig(dataType, isSingleValue, withPartitioning);
    // Logical types map to stored types: BOOLEAN->INT, TIMESTAMP->LONG, JSON->STRING
    DataType storedType = dataType.getStoredType();
    switch (storedType) {
      case INT:
        return new IntColumnPreIndexStatsCollector(COLUMN_NAME, config);
      case LONG:
        return new LongColumnPreIndexStatsCollector(COLUMN_NAME, config);
      case FLOAT:
        return new FloatColumnPreIndexStatsCollector(COLUMN_NAME, config);
      case DOUBLE:
        return new DoubleColumnPreIndexStatsCollector(COLUMN_NAME, config);
      case BIG_DECIMAL:
        return new BigDecimalColumnPreIndexStatsCollector(COLUMN_NAME, config);
      case STRING:
        return new StringColumnPreIndexStatsCollector(COLUMN_NAME, config);
      case BYTES:
        return new BytesColumnPreIndexStatsCollector(COLUMN_NAME, config);
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  /**
   * Helper method to assert basic statistics after collection.
   * Assertions follow ColumnStatistics API definition order.
   */
  private void assertBasicStats(AbstractColumnStatisticsCollector collector, Object expectedMin, Object expectedMax,
      int expectedCardinality, boolean expectedSorted, int expectedTotalEntries) {
    if (expectedMin != null && expectedMax != null) {
      assertEquals(collector.getMinValue(), expectedMin);
      assertEquals(collector.getMaxValue(), expectedMax);
    }
    assertEquals(collector.getCardinality(), expectedCardinality);
    assertEquals(collector.isSorted(), expectedSorted);
    assertEquals(collector.getTotalNumberOfEntries(), expectedTotalEntries);
  }

  @DataProvider(name = "allDataTypes")
  public Object[][] allDataTypes() {
    return new Object[][]{
        {DataType.INT}, {DataType.LONG}, {DataType.FLOAT}, {DataType.DOUBLE}, {DataType.BIG_DECIMAL},
        {DataType.BOOLEAN}, {DataType.TIMESTAMP}, {DataType.STRING}, {DataType.JSON}, {DataType.BYTES}
    };
  }

  @DataProvider(name = "primitiveDataTypes")
  public Object[][] primitiveDataTypes() {
    return new Object[][]{
        {DataType.INT}, {DataType.LONG}, {DataType.FLOAT}, {DataType.DOUBLE}
    };
  }

  @DataProvider(name = "numericDataTypes")
  public Object[][] numericDataTypes() {
    return new Object[][]{
        {DataType.INT}, {DataType.LONG}, {DataType.FLOAT}, {DataType.DOUBLE}, {DataType.BIG_DECIMAL}
    };
  }

  @DataProvider(name = "multiValueSupportedTypes")
  public Object[][] multiValueSupportedTypes() {
    return new Object[][]{
        {DataType.INT}, {DataType.LONG}, {DataType.FLOAT}, {DataType.DOUBLE}, {DataType.BIG_DECIMAL},
        {DataType.STRING}, {DataType.BYTES}
    };
  }

  @DataProvider(name = "logicalDataTypes")
  public Object[][] logicalDataTypes() {
    return new Object[][]{
        {DataType.BOOLEAN}, {DataType.TIMESTAMP}, {DataType.JSON}
    };
  }

  @DataProvider(name = "lengthTrackingTypes")
  public Object[][] lengthTrackingTypes() {
    return new Object[][]{
        {DataType.BIG_DECIMAL}, {DataType.STRING}, {DataType.BYTES}
    };
  }

  // Test 1: Single-Value Collection using Primitive Methods

  @Test(dataProvider = "primitiveDataTypes")
  public void testSingleValuePrimitiveCollectionSorted(DataType dataType) {
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

    assertBasicStats(collector, getMinValue(dataType), getMaxValue(dataType), 3, true, 4);

    Object uniqueValuesSet = collector.getUniqueValuesSet();
    assertNotNull(uniqueValuesSet);
    assertEquals(getArrayLength(uniqueValuesSet), 3);
  }

  @Test(dataProvider = "primitiveDataTypes")
  public void testSingleValuePrimitiveCollectionUnsorted(DataType dataType) {
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

    assertBasicStats(collector, getMinValue(dataType), getMaxValue(dataType), 3, false, 4);
  }

  // Test 2: Single-Value Collection using collect(Object)

  @Test(dataProvider = "allDataTypes")
  public void testSingleValueObjectCollection(DataType dataType) {
    for (boolean sorted : new boolean[]{false, true}) {
      AbstractColumnStatisticsCollector collector = createCollector(dataType, true, false);

      // Collect values using Object method
      Object[] testData = getTestDataForType(dataType, sorted);
      for (Object value : testData) {
        collector.collect(value);
      }

      collector.seal();

      int expectedCardinality = dataType != DataType.BOOLEAN ? 3 : 2;  // BOOLEAN test data has only 2 unique values
      assertBasicStats(collector, getExpectedMinForType(dataType), getExpectedMaxForType(dataType), expectedCardinality,
          sorted, 4);
    }
  }

  // Test 3: Multi-Value Collection using Primitive Arrays

  @Test(dataProvider = "multiValueSupportedTypes")
  public void testMultiValuePrimitiveArrayCollection(DataType dataType) {
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
      case BIG_DECIMAL:
        collector.collect(new BigDecimal[]{new BigDecimal("1.5"), new BigDecimal("2.5")});
        collector.collect(new BigDecimal[]{new BigDecimal("3.5"), new BigDecimal("4.5"), new BigDecimal("5.5")});
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

    assertEquals(collector.getCardinality(), 5);
    assertFalse(collector.isSorted());  // multi-value is never sorted
    assertEquals(collector.getTotalNumberOfEntries(), 5);
    assertEquals(collector.getMaxNumberOfMultiValues(), 3);
  }

  @Test(dataProvider = "multiValueSupportedTypes")
  public void testMultiValueObjectArrayCollection(DataType dataType) {
    AbstractColumnStatisticsCollector collector = createCollector(dataType, false, false);

    // Collect multi-value entries using Object[]
    Object[][] testData = getMultiValueTestData(dataType);
    for (Object[] values : testData) {
      collector.collect(values);
    }

    collector.seal();

    assertEquals(collector.getCardinality(), 5);
    assertFalse(collector.isSorted());
    assertEquals(collector.getTotalNumberOfEntries(), 5);
    assertEquals(collector.getMaxNumberOfMultiValues(), 3);
  }

  // Test 4: Edge Cases

  @Test(dataProvider = "allDataTypes")
  public void testEmptyCollection(DataType dataType) {
    AbstractColumnStatisticsCollector collector = createCollector(dataType, true, false);

    // Seal without collecting any data
    collector.seal();

    assertEquals(collector.getCardinality(), 0);
    assertTrue(collector.isSorted());  // Empty is considered sorted
    assertEquals(collector.getTotalNumberOfEntries(), 0);
  }

  @Test(dataProvider = "allDataTypes")
  public void testSingleValue(DataType dataType) {
    AbstractColumnStatisticsCollector collector = createCollector(dataType, true, false);

    // Collect single value
    Object singleValue = getSingleValueForType(dataType);
    collector.collect(singleValue);
    collector.seal();

    Object expectedValue = dataType == DataType.BYTES ? new ByteArray((byte[]) singleValue) : singleValue;
    assertBasicStats(collector, expectedValue, expectedValue, 1, true, 1);
  }

  @Test(dataProvider = "allDataTypes")
  public void testAllDuplicates(DataType dataType) {
    AbstractColumnStatisticsCollector collector = createCollector(dataType, true, false);

    // Collect same value multiple times
    Object value = getSingleValueForType(dataType);
    for (int i = 0; i < 10; i++) {
      collector.collect(value);
    }
    collector.seal();

    Object expectedValue = dataType == DataType.BYTES ? new ByteArray((byte[]) value) : value;
    assertBasicStats(collector, expectedValue, expectedValue, 1, true, 10);
  }

  @Test(dataProvider = "allDataTypes", expectedExceptions = IllegalStateException.class)
  public void testUnsealedAccessThrowsException(DataType dataType) {
    AbstractColumnStatisticsCollector collector = createCollector(dataType, true, false);

    // Try to access stats before sealing - should throw
    collector.getMinValue();
  }

  // Test 5: Partition Function Tests

  @Test(dataProvider = "allDataTypes")
  public void testPartitionFunctionEnabled(DataType dataType) {
    for (boolean sorted : new boolean[]{false, true}) {
      AbstractColumnStatisticsCollector collector = createCollector(dataType, true, true);

      // Collect values
      Object[] testData = getTestDataForType(dataType, sorted);
      for (Object value : testData) {
        collector.collect(value);
      }
      collector.seal();

      PartitionFunction partitionFunction = collector.getPartitionFunction();
      assertNotNull(partitionFunction);
      assertEquals(partitionFunction.getNumPartitions(), NUM_PARTITIONS);

      Set<Integer> partitions = collector.getPartitions();
      assertNotNull(partitions);
      assertFalse(partitions.isEmpty());
      assertTrue(partitions.size() <= NUM_PARTITIONS);

      for (Integer partition : partitions) {
        assertTrue(partition >= 0 && partition < NUM_PARTITIONS);
      }
    }
  }

  @Test(dataProvider = "allDataTypes")
  public void testPartitionFunctionDisabled(DataType dataType) {
    for (boolean sorted : new boolean[]{false, true}) {
      AbstractColumnStatisticsCollector collector = createCollector(dataType, true, false);

      // Collect values
      Object[] testData = getTestDataForType(dataType, sorted);
      for (Object value : testData) {
        collector.collect(value);
      }
      collector.seal();

      assertNull(collector.getPartitionFunction());
      assertNull(collector.getPartitions());
    }
  }

  // Test 6: Type-Specific Length Tracking Tests
  // (ordered by DataType enum: BIG_DECIMAL, STRING, JSON, BYTES)

  @Test
  public void testBigDecimalLengthTracking() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.BIG_DECIMAL, true, false);

    collector.collect(new BigDecimal("1.0"));
    collector.collect(new BigDecimal("123456.789"));
    collector.collect(new BigDecimal("99.99"));

    collector.seal();

    assertTrue(collector.getLengthOfShortestElement() > 0);
    assertTrue(collector.getLengthOfLongestElement() >= collector.getLengthOfShortestElement());
  }

  @Test
  public void testBigDecimalMultiValueLengthTracking() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.BIG_DECIMAL, false, false);

    int shortLength = BigDecimalUtils.byteSize(new BigDecimal("1.0"));
    int midLength = BigDecimalUtils.byteSize(new BigDecimal("99.99"));
    int longLength = BigDecimalUtils.byteSize(new BigDecimal("123456.789"));
    collector.collect(new BigDecimal[]{new BigDecimal("1.0"), new BigDecimal("99.99")});
    collector.collect(new BigDecimal[]{new BigDecimal("123456.789"), new BigDecimal("99.99")});

    collector.seal();

    assertEquals(collector.getLengthOfShortestElement(), Math.min(shortLength, midLength));
    assertEquals(collector.getLengthOfLongestElement(), longLength);
    assertEquals(collector.getMaxRowLengthInBytes(), longLength + midLength);
  }

  @Test
  public void testStringLengthTracking() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.STRING, true, false);

    collector.collect("a");          // length 1
    collector.collect("xyz");        // length 3
    collector.collect("hello");      // length 5

    collector.seal();

    assertEquals(collector.getLengthOfShortestElement(), 1);
    assertEquals(collector.getLengthOfLongestElement(), 5);
    assertFalse(collector.isFixedLength());
    assertEquals(collector.getMaxRowLengthInBytes(), 5);
  }

  @Test
  public void testStringMultiValueLengthTracking() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.STRING, false, false);

    collector.collect(new String[]{"a", "xy"});        // row length: 1 + 2 = 3
    collector.collect(new String[]{"hello", "world"}); // row length: 5 + 5 = 10

    collector.seal();

    assertEquals(collector.getLengthOfShortestElement(), 1);
    assertEquals(collector.getLengthOfLongestElement(), 5);
    assertFalse(collector.isFixedLength());
    assertEquals(collector.getMaxRowLengthInBytes(), 10);
  }

  @Test
  public void testJsonLengthTracking() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.JSON, true, false);

    collector.collect("{\"a\":1}");                    // length 7
    collector.collect("{\"key\":\"value\"}");          // length 15
    collector.collect("{\"long_key\":\"long_value\"}"); // length 25

    collector.seal();

    assertEquals(collector.getLengthOfShortestElement(), 7);
    assertEquals(collector.getLengthOfLongestElement(), 25);
    assertFalse(collector.isFixedLength());
    assertEquals(collector.getMaxRowLengthInBytes(), 25);
  }

  @Test
  public void testBytesLengthTracking() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.BYTES, true, false);

    collector.collect(new byte[]{1});           // length 1
    collector.collect(new byte[]{1, 2, 3});     // length 3
    collector.collect(new byte[]{1, 2, 3, 4, 5}); // length 5

    collector.seal();

    assertEquals(collector.getLengthOfShortestElement(), 1);
    assertEquals(collector.getLengthOfLongestElement(), 5);
    assertFalse(collector.isFixedLength());
    assertEquals(collector.getMaxRowLengthInBytes(), 5);
  }

  @Test
  public void testBytesMultiValueLengthTracking() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.BYTES, false, false);

    collector.collect(new byte[][]{{1}, {1, 2}});              // row length: 1 + 2 = 3
    collector.collect(new byte[][]{{1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}}); // row length: 5 + 5 = 10

    collector.seal();

    assertEquals(collector.getLengthOfShortestElement(), 1);
    assertEquals(collector.getLengthOfLongestElement(), 5);
    assertFalse(collector.isFixedLength());
    assertEquals(collector.getMaxRowLengthInBytes(), 10);
  }

  // Test 7: Logical Types (ordered by DataType enum: BOOLEAN, TIMESTAMP, JSON)

  @Test
  public void testBooleanCollectionSorted() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.BOOLEAN, true, false);

    // BOOLEAN is stored as INT: 0 = false, 1 = true
    collector.collect(0);  // false
    collector.collect(0);  // false (duplicate)
    collector.collect(1);  // true
    collector.collect(1);  // true (duplicate)
    collector.seal();

    assertBasicStats(collector, 0, 1, 2, true, 4);
  }

  @Test
  public void testBooleanCollectionUnsorted() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.BOOLEAN, true, false);

    collector.collect(1);  // true
    collector.collect(0);  // false
    collector.collect(1);  // true
    collector.seal();

    assertBasicStats(collector, 0, 1, 2, false, 3);
  }

  @Test
  public void testTimestampCollectionSorted() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.TIMESTAMP, true, false);

    // TIMESTAMP is stored as LONG (millis since epoch)
    collector.collect(1000L);
    collector.collect(2000L);
    collector.collect(2000L);  // duplicate
    collector.collect(3000L);
    collector.seal();

    assertBasicStats(collector, 1000L, 3000L, 3, true, 4);
  }

  @Test
  public void testTimestampCollectionUnsorted() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.TIMESTAMP, true, false);

    collector.collect(3000L);
    collector.collect(1000L);
    collector.collect(2000L);
    collector.seal();

    assertBasicStats(collector, 1000L, 3000L, 3, false, 3);
  }

  @Test
  public void testJsonCollectionSorted() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.JSON, true, false);

    // JSON is stored as STRING
    collector.collect("{\"a\":1}");
    collector.collect("{\"b\":2}");
    collector.collect("{\"b\":2}");  // duplicate
    collector.collect("{\"c\":3}");
    collector.seal();

    assertBasicStats(collector, "{\"a\":1}", "{\"c\":3}", 3, true, 4);
  }

  @Test
  public void testJsonCollectionUnsorted() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.JSON, true, false);

    collector.collect("{\"c\":3}");
    collector.collect("{\"a\":1}");
    collector.collect("{\"b\":2}");
    collector.seal();

    assertBasicStats(collector, "{\"a\":1}", "{\"c\":3}", 3, false, 3);
  }

  @Test(dataProvider = "logicalDataTypes")
  public void testLogicalTypeStoredTypeMapping(DataType dataType) {
    AbstractColumnStatisticsCollector collector = createCollector(dataType, true, false);

    // Verify the collector uses the correct stored type
    DataType expectedStoredType = dataType.getStoredType();
    assertEquals(collector.getStoredType(), expectedStoredType);
  }

  // Test 8: isAscii Tests

  @Test
  public void testIsAsciiWithAsciiStrings() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.STRING, true, false);

    collector.collect("hello");
    collector.collect("world");
    collector.collect("test123");
    collector.seal();

    assertTrue(collector.isAscii());
  }

  @Test
  public void testIsAsciiWithNonAsciiStrings() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.STRING, true, false);

    collector.collect("hello");
    collector.collect("wörld");  // 'ö' is non-ASCII
    collector.seal();

    assertFalse(collector.isAscii());
  }

  @Test
  public void testIsAsciiWithUnicodeStrings() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.STRING, true, false);

    collector.collect("世界");  // Chinese characters
    collector.seal();

    assertFalse(collector.isAscii());
  }

  @Test
  public void testIsAsciiWithEmptyStrings() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.STRING, true, false);

    collector.collect("");
    collector.seal();

    // Empty string is ASCII (no non-ASCII bytes)
    assertTrue(collector.isAscii());
  }

  @Test
  public void testIsAsciiWithMixedSingleAndNonAscii() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.STRING, true, false);

    // First string is ASCII, second is not - once non-ASCII is detected, isAscii stays false
    collector.collect("ascii");
    collector.collect("café");  // 'é' is non-ASCII
    collector.collect("more_ascii");
    collector.seal();

    assertFalse(collector.isAscii());
  }

  @Test
  public void testIsAsciiMultiValue() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.STRING, false, false);

    collector.collect(new String[]{"hello", "world"});
    collector.collect(new String[]{"test"});
    collector.seal();

    assertTrue(collector.isAscii());
  }

  @Test
  public void testIsAsciiMultiValueWithNonAscii() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.STRING, false, false);

    collector.collect(new String[]{"hello", "üöä"});  // German umlauts
    collector.seal();

    assertFalse(collector.isAscii());
  }

  @Test
  public void testIsAsciiJsonColumn() {
    // JSON columns use StringColumnPreIndexStatsCollector - verify isAscii works
    AbstractColumnStatisticsCollector collector = createCollector(DataType.JSON, true, false);

    collector.collect("{\"key\":\"value\"}");
    collector.collect("{\"num\":123}");
    collector.seal();

    assertTrue(collector.isAscii());
  }

  @Test
  public void testIsAsciiJsonColumnWithUnicode() {
    AbstractColumnStatisticsCollector collector = createCollector(DataType.JSON, true, false);

    collector.collect("{\"name\":\"émile\"}");  // French name with accent
    collector.seal();

    assertFalse(collector.isAscii());
  }

  // Helper methods for test data generation

  private Object[] getTestDataForType(DataType dataType, boolean sorted) {
    switch (dataType) {
      case INT:
        return sorted ? new Object[]{1, 5, 5, 10} : new Object[]{10, 1, 5, 5};
      case LONG:
        return sorted ? new Object[]{1L, 5L, 5L, 10L} : new Object[]{10L, 1L, 5L, 5L};
      case FLOAT:
        return sorted ? new Object[]{1.5f, 5.5f, 5.5f, 10.5f} : new Object[]{10.5f, 1.5f, 5.5f, 5.5f};
      case DOUBLE:
        return sorted ? new Object[]{1.5, 5.5, 5.5, 10.5} : new Object[]{10.5, 1.5, 5.5, 5.5};
      case BIG_DECIMAL:
        return sorted ? new Object[]{
            new BigDecimal("1.0"), new BigDecimal("5.0"), new BigDecimal("5.0"), new BigDecimal("10.0")
        } : new Object[]{
            new BigDecimal("10.0"), new BigDecimal("1.0"), new BigDecimal("5.0"), new BigDecimal("5.0")
        };
      case BOOLEAN:
        // BOOLEAN stored as INT: 0 = false, 1 = true
        return sorted ? new Object[]{0, 0, 1, 1} : new Object[]{1, 0, 1, 0};
      case TIMESTAMP:
        // TIMESTAMP stored as LONG (millis since epoch)
        return sorted ? new Object[]{1000L, 2000L, 2000L, 3000L} : new Object[]{3000L, 1000L, 2000L, 2000L};
      case STRING:
        return sorted ? new Object[]{"a", "b", "b", "c"} : new Object[]{"c", "a", "b", "b"};
      case JSON:
        // JSON stored as STRING
        return sorted ? new Object[]{"{\"a\":1}", "{\"b\":2}", "{\"b\":2}", "{\"c\":3}"}
            : new Object[]{"{\"c\":3}", "{\"a\":1}", "{\"b\":2}", "{\"b\":2}"};
      case BYTES:
        return sorted ? new Object[]{
            new byte[]{1}, new byte[]{2}, new byte[]{2}, new byte[]{3}
        } : new Object[]{
            new byte[]{3}, new byte[]{1}, new byte[]{2}, new byte[]{2}
        };
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  private Object[][] getMultiValueTestData(DataType dataType) {
    switch (dataType) {
      case INT:
        return new Object[][]{
            {1, 2}, {3, 4, 5}
        };
      case LONG:
        return new Object[][]{
            {1L, 2L}, {3L, 4L, 5L}
        };
      case FLOAT:
        return new Object[][]{
            {1.5f, 2.5f}, {3.5f, 4.5f, 5.5f}
        };
      case DOUBLE:
        return new Object[][]{
            {1.5, 2.5}, {3.5, 4.5, 5.5}
        };
      case BIG_DECIMAL:
        return new Object[][]{
            {new BigDecimal("1.5"), new BigDecimal("2.5")},
            {new BigDecimal("3.5"), new BigDecimal("4.5"), new BigDecimal("5.5")}
        };
      case STRING:
        return new Object[][]{
            {"a", "b"}, {"c", "d", "e"}
        };
      case BYTES:
        return new Object[][]{
            {new byte[]{1}, new byte[]{2}}, {new byte[]{3}, new byte[]{4}, new byte[]{5}}
        };
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  private Object getSingleValueForType(DataType dataType) {
    switch (dataType) {
      case INT:
        return 42;
      case LONG:
        return 42L;
      case FLOAT:
        return 42.5f;
      case DOUBLE:
        return 42.5;
      case BIG_DECIMAL:
        return new BigDecimal("42.5");
      case BOOLEAN:
        return BooleanUtils.toInt(true);
      case TIMESTAMP:
        return 1000L;
      case STRING:
        return "test";
      case JSON:
        return "{\"key\":\"value\"}";
      case BYTES:
        return new byte[]{1, 2, 3};
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  private Object getMinValue(DataType dataType) {
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

  private Object getMaxValue(DataType dataType) {
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

  private Object getExpectedMinForType(DataType dataType) {
    switch (dataType) {
      case INT:
        return 1;
      case LONG:
        return 1L;
      case FLOAT:
        return 1.5f;
      case DOUBLE:
        return 1.5;
      case BIG_DECIMAL:
        return new BigDecimal("1.0");
      case BOOLEAN:
        return 0;
      case TIMESTAMP:
        return 1000L;
      case STRING:
        return "a";
      case JSON:
        return "{\"a\":1}";
      case BYTES:
        return new ByteArray(new byte[]{1});
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  private Object getExpectedMaxForType(DataType dataType) {
    switch (dataType) {
      case INT:
        return 10;
      case LONG:
        return 10L;
      case FLOAT:
        return 10.5f;
      case DOUBLE:
        return 10.5;
      case BIG_DECIMAL:
        return new BigDecimal("10.0");
      case BOOLEAN:
        return 1;
      case TIMESTAMP:
        return 3000L;
      case STRING:
        return "c";
      case JSON:
        return "{\"c\":3}";
      case BYTES:
        return new ByteArray(new byte[]{3});
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
